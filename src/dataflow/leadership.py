"""PostgreSQL-backed leader election for long-lived DataFlow controllers."""

from __future__ import annotations

from typing import Any, Protocol

import psycopg
from psycopg import Connection

DEFAULT_LOCK_NAMESPACE = 441001
DEFAULT_LOCK_KEY = 1
_INT32_MIN = -(2**31)
_INT32_MAX = 2**31 - 1


class LeadershipUnavailable(RuntimeError):
    """The leadership backend cannot currently be reached."""


class ControllerLeadership(Protocol):
    """Exclusive leadership session used to fence controller reconciliation."""

    def try_acquire(self) -> bool: ...

    def is_current(self) -> bool: ...

    def release(self) -> None: ...


class PostgresControllerLeadership:
    """Hold a PostgreSQL session advisory lock for one controller replica.

    PostgreSQL releases advisory locks automatically when the owning session ends, so
    process death or a broken database connection makes leadership available to a
    standby without a lease-expiry delay.
    """

    def __init__(
        self,
        dsn: str,
        *,
        lock_namespace: int = DEFAULT_LOCK_NAMESPACE,
        lock_key: int = DEFAULT_LOCK_KEY,
    ) -> None:
        self._dsn = dsn
        self._lock_namespace = _int32("lock_namespace", lock_namespace)
        self._lock_key = _int32("lock_key", lock_key)
        self._connection: Connection[Any] | None = None

    @property
    def lock_namespace(self) -> int:
        return self._lock_namespace

    @property
    def lock_key(self) -> int:
        return self._lock_key

    def try_acquire(self) -> bool:
        if self._connection is not None:
            if self.is_current():
                return True
            self.release()

        connection: Connection[Any] | None = None
        try:
            connection = psycopg.connect(
                self._dsn,
                autocommit=True,
                application_name="dataflow-controller-leader-election",
            )
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_try_advisory_lock(%s, %s)",
                    (self._lock_namespace, self._lock_key),
                )
                row = cursor.fetchone()
        except psycopg.Error as error:
            if connection is not None:
                try:
                    connection.close()
                except psycopg.Error:
                    pass
            raise LeadershipUnavailable("PostgreSQL leadership backend is unavailable") from error

        acquired = bool(row and row[0])
        if not acquired:
            connection.close()
            return False

        self._connection = connection
        return True

    def is_current(self) -> bool:
        connection = self._connection
        if connection is None or connection.closed:
            return False
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        except psycopg.Error:
            self._drop_connection()
            return False
        return True

    def release(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        try:
            if not connection.closed:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(%s, %s)",
                        (self._lock_namespace, self._lock_key),
                    )
        except psycopg.Error:
            # A broken session has already released its advisory locks server-side.
            pass
        finally:
            try:
                connection.close()
            except psycopg.Error:
                pass

    def close(self) -> None:
        self.release()

    def __enter__(self) -> PostgresControllerLeadership:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()

    def _drop_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        try:
            connection.close()
        except psycopg.Error:
            pass


def _int32(name: str, value: int) -> int:
    if not _INT32_MIN <= value <= _INT32_MAX:
        raise ValueError(f"{name} must fit in a signed 32-bit integer")
    return value


__all__ = [
    "DEFAULT_LOCK_KEY",
    "DEFAULT_LOCK_NAMESPACE",
    "ControllerLeadership",
    "LeadershipUnavailable",
    "PostgresControllerLeadership",
]
