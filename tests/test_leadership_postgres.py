from __future__ import annotations

import os
from uuid import uuid4

import pytest

from dataflow.leadership import PostgresControllerLeadership


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    dsn = os.environ.get("DATAFLOW_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("DATAFLOW_TEST_DATABASE_URL is not configured")
    return dsn


def _lock_key() -> int:
    return uuid4().int % (2**31 - 1)


def test_only_one_controller_owns_same_advisory_lock(postgres_dsn: str) -> None:
    key = _lock_key()
    first = PostgresControllerLeadership(postgres_dsn, lock_key=key)
    second = PostgresControllerLeadership(postgres_dsn, lock_key=key)

    try:
        assert first.try_acquire() is True
        assert first.is_current() is True
        assert second.try_acquire() is False

        first.release()

        assert first.is_current() is False
        assert second.try_acquire() is True
        assert second.is_current() is True
    finally:
        first.release()
        second.release()


def test_broken_leader_session_releases_lock_for_standby(postgres_dsn: str) -> None:
    key = _lock_key()
    first = PostgresControllerLeadership(postgres_dsn, lock_key=key)
    second = PostgresControllerLeadership(postgres_dsn, lock_key=key)

    try:
        assert first.try_acquire() is True
        assert first._connection is not None
        first._connection.close()

        assert first.is_current() is False
        assert second.try_acquire() is True
    finally:
        first.release()
        second.release()


def test_same_controller_can_reacquire_after_release(postgres_dsn: str) -> None:
    leadership = PostgresControllerLeadership(postgres_dsn, lock_key=_lock_key())

    try:
        assert leadership.try_acquire() is True
        leadership.release()
        assert leadership.try_acquire() is True
    finally:
        leadership.release()


def test_lock_identifiers_must_fit_postgresql_int32() -> None:
    with pytest.raises(ValueError, match="lock_key"):
        PostgresControllerLeadership("postgresql://unused", lock_key=2**31)
