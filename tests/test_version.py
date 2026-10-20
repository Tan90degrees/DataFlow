from dataflow import __version__
from dataflow.version import package_version


def test_package_version_matches_public_version():
    assert __version__ == package_version()
