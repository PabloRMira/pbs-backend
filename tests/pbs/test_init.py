import pbs


def test_version():
    assert isinstance(pbs.__version__, str)
