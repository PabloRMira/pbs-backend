from unittest.mock import patch

from pbs.cli import make_cli


def some_func(some_input: str):
    return some_input


def test_make_cli():
    out_func = make_cli(some_func)
    some_input = "bla_bla"
    with patch("sys.argv", ["prog", f"--some-input={some_input}"]):
        out = out_func()
    assert out == some_input
