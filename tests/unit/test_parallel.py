"""map_io: concurrent, in input order, errors surface."""
import pytest

from loader.parallel import map_io


def test_results_keep_input_order():
    assert map_io(lambda x: x * 2, range(500)) == [x * 2 for x in range(500)]


def test_the_first_error_is_raised():
    def boom(x):
        if x == 7:
            raise ValueError("seven")
        return x
    with pytest.raises(ValueError, match="seven"):
        map_io(boom, range(20))
