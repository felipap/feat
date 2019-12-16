
from .compare import call_first



def test_increment():
    assert call_first.increment(3) == 4

def test_decrement():
    assert call_first.decrement(3) == 4
