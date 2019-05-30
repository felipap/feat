
from .Frame import Frame
from functools import wraps

def assert_returns_frame(function):
  @wraps(function)
  def foo(*args, **kwargs):
    result = function(*args, **kwargs)

    assert result.__class__.__name__ == 'Frame', result.__class__.__name__
    # assert isinstance(result, Frame) # Won't work with jupyter autoreload.
    # assert isinstance(result, Frame), "%s isn't Frame" % result
    return result
  return foo
