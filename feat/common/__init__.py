
from .Context import Context
from .Frame import Frame
from .Graph import Graph
from .Output import Output
from .assert_returns_frame import assert_returns_frame

from functools import wraps

def assemble_column_log_errors(inner):
  @wraps(inner)
  def outer(ctx, tree):
    try:
      result = inner(ctx, tree)
    except Exception as e:
      print("^ Error with", ctx.current, tree.get_name())
      raise e
    assert result.__class__.__name__ == 'Frame', result.__class__.__name__
    # assert isinstance(result, Frame) # Won't work with jupyter autoreload.
    # assert isinstance(result, Frame), "%s isn't Frame" % result
    return result
  return outer