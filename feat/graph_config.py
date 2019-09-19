# https://github.com/tensorflow/tensorflow/blob/master/tensorflow/python/framework/function.py


class GraphConfig(object):
  """An interface to read the graph configuration provided by the user."""

  def __init__(self, types):
    """Create a `GraphInput` object.

    Args:
      types: a dictionary of type configurations keyed by type ids.

    """
    
    self._input_types = types
    print("types is", types)