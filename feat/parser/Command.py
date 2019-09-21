
# TODO rename.

class Command(object):
  def __init__(self, tree):
    self._tree = tree

  def get_name(self):
    return self._tree['name']

  def is_terminal(self):
    return self._tree.get('is_terminal')

  def is_function(self):
    return 'function' in self._tree

  def has_next(self):
    return 'next' in self._tree

  def get_next(self):
    return Command(self._tree.get('next'))

  def get_root(self):
    return self._tree.get('root')

  def get_translation(self):
    return self._tree.get('translation')

  def get_this(self):
    return self._tree['this']

  def has_groupby(self):
    return bool(self._tree.get('groupby')) # 'groupby' in self._tree

  def get_groupby(self):
    return [Command(g) for g in self._tree['groupby']]

  def get_function(self):
    return self._tree.get('function')

  def get_args(self):
    args = []
    for arg in self._tree.get('args', []):
      if isinstance(arg, dict): # FIXME very shaky shit
        args.append(Command(arg))
      else:
        args.append(arg)
    return args
    # return self._tree.get('args')