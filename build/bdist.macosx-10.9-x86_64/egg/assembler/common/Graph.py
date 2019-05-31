

class Graph(object):

  def __init__(self):
    self.nodes = []
    self.pivots = dict()
    self.edges = []


  def add_node(self, name, pivots=[]):
    if not pivots:
      print("Warn: pivots is empty")
    if name in self.nodes:
      raise Exception('Node %s already registered.' % name)
    self.nodes.append(name)
    self.pivots[name] = pivots


  def add_edge(self, tableOut, colOut, tableIn, colIn):
    assert tableOut in self.nodes, "%s not a registered node" % tableOut
    assert tableIn in self.nodes, "%s not a registered node" % tableIn
    self.edges.append([tableOut, colOut, tableIn, colIn])


  def find_edge(self, tableOut=None, colOut=None, tableIn=None, colIn=None):
    if tableOut:
      assert tableOut in self.nodes, "%s not a registered node" % tableOut
    if tableIn:
      assert tableIn in self.nodes, "%s not a registered node" % tableIn

    if not tableOut and not colOut and not tableIn and not colIn:
      raise Exception()
    found = []
    for edge in self.edges:
      if tableOut and edge[0] != tableOut:
        continue
      if colOut and edge[1] != colOut:
        continue
      if tableIn and edge[2] != tableIn:
        continue
      if colIn and edge[3] != colIn:
        continue
      found.append(edge)
    return found


  def get_leaf_information(self, current, path):
    """
    Example: Given current='OrdersItems' and path='order.user', return
      ['User','id'], the leaf table and the column that it is mapped to by
      order.user.
    """

    def recurse(current, path, colIn=None):
      if not path:
        return current, colIn

      split = path.split('.')
      colOut, rest = split[0], '.'.join(split[1:])
      edges = self.find_edge(tableOut=current, colOut=colOut)
      if not edges:
        return None
      edge = edges[0]
      return recurse(edge[2], rest, edge[3])

    return recurse(current, path)


  def __str__(self):
    return 'Graph(<%s>|<%s>)' % (self.nodes, self.edges)
