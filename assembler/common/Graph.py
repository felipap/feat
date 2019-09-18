
import re

def caseword(word):
  return word[0].upper()+word[1:]

RE_POINTER = re.compile('^\w+\.\w+$')

class Graph(object):
  """ asdf """

  def __init__(self):
    self.nodes = []
    self.pivots = dict()
    self.edges = []

    ####

    self.tables = {}
    self.output = None
    self._wrapped = False

  def add_node(self, name, pivots=[]):
    if not pivots:
      print("Warn: pivots is empty")
    if name in self.nodes:
      raise Exception('Node %s already registered.' % name)
    
    self.nodes.append(name)
    self.pivots[name] = pivots

  def wrap(self):
    """
    To be called once all the tables have been added, so that pointers
    between them can be created and checked for dangling values.
    """
    
    if self._wrapped:
      raise Exception()
    self._wrapped = True
    
    self._build_edges()
    self._check_dangling_pointers()

  def _check_dangling_pointers(self):
    """
    TODO
    Expensive operation.
    """
    
    print("CHECK DANGLING POINTERS")

  def add_table(self, table):
    if not table.name.islower():
      raise Exception('Table names must be lowercase.')
    if table.name in self.tables:
      raise Exception()
    self.tables[table.name] = table

    self.add_node(table.name, table.get_keys())

  def _build_edges(self):    
    # Must register all nodes first, and only then register the edges.

    for table in self.tables.values():
      pointers = table.get_pointers()
      if not pointers:
        continue
      for out_column, pointer in pointers.items():
        if not RE_POINTER.match(pointer):
          raise Exception()

        in_table, in_column = pointer.split('.')
        self.add_edge(table.name, out_column, in_table, in_column)

    if not self.output:
      raise Exception('build_edges() must be called after add_output()')

    for (field, pointer) in self.output.get_pointers().items():
      in_table, in_column = pointer.split('.')
      self.add_edge('output', field, in_table, in_column)

  def get_table(self, name):
    # if name == 'output':
    #   return self.output
    return self.tables[name]

  def add_output(self, output):
    if self.output:
      raise Exception()
    self.output = output
    self.tables['output'] = output

    self.add_node('output', [output.DATE_FIELD, *output.get_pointers().keys()])

  ###

  def add_edge(self, tableOut, colOut, tableIn, colIn):
    assert tableOut in self.nodes, "%s not a registered node" % tableOut
    assert tableIn in self.nodes, "%s not a registered node" % tableIn
    self.edges.append([tableOut, colOut, tableIn, colIn])

  def find_edge(self, tableOut=None, colOut=None, tableIn=None, colIn=None):
    if tableOut:
      tableOut = tableOut.lower()
    if tableIn:
      tableIn = tableIn.lower()
    
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
