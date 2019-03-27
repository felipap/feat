from .Frame import Frame

class Context(object):
  def __init__(self, globals, current, timeCol):
    self.current = None
    self.globals = globals
    self.swapIn(current)
    self.timeCol = timeCol

    self.rels = []
    self.pivots = []

  def swapIn(self, current):
    assert current in self.globals
    oldCurrent = self.current
    self.current = current
    return oldCurrent

  @property
  def df(self):
    # print("fs is", self.current, self.globals)
    return self.globals[self.current]

  @df.setter
  def df(self, value):
    self.globals[self.current] = value

  def addRelationship(self, table1, col1, table2, col2):
    self.rels.append((table1, col1, table2, col2))

  def getRelationshipOnField(self, table, field):
    # print("table", table, field)
    for rel in self.rels:
      if rel[0] == table and rel[1] == field:
        return (rel[0], rel[1], rel[2], rel[3])
      elif rel[2] == table and rel[3] == field:
        return (rel[2], rel[3], rel[0], rel[1])
    return None

  def createFrameFromCurrent(self):
    return
