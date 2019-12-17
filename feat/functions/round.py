
# from jenks import jenks
from timeit import default_timer as timer

from ..common.Frame import Frame
from .lib.per_col import make_per_col

def call_round(column, to_decimal):
  to_decimal = int(to_decimal)
  
  def apply(value):
    if type(value) in [int, float]:
      return round(value, to_decimal) # Second arg must be an integer.
    elif pd.isna(value):
      return None
    else:
      print(value)
      raise Exception("WTF")
  return column.apply(apply)

def jenks_column(column, n_clusters):
  start = timer()
  breaks = jenks(column[~column.isna()].values, n_clusters)
  print("\nJenks took: %s seconds" % round(timer() - start, 2))
  
  print(breaks)
  def apply(value):
    if value is None:
      return None
    for index, jenks_break in enumerate(breaks):
      if value <= jenks_break:
        return value
        # return index
    return index
  return column.apply(apply)

def call_round_cluster(column, n_clusters):
  n_clusters = int(n_clusters)
  
  return jenks_column(column, n_clusters)



functions = {
  'ROUND_FLOAT': make_per_col(call_round, fillna=-1, dtype=int, num_args=2),
  'ROUND_CLUSTER': make_per_col(call_round_cluster, fillna=-1, dtype=int, num_args=2),
}
