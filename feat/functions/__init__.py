
from . import default
from . import counts
from . import nested
from . import compare
from . import future
from . import stats
from . import people
from . import formatter
from . import datetime
from . import round
from . import math

ALL_PACKS = [
  default.functions,
  counts.functions,
  nested.functions,
  compare.functions,
  future.functions,
  stats.functions,
  people.functions,
  formatter.functions,
  datetime.functions,
  round.functions,
  math.functions,
]

def assert_valid_function(fn):
  try:
    assert isinstance(fn, dict)
    assert 'call' in fn
    assert 'num_args' in fn
    assert set(fn.keys()).issubset(['call', 'num_args', 'takes_pivots', 'keyword', 'name'])
  except AssertionError as err:
    print(f'Invalid function {fn}', err)
    raise err

FUNCTIONS = {}
for pack in ALL_PACKS:
  all(map(assert_valid_function, pack.values()))

  duplicated = set(pack.keys()).intersection(FUNCTIONS.keys())
  if duplicated:
    raise Exception(f'Duplicated FUNCTIONS {duplicated}')
  FUNCTIONS.update(pack)

def getFunction(name: str):
  """
  Get a function of `name`.
  """

  if name in FUNCTIONS:
    return FUNCTIONS[name]
  raise Exception(f'Can\'t find function named {name}.')

