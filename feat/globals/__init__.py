
from . import default
from . import counts
from . import verb
from . import compare
from . import stats
from . import formatter
from . import datetime

ALL_PACKS = [
  default.functions,
  counts.functions,
  verb.functions,
  compare.functions,
  stats.functions,
  formatter.functions,
  datetime.functions,
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

print("Registered functions", FUNCTIONS.keys())

def getFunction(name: str):
  """
  Get a function of `name`.
  """

  if name in FUNCTIONS:
    return FUNCTIONS[name]
  raise Exception(f'Can\'t find function named {name}.')