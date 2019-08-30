"""
Functions related to comparing.
"""

import numpy as np
import pandas as pd
from numba import njit

from .lib.percol import make_percol
from .lib.pergroup import make_pergroup

# https://stackoverflow.com/a/52674448/396050
# @njit
def call_greaterthan(column, args):
  return column > args[1]

greaterthan = make_percol(call_greaterthan, fillna=0, dtype=np.bool)

def call_changed(keys, rows):
  '''True when a value changes from yesterday to today.'''

  result = {}
  last = 23456123489123 # A random thing.
  for date in sorted(rows.keys()):
    if rows[date] and rows[date]['_value_']:
      this = rows[date]['_value_'] 
      result[date] = last != this
      last = this
    else:
      result[date] = False
  return result

changed = make_pergroup(call_changed, fillna=0)