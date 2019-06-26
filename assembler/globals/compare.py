"""
Functions related to comparing.
"""

import numpy as np
import pandas as pd
from numba import njit

from .lib.percol import make_percol

# https://stackoverflow.com/a/52674448/396050
# @njit
def call_greaterthan(column, args):
  return column > args[1]

greaterthan = make_percol(call_greaterthan, fillna=0, dtype=np.bool)
