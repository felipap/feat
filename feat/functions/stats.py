"""
Functions related to statistics.
"""

import numpy as np
import pandas as pd

from .lib.per_group import make_per_group

def strend_foreach(rows):
  result = {}
  last_few = [0, 0, 0, 0, 0]
  for index, date in enumerate(sorted(rows.keys())):
    now = rows[date] and rows[date]['_value_'] or 0
    lastmean = sum(last_few)/len(last_few)
    trend = now - lastmean
    last_few = last_few[1:]+[now]
    result[date] = trend
  return result

functions = {
  'STREND': make_per_group(strend_foreach, fillna=-99999),
}
