"""
Functions related to statistics.
"""

import numpy as np
import pandas as pd

from .lib.pergroup import make_pergroup

def strend_foreach(key, rows):
  result = {}
  last_few = [0,0]
  for index, date in enumerate(sorted(rows.keys())):
    now = rows[date] and rows[date]['_value_'] or 0
    lastmean = sum(last_few)/len(last_few)
    trend = now - lastmean
    last_few = last_few[1:]+[now]
    result[date] = trend
  return result
  # result = {}
  # mean = 0
  # for index, date in enumerate(sorted(rows.keys())):
  #   now = rows[date] and rows[date]['_value_'] or 0
  #   mean = ((mean * index)+now)/(index+1)
  #   result[date] = now - mean
  # return result

strend = make_pergroup(strend_foreach, fillna=-99999)


