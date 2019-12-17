"""
Functions related to statistics.
"""

import numpy as np
import pandas as pd

from .lib.per_group import make_per_group, get_window_values

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


def call_trend_diff(rows, distance):
  """
  """

  distance = int(distance)
  
  # REVIEW
  # What should the result be for None values in this or in past? Is there any
  # situation we want it to be None as well?
  
  result = {}
  for date in sorted(rows.keys()):
    this = rows.get(date)
    past = rows.get(date-distance)
    this_value = this['_value_'] if (this and pd.notna(this['_value_'])) else 0
    past_value = past['_value_'] if (past and pd.notna(past['_value_'])) else 0
    result[date] = this_value - past_value
  return result


functions = {
  'STREND': make_per_group(strend_foreach, fillna=-99999),
  'TREND_DIFF': make_per_group(call_trend_diff, num_args=2),
}
