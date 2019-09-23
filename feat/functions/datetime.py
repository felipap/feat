"""
Functions related to date and time.
"""

import numpy as np
import pandas as pd
from datetime import datetime
from random import randrange
from ..lib.tblock import date_to_cmonth, cmonth_to_date, date_to_cweek

from .lib.per_col import make_per_col
from .lib.per_value import per_value

def call_dayoftheweek(column, _):
  def apply(value):
    return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ').isoweekday()
  return column.apply(apply)

def call_dayofthemonth(column, _):
  def apply(value):
    return (datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ').day)/4 + 10
  return column.apply(apply)

def call_monthoftheyear(column, _):
  def apply(value):
    return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ').month
  return column.apply(apply)

def call_date(ctx, value):
  date = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')

  if ctx['block_type'] == 'month':
    return date_to_cmonth(date)
  else:
    return date_to_cweek(date)

functions = {
  'DT_DAY_OF_THE_WEEK': make_per_col(call_dayoftheweek, fillna=-1, dtype=int),
  'DT_DAY_OF_THE_MONTH': make_per_col(call_dayofthemonth, fillna=-1, dtype=int),
  'DT_MONTH_OF_THE_YEAR': make_per_col(call_monthoftheyear, fillna=-1, dtype=int),
  'DATE': per_value(call_date, fillna=-1, dtype=int, takes_ctx=True),
}
