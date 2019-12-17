"""
Functions related to date and time.
"""

import numpy as np
import pandas as pd
from datetime import datetime
from random import randrange
from ..lib.tblock import date_to_cmonth, cweek_to_date, date_to_cweek

from .lib.per_col import make_per_col
from .lib.per_value import per_value

def call_dayoftheweek(column):
  def apply(value):
    if type(value) == int:
      # We should check if the block is week here
      # if ctx['block_type'] == 'month':
      date = cweek_to_date(value)
    else:
      date = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    return date.isoweekday()
  return column.apply(apply)

def call_dayofthemonth(column):
  def apply(value):
    if type(value) == int:
      # We should check if the block is week here
      # if ctx['block_type'] == 'month':
      date = cweek_to_date(value)
    else:
      date = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    return date.day # /4 + 10
  return column.apply(apply)

def call_monthoftheyear(column):
  def apply(value):
    if type(value) == int:
      # We should check if the block is week here
      # if ctx['block_type'] == 'month':
      date = cweek_to_date(value)
    else:
      date = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    return date.month
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
