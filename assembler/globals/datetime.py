"""
Functions related to date and time.
"""

import numpy as np
import pandas as pd
from datetime import datetime
from random import randrange

from .lib.percol import make_percol

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

functions = {
  'DT_DAY_OF_THE_WEEK': make_percol(call_dayoftheweek, fillna=-1, dtype=int),
  'DT_DAY_OF_THE_MONTH': make_percol(call_dayofthemonth, fillna=-1, dtype=int),
  'DT_MONTH_OF_THE_YEAR': make_percol(call_monthoftheyear, fillna=-1, dtype=int),
}
