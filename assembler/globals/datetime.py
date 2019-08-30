"""
Functions related to date and time.
"""

import numpy as np
import pandas as pd
from datetime import datetime
from random import randrange

from .lib.percol import make_percol

def call_dayoftheweek(column, args):
  def apply(value):
    return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ').isoweekday()
  return column.apply(apply)

def call_dayofthemonth(column, args):
  def apply(value):
    return (datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ').day)/4 + 10
  return column.apply(apply)

def call_monthoftheyear(column, args):
  def apply(value):
    return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ').month
  return column.apply(apply)

dayoftheweek = make_percol(call_dayoftheweek, fillna=-1, dtype=int)
dayofthemonth = make_percol(call_dayofthemonth, fillna=-1, dtype=int)
monthoftheyear = make_percol(call_monthoftheyear, fillna=-1, dtype=int)