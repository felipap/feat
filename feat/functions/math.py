
import pandas as pd
import numpy as np
from ..common.Frame import Frame
from .lib.per_value import per_value

def math_divide(ctx, name, args):
  # Args one and two must be (already generated) frames of the same table.
  if not isinstance(args[0], Frame):
    raise Exception()
  if not isinstance(args[1], Frame):
    raise Exception()
  if args[0].table != args[1].table:
    raise Exception()
  if args[0].table != ctx.table: # Is this check necessary?
    raise Exception()

  df = ctx.df.copy()
  def apply(row):
    return row[args[0].name] / (row[args[1].name] + 0.001)
  df[name] = ctx.df.apply(apply, axis=1)

  result = ctx.table.create_subframe(name, ctx.table.get_keys())
  result.fill_data(df)
  return result


def call_equal(value, second):
  return value == second

def call_notnull(value):
  return pd.notna(value)

functions = {
  'MATH_DIVIDE': {
    'call': math_divide,
    'num_args': 2,
    'takes_pivots': False,
  },
  'MATH_EQUAL': per_value(call_equal, fillna=False, dtype=np.bool, num_args=2),
  'MATH_NOTNULL': per_value(call_notnull, fillna=False, dtype=np.bool, num_args=1),
}
