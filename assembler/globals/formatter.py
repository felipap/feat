
import numpy as np
import pandas as pd

from .lib.per_value import per_value
from .lib.lib import fancy_apply, can_collapse_date, uncollapse_date, assert_constant_nrows

@assert_constant_nrows
def EMAIL_DOMAIN(ctx, name, args):
  child = args[0]

  df = child.get_stripped()

  if child.get_date_col():
    date_field = child.get_date_col()
    collapse = can_collapse_date(child, date_field)
    if collapse:
      df = df.drop(date_field, axis=1).drop_duplicates()

  df.rename(columns={ child.name: name }, inplace=True)

  def get_domain(row):
    if row[name] and pd.notna(row[name]) and '@' in row[name]:
      return row[name].split('@')[1].lower()
    return None
  df[name] = fancy_apply(df, get_domain, axis=1)

  if collapse:
    df = uncollapse_date(name, df, child, date_field)

  result = ctx.table.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result

@assert_constant_nrows
def DOMAIN_EXT(ctx, name, args):
  child = args[0]

  df = child.get_stripped()

  collapsed = False
  if child.get_date_col():
    date_field = child.get_date_col()
    if can_collapse_date(child, date_field):
      collapsed = True
      df = df.drop(date_field, axis=1).drop_duplicates()
  df.rename(columns={ child.name: name }, inplace=True)

  def get_domain_ext(row):
    if row[name] and pd.notnull(row[name]) and '.' in row[name]:
      return '.'.join(row[name].split('.')[1:])
    return None
  df[name] = fancy_apply(df, get_domain_ext, axis=1)
 
  if collapsed:
    df = uncollapse_date(name, df, child, date_field)

  result = ctx.table.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result
  

def call_email_domain(value, _):
  # if value and pd.notnull(value):
  #   return 'AHAHA'+value
  # return None
  if value and pd.notna(value) and '@' in value:
    return value.split('@')[1].lower()
  return None

def call_domain_ext(value, _):
  if value and pd.notnull(value) and '.' in value:
    return '.'.join(value.split('.')[1:])
  return None

functions = {
  # 'EMAIL_DOMAIN': dict(call=EMAIL_DOMAIN, num_args=1, takes_pivot=False),
  'EMAIL_DOMAIN': per_value(call_email_domain, fillna=None),
  # 'DOMAIN_EXT': dict(call=DOMAIN_EXT, num_args=1, takes_pivot=False),
  'DOMAIN_EXT': per_value(call_domain_ext, fillna=None),
}
