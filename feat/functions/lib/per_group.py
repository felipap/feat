
from timeit import default_timer as timer
from functools import wraps
from typing import List, Dict, Tuple
import multiprocessing
import pandas as pd
import numpy as np
from timeit import default_timer as timer

SPLIT = 0
BE_THOROUGH = True

# TODO validate the functions regarding the arguments they take
# https://github.com/tensorflow/tensorflow/blob/master/tensorflow/python/framework/function.py#L130


def get_window_values(end, n, rows):
  """
  Return the `n` consecutive values ending on index `end`.
  """
  
  window = []
  for index in range(end-n+1, end+1):
    item = rows.get(index)
    if not item:
      window.append(None)
    elif pd.notna(item['_value_']):
      window.append(item['_value_'])
    else:
      window.append(None)
  return window


def _process_chunk(chunk):
  """
  Run a user-defined function over the groups in a list. Groups meaning an array
  of records that share the same set of key values. All required info to do so
  must be passed as a single argument (eg. a dictionary), so that this function
  can be easily parallelized using multiprocessing.Pool.
  """
  
  columns = chunk['columns']
  date_counts = chunk['date_counts']
  groups = chunk['groups']
  args = chunk['args']
  function = chunk['function']
  time_col = chunk['time_col']

  def process_group(rows):
    keys = { col: rows[0][col] for col in columns }
    asdict = { r[time_col]: r for r in rows }
    datetovalue = { d: asdict.get(d) for d in date_counts }

    if args is None:
      results = function(datetovalue)
    else:
      results = function(datetovalue, *args)

    ret = []
    for (tcount, row) in datetovalue.items():
      ret.append(dict(keys, _tcount_=tcount, _result_=results.get(tcount)))
    return ret

  if not groups:
    return []

  # For each group, call `dogroup_flat`, which wraps `function`.
  result = []
  for group in groups:
    result.extend(process_group(group))
  return result


def _get_groups(df, keyminustime):
  """
  Split a pandas dataframe into groups that share the same key combination.
  """

  # Something must be wrong. Creating groups by looping df.groupby(keyminustime)
  # is taking 80x what's taking this hard-coded alternative:
  # for keys, group in df.groupby(keyminustime):
  #   groups1.append(group.to_dict('records'))
  
  groups = []
  records = df.sort_values(keyminustime).to_dict('records')
  # Loop records sorted by their key values. Use `last_key_values` and `this`
  # to accumulate all the records with the same key values.
  last_key_values = tuple(records[0][col] for col in keyminustime)
  accumulated = []
  for record in records:
    this = tuple(record[col] for col in keyminustime)
    if last_key_values == this:
      accumulated.append(record)
    else:
      groups.append(accumulated)
      accumulated = [record]
      last_key_values = this
  if accumulated:
    groups.append(accumulated)
  
  return groups


def make_per_group(innerfn, num_args=1, fillna=0, dtype=None):
  """
  Wraps around a function that takes as argument a *group*, that is, a set of
  rows with the same key(s).
  """
  
  @wraps(innerfn)
  def magic(ctx, name, args, pivots=None):
    child = args[0]

    df = child.get_stripped()
    pivots = pivots or child.pivots
    # df = df[df['customer']=='5b69c4250998ba2b42de9de2']

    df.rename(columns={ child.name: '_value_' }, inplace=True)

    keyminustime = list(set(pivots)-{child.get_date_col()})
    date_counts = sorted(ctx.get_date_range())

    # Make sure that the date values found in this dataframe are a subset of
    # the values from ctx.get_date_range().
    if not set(df[child.get_date_col()].unique()).issubset(date_counts):
      print(set(df[child.get_date_col()].unique()) - set(date_counts))
      # raise Exception()

    start = timer()
    groups = _get_groups(df, keyminustime)
    # print("other one took: %ds" % (timer() - start))

    if SPLIT > 1:
      # Make innerfn application faster by splitting it into multiple chunks and
      # running it in parallel. REVIEW is this really faster?
      chunks = list(map(list, np.array_split(groups, SPLIT)))
    else:
      chunks = [groups]

    chunks = [dict(
      groups=groups,
      columns=keyminustime,
      date_counts=date_counts,
      function=innerfn,
      args=None if num_args == 1 else args[1:],
      time_col="__date__", # TODO SUPPORT PIVOTS
    ) for chunk in chunks]

    if SPLIT > 1:
      pool = multiprocessing.Pool()
      results = pool.map(_process_chunk, chunks)
    else:
      results = list(map(_process_chunk, chunks))
    
    concatenated = list(np.hstack(results))
    final = pd.DataFrame(concatenated)

    # print("per_group took: %ds" % (timer() - start))
    final.rename(columns={ '_tcount_': child.get_date_col(), '_result_': name }, inplace=True)

    if dtype:
      print("using dtype %s" % repr(dtype))
      final[name] = final[name].astype(dtype)

    result = ctx.table.create_subframe(name, pivots)
    result.fill_data(final, fillnan=fillna)
    return result
  
  return {
    'call': magic,
    'takes_pivots': True,
    'num_args': num_args,
  }