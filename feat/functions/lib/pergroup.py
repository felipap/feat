
from timeit import default_timer as timer
from functools import wraps
import multiprocessing

import pandas as pd
import numpy as np

from .groupby_combine import split_respecting_boundaries, process_df_chunk

SPLIT = 0

def make_pergroup(innerfn, fillna=0):
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
      raise Exception()

    if SPLIT > 1:
      # Make innerfn application faster by splitting it into multiple chunks and
      # running it in parallel. REVIEW is this really faster?
      df_chunks = split_respecting_boundaries(df, keyminustime, 4)
    else:
      df_chunks = [df.to_dict('records')]

    chunks = [{
      "records": df_chunk,
      "columns": keyminustime,
      "date_counts": date_counts,
      "function": innerfn,
      # TODO SUPPORT PIVOTS
      "time_col": "__date__", # "CMONTH(date)",
    } for df_chunk in df_chunks]

    start = timer()

    if SPLIT > 1:
      pool = multiprocessing.Pool()
      results = pool.map(process_df_chunk, chunks)
    else:
      results = list(map(process_df_chunk, chunks))
    
    final = pd.DataFrame(list(np.hstack(results)))
    # print(final)
    print("elapsed: %ds" % (timer() - start))
    final.rename(columns={ '_tcount_': child.get_date_col(), '_result_': name }, inplace=True)

    result = ctx.table.create_subframe(name, pivots)
    result.fill_data(final, fillnan=fillna)
    return result
  
  return {
    'call': magic,
    'takes_pivots': True,
    'num_args': 1,
  }