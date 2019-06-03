# TODO: rename context to namespace????

import types
import time
from functools import wraps

import numpy as np
import pandas as pd

from ..common import Context, Frame, assert_returns_frame
from ..globals import getFunction

def assemble_column_log_errors(inner):
  @wraps(inner)
  def outer(ctx, tree):
    try:
      result = inner(ctx, tree)
    except Exception as e:
      print("^ Error with", ctx.current, tree['name'])
      raise e
    assert result.__class__.__name__ == 'Frame', result.__class__.__name__
    # assert isinstance(result, Frame) # Won't work with jupyter autoreload.
    # assert isinstance(result, Frame), "%s isn't Frame" % result
    return result
  return outer

@assert_returns_frame
def assemble_function(context, tree):
  keyword = tree['function']

  fn = getFunction(keyword)
  if not fn:
    raise Exception('Unregistered function %s' % keyword)

  def assemble_groupbys(context, tree):
    groupby = None
    if tree.get('groupby'):
      for g in tree['groupby']:
        assemble_column(context, g)
      groupby = [f['name'] for f in tree['groupby']]
    return groupby

  def assemble_args(context, tree):
    ll = []
    for arg in tree.get('args'):
      if isinstance(arg, dict): # REVIEW document this at least, very shaky
        ll.append(assemble_column(context, arg))
        # print("r is", r, r.df.columns, context.df.columns)
      else:
        ll.append(arg)
    return ll

  groupby = assemble_groupbys(context, tree)
  args = assemble_args(context, tree)

  if fn['num_args'] != -1:
    assert len(args) == fn['num_args']
  if fn.get('takes_pivots', False):
    pass # pivots are optional
  else:
    assert groupby is None

  if fn.get('takes_pivots', False):
    result = fn['call'](context, tree['name'], args, groupby)
  else:
    result = fn['call'](context, tree['name'], args)

  return result


def fetch_existing_subframe(ctx, tree):
  # When the column for the current tree node has already been generated
  # (ie. tree['name'] is in dataframe.columns), we can use the column already
  # in the dataframe to create a Frame, but we need to use the appropriate
  # pivots. Eg. if "SUM(..|CMONTH(date),shop)" is already in the dataframe, we
  # need to create a Frame() using ['CMONTH(date)', 'shop'] as pivots.

  # Original columns of the dataframe have the table pivots as their pivot.
  if tree['name'] in ctx.original_columns[ctx.current]:
    result = ctx.create_subframe(tree['name'], ctx.get_pivots_for_table(ctx.current))
    result.fill_data(ctx.df)
    return result

  # If the column is not original, ctx.get_pivots_for_frame gives cached
  # information about frames we have already seen.
  # REVIEW good idea?
  result = ctx.create_subframe(tree['name'], ctx.get_pivots_for_frame(tree['name']))
  result.fill_data(ctx.df)
  return result

@assemble_column_log_errors
@assert_returns_frame
def assemble_column(ctx, tree):
  # If column was already generated, stop.
  if ctx.currHasColumn(tree.get('name')):
    return fetch_existing_subframe(ctx, tree)

  if tree.get('is_terminal'):
    if 'function' not in tree:
      print("tree is", tree)
      raise Exception('Does this ever happen?')
      # assert tree['this'] in ctx.df.columns, "Terminal node isn't a " \
      # "function, so expected a string that belongs to the dataframe."
      # result = ctx.create_subframe(tree['this'])
      # result.fill_data(ctx.df)
      # return result
    result = assemble_function(ctx, tree)
    if not ctx.currHasColumn(result.name):
      ctx.merge_frame_with_df(result, on=list(result.pivots))

    return result

  # If the tree isn't terminal, it has a child. Update the context appropriately
  # and call assemble_column on the child. First, figure out the appropriate
  # dataframe for the current node.

  assert 'next' in tree, "Non-terminal notes must have a child"

  if tree.get('root'):
    oldCurrent = ctx.swapIn(tree['root'])
    childResult = assemble_column(ctx, tree['next'])
    ctx.swapIn(oldCurrent)

    mapping = tree.get('translation', {}).get('map_str')
    childResult.translate_pivots_root(ctx, ctx.current, mapping)
    childResult.rename(tree['name'])

    # merge_frame_with_df will return an expanded version of childResult, with
    # combinations of pivots that might not be in the original childResult. We
    # return that because merge_frame_with_df will fillna() those new values,
    # which is something that we need to pass to the parent nodes.
    return ctx.merge_frame_with_df(childResult, on=list(childResult.pivots))

  # Handle the implicit 1-to-1 join.
  # Pure tables can't have dots in their column names, so being here means that
  # something of the kind `Sales.item.price` is happening, where `Sales.item`
  # is a relationship endpoint from the Sales to the Items table. In this case,
  # tree['this'] == 'item', and tree['next'] points to the tree for 'price'.

  tableOut = ctx.current
  keyOut = tree['this']

  # Find table and key to join into from context.
  rel = ctx.findGraphEdge(tableOut=tableOut, colOut=keyOut)
  if not rel:
    raise Exception('Relationship at %s.%s not found' % (tableOut, keyOut))
  [_, _, tableIn, keyIn] = rel[0]

  # Execute child with context of tableIn.
  ctx.swapIn(tableIn)
  childResult = assemble_column(ctx, tree['next'])
  ctx.swapIn(tableOut)

  childResult.rename(tree['name'])

  if childResult.name in ctx.df.columns:
    # Not expected, as the condition of it already being in columns should've
    # triggered the `tree['name'] in ctx.df.columns` check above.
    raise Exception()

  ctx.merge_frame_with_df(childResult, left_on=keyOut, right_on=keyIn)

  # If we don't drop the right_on key, it will stick to the merged dataframe, so
  # trying to use the same relationship again might throw a column overlap
  # error. For instance, if we expand first `items.category.type` and then
  # `items.category.sub_type`, unless we explicitly drop `items.id` (ie keyIn)
  # below, Pandas will throw an error.
  # print("Result", ctx.df.columns, childResult.get_stripped().columns)

  # print("end result is", ctx.df.columns, childResult, "\n\n")

  result = ctx.create_subframe(tree['name'], ctx.get_pivots_for_table(ctx.current))
  result.fill_data(ctx.df)
  return result
