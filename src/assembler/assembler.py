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
      print("Error with", ctx.current, tree['name'])
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

  def assemble_groupby(context, tree):
    groupby = None
    if tree.get('groupby'):
      for g in tree['groupby']:
        assemble_column(context, g)
      groupby = [f['name'] for f in tree['groupby']]
    return groupby

  def assemble_args(context, tree):
    ll = []
    for arg in tree.get('args'):
      if isinstance(arg, dict):
        ll.append(assemble_column(context, arg))
      else:
        ll.append(arg)
    return ll

  groupby = assemble_groupby(context, tree)
  args = assemble_args(context, tree)

  if fn['num_args'] != -1:
    assert len(args) == fn['num_args']
  if fn.get('takes_pivots', False):
    assert not groupby is None
  else:
    assert groupby is None

  if fn.get('takes_pivots', False):
    result = fn['call'](context, tree['name'], args, groupby)
  else:
    result = fn['call'](context, tree['name'], args)

  return result


@assemble_column_log_errors
@assert_returns_frame
def assemble_column(ctx, tree):
  # If column was already generated, stop.
  # NOTE Something won't work here because of the A.b.c vs. B.c!!!!!
  if ctx.currHasColumn(tree.get('name')):
    result = ctx.create_subframe(tree['name'])
    result.fillData(ctx.df)
    return result

  if tree.get('is_terminal'):
    if tree.get('function'):
      result = assemble_function(ctx, tree)
      ctx.currMergeFrame(result)
      return result

    else:
      print(tree['this'], ctx.current)
      assert tree['this'] in ctx.df.columns, "Terminal node isn't a " \
      "function, so expected a string that belongs to the dataframe."

      result = ctx.create_subframe(tree['this'])
      result.fillData(ctx.df)
      return result

  # If the tree isn't terminal, it has a child. Update the context appropriately
  # and call assemble_column on the child. First, figure out the appropriate
  # dataframe for the current node.

  assert 'next' in tree, "Non-terminal notes must have a child"

  if tree.get('root'):
    oldCurrent = ctx.swapIn(tree['root'])

    childResult = assemble_column(ctx, tree['next'])
    ctx.swapIn(oldCurrent)

    return childResult.getWithNamedRoot(tree['root'])

  # Handle the implicit 1-to-1 join.
  # Pure tables can't have dots in their column names, so being here means that
  # something of the kind `Sales.item.price` is happening, where `Sales.item`
  # is a relationship endpoint from the Sales to the Items table. In this case,
  # tree['this'] == 'item', and tree['next'] points to the tree for 'price'.

  tableOut = ctx.current
  keyOut = tree['this']

  # Find table and key to join into from context.
  rel = ctx.getRelationshipOnField(tableOut, keyOut)
  if not rel:
    raise Exception('Relationship at %s.%s not found' % (tableOut, keyOut))
  [_, _, tableIn, keyIn] = rel

  # Execute child with context of tableIn.
  ctx.swapIn(tableIn)
  childResult = assemble_column(ctx, tree['next'])
  ctx.swapIn(tableOut)

  nestedChild = childResult.getAsNested(ctx, tableOut, keyOut)

  if nestedChild.name in ctx.df.columns:
    # Not expected, as the condition of it already being in columns should've
    # triggered the `tree['name'] in ctx.df.columns` check above.
    raise Exception()

  rightOn = '%s.%s' % (keyOut, keyIn)

  ctx.df = pd.merge(ctx.df, \
    nestedChild.get_stripped(), \
    left_on=keyOut, \
    right_on=rightOn, \
    how='left', \
    suffixes=(False, False))

  # If we don't drop the right_on key, it will stick to the merged dataframe, so
  # trying to use the same relationship again might throw a column overlap
  # error. For instance, if we expand first `items.category.type` and then
  # `items.category.sub_type`, unless we explicitly drop `items.id` (ie rightOn)
  # below, Pandas will throw an error.
  ctx.df = ctx.df.drop(rightOn, axis=1)

  result = ctx.create_subframe(tree['name'])
  result.fillData(ctx.df)
  return result
