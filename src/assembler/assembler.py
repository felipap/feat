# TODO: rename context to namespace????

import types
import time
from functools import wraps

import numpy as np
import pandas as pd

from ..common import Context, Frame
from ..globals import globals as globalFunctions

TIME_COL = 'month_block'

def logErrors(function):
  @wraps(function)
  def foo(ctx, tree):
    try:
      result = function(ctx, tree)
    except Exception as e:
      print("Error with", ctx.current, tree['name'])
      raise e

    # assert result.__class__.__name__ == 'Frame', result.__class__.__name__
    # assert isinstance(result, Frame) # Won't work with jupyter autoreload.

    # assert isinstance(result, Frame), "%s isn't Frame" % result
    return result
  return foo

def assertReturnsFrame(function):
  @wraps(function)
  def foo(*args, **kwargs):
    result = function(*args, **kwargs)

    assert result.__class__.__name__ == 'Frame', result.__class__.__name__
    # assert isinstance(result, Frame) # Won't work with jupyter autoreload.

    # assert isinstance(result, Frame), "%s isn't Frame" % result
    return result
  return foo

# @assertReturnsFrame
def execFunction(context, tree):
  keyword = tree['function']
  if tree['function'] not in globalFunctions:
    raise Exception('Unregistered function %s' % tree['function'])

  groupby = None
  if tree.get('groupby'):
    for g in tree['groupby']:
      assemble(context, g)

    groupby = [f['name'] for f in tree['groupby']]

  def generateClosure(childTree):
    # We pass a 'closured' function to make sure globalFunctions won't try to do
    # something funny.
    return assemble(context, childTree)

  result = globalFunctions[tree['function']](context, tree, generateClosure, groupby)

  # assert result.__class__.__name__ == 'Frame', result.__class__.__name__
  # assert isinstance(result, Frame) # Won't work with jupyter autoreload.

  return result


@logErrors
@assertReturnsFrame
def assemble(ctx, tree):
  if tree.get('is_terminal'):
    if tree.get('name') in ctx.df.columns:
      result = Frame(ctx.current, ctx.pivots[ctx.current], tree['name'])
      result.fillData(ctx.df)
      return result

    if tree.get('function'):
      result = execFunction(ctx, tree)

      # display("now is", result.getStripped())
      # import builtins
      # builtins.fuck = result.getStripped()
      # display(ctx.df)

      # FIXME Even if df of child is different (eg. Items.MEAN(Sales.item)), we are
      # merging based on the child's columns, without any warning or thinking about it
      # deeper

      if not set(result.pivots).issubset(ctx.df.columns):
        raise Exception('Result can\'t be merged into current dataset: ', \
          result.pivots, ctx.df.columns)

      if result.colName not in ctx.df.columns:
        ctx.df = pd.merge(ctx.df, \
          result.getStripped(), \
          on=list(result.pivots), \
          how='left', \
          suffixes=(False, False))

      return result
    else:
      print(tree['this'], ctx.current)
      assert tree['this'] in ctx.df.columns, "Terminal node isn't a " \
      "function, so expected a string that belongs to the dataframe."

      result = Frame(ctx.current, ctx.pivots[ctx.current], tree['this'])
      result.fillData(ctx.df)
      return result

  # If column was already generated, stop.
  # NOTE Something won't work here because of the A.b.c vs. B.c!!!!!
  if tree['name'] in ctx.df.columns:
    # if 'this' not in tree:
    #   print(tree)
    result = Frame(ctx.current, ctx.pivots[ctx.current], tree['name'])
    result.fillData(ctx.df)
    return result

  # If the tree isn't terminal, it has a child. Update the context appropriately
  # and call assemble on the child. First, figure out the appropriate dataframe
  # for the current node.

  assert 'next' in tree, "Non-terminal notes must have a child"
  if tree.get('root'):
    oldCurrent = ctx.swapIn(tree['root'])
    childResult = assemble(ctx, tree['next'])
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
  childResult = assemble(ctx, tree['next'])
  ctx.swapIn(tableOut)

  nestedChild = childResult.getAsNested(tableOut, keyOut)

  if nestedChild.colName in ctx.df.columns:
    # Not expected, as the condition of it already being in columns should've
    # triggered the `tree['name'] in ctx.df.columns` check above.
    raise Exception()

  rightOn = '%s.%s' % (keyOut, keyIn)

  ctx.df = pd.merge(ctx.df, \
    nestedChild.getStripped(), \
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

  result = Frame(ctx.current, ctx.pivots[ctx.current], tree['name'])
  result.fillData(ctx.df)
  return result
