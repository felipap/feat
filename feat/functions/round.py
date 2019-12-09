
from ..common.Frame import Frame


def round(ctx, name, args):
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


functions = {
  'ROUND': {
    'call': round,
    'num_args': 2,
    'takes_pivots': False,
  },
}
