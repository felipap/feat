# https://github.com/lark-parser/lark/blob/master/docs/lark_cheatsheet.pdf

from functools import wraps

from lark import Lark, Transformer, v_args, Token, Tree

def annotate_match(method):
  """Adds a 'name' field to the dictionary result of the decorated method, corresponding to
  the underlying matched text. Also adds a '_last' field to the dictionary, and sets it to
  the name of the decorated method."""

  warning = "decorator expects method to be annotated with @v_args(meta=True)"

  def objectIsLarkMeta(obj):
    return hasattr(obj, "empty")

  @wraps(method)
  def foo(self, *args):
    assert len(args) == 2, warning
    assert objectIsLarkMeta(args[1]), warning

    [children, meta] = args

    result = method(self, children, meta)
    assert type(result) == dict, "decorator expects method result to always be a dictionary"
    result["name"] = self.text[meta.start_pos: meta.end_pos]
    # result["_last"] = method.__name__
    return result

  return foo


class JSONifier(Transformer):
  def __init__(self, text):
      """text is the original command that was parsed, which we need inside
      column_call()
      """

      self.text = text

  command = v_args()(lambda self, x: x[0])

  @v_args(meta=True)
  def command_col(self, children, meta):
      return {
          "command": "command_col",
          "column": children[0],
      }

  @v_args(meta=True)
  def command_rel(self, children, meta):
      return {
          "command": "command_rel",
          "table1": children[0].value,
          "col1": children[1].value,
          "table2": children[2].value,
          "col2": children[3].value,
      }

  @v_args(meta=True)
  @annotate_match
  def root_column(self, children, _):
    assert len(children) == 1 or len(children) == 2
    # If : this_column
    if len(children) == 1:
      return children[0]
    # If : TABLE_NAME "." this_column
    assert type(children[0]) == Token
    return {
      "root": children[0].value,
      "next": children[1]
    }

  @v_args(meta=True)
  @annotate_match
  def this_column(self, children, meta):
    if len(children) == 1 and type(children[0]) == Token:
      assert children[0].type == 'LOWERCASE'
      return {
        "this": children[0].value,
        "is_terminal": True,
      }

    if len(children) == 2 and type(children[0]) == Token: # child[1] is from 'this_column'
      assert children[0].type == 'LOWERCASE'

      return {
        "this": children[0].value,
        "next": children[1],
        # "column": '%s.%s' % (children[0].value, children[1].column),
      }

    if len(children) == 1 and type(children[0]) == dict: # child[0] is from 'fn_call'
      return children[0]

    raise Exception()

  @v_args(meta=True)
  @annotate_match
  def fn_call(self, children, meta):
    assert children[0].type == 'FUNCTION_NAME'
    assert type(children[1]['args']) == list

    groupby = None
    if len(children) == 3:
      assert type(children[2]) == Tree and children[2].data == 'groupby'
      groupby = children[2].children

    return {
      "is_terminal": True,
      "function": children[0].value,
      "args": children[1]['args'],
      "groupby": groupby,
    }

  @v_args(meta=True)
  @annotate_match
  def fn_args(self, children, _):
    for child in children:
      assert child.data == 'fn_arg'
    return {
      "args": [child.children[0] for child in children]
    }

  array = list
  number = v_args(inline=True)(float)

def parseLineToCommand(string):
  grammar = open('./grammar.lark').read()

  json_parser = Lark(
    grammar,
    start='command',
    propagate_positions=True,
    debug=True)

  tree = json_parser.parse(string)
  # print("Tree is", tree, "\n", tree.data, dir(tree.data))
  return JSONifier(string).transform(tree)
