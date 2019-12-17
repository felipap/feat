
from .window import call_window_sum

rows = {
  55: None,
  56: {'__date__': 56, '_value_': 1, 'customer': 'abc'},
  57: {'__date__': 57, '_value_': 3, 'customer': 'abc'},
  58: {'__date__': 58, '_value_': 0, 'customer': 'abc'},
  59: {'__date__': 59, '_value_': 0, 'customer': 'abc'},
  60: None,
  61: None,
  62: {'__date__': 62, '_value_': 1, 'customer': 'abc'},
  63: None,
  64: None,
  65: {'__date__': 65, '_value_': 2, 'customer': 'abc'},
  66: {'__date__': 66, '_value_': 3, 'customer': 'abc'},
  67: {'__date__': 67, '_value_': -2, 'customer': 'abc'},
  68: {'__date__': 68, '_value_': 0, 'customer': 'abc'},
  69: {'__date__': 69, '_value_': 0, 'customer': 'abc'},
  70: {'__date__': 70, '_value_': 0, 'customer': 'abc'},
}


def test_window_sum():
  assert(call_window_sum(rows, 2) == {
    55: None,
    56: 1,
    57: 4,
    58: 3,
    59: 0,
    60: 0,
    61: None,
    62: 1,
    63: 1,
    64: None,
    65: 2,
    66: 5,
    67: 1,
    68: -2,
    69: 0,
    70: 0,
  })

