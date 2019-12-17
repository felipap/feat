
from .lib.per_group import get_window_values
from .future import call_within

rows = {
  2556: {'__date__': 2556, '_value_': 1, 'customer': 'abc'},
  2557: {'__date__': 2557, '_value_': 3, 'customer': 'abc'},
  2558: {'__date__': 2558, '_value_': 0, 'customer': 'abc'},
  2559: {'__date__': 2559, '_value_': 0, 'customer': 'abc'},
  2560: {'__date__': 2560, '_value_': 0, 'customer': 'abc'},
  2561: {'__date__': 2561, '_value_': 0, 'customer': 'abc'},
  2562: {'__date__': 2562, '_value_': 1, 'customer': 'abc'},
  2563: None,
  2564: None,
  2565: {'__date__': 2565, '_value_': 2, 'customer': 'abc'},
  2566: {'__date__': 2566, '_value_': 3, 'customer': 'abc'},
  2567: {'__date__': 2567, '_value_': -2, 'customer': 'abc'},
  2568: {'__date__': 2568, '_value_': 0, 'customer': 'abc'},
  2569: {'__date__': 2569, '_value_': 0, 'customer': 'abc'},
  2570: {'__date__': 2570, '_value_': 0, 'customer': 'abc'},
}

def test_get_window_values():
  # Handles zero values properly.
  assert(get_window_values(2567, 3, rows) == [2,3,-2])

  # Handles zero values properly.
  assert(get_window_values(2570, 3, rows) == [0,0,0])

  # Handles None rows properly.
  assert(get_window_values(2563, 3, rows) == [0,1,None])

  # Handles windows ending on the first date of the rows.
  assert(get_window_values(2556, 3, rows) == [None,None,1])


def test_within():
  assert(call_within(rows, 2) == {
    2556: True,
    2557: False,
    2558: False,
    2559: False,
    2560: True,
    2561: True,
    2562: False,
    2563: True,
    2564: True,
    2565: True,
    2566: True,
    2567: False,
    2568: False,
    2569: False,
    2570: False,
  })
  assert(call_within(rows, 3) == {
    2556: True,
    2557: False,
    2558: False,
    2559: True,
    2560: True,
    2561: True,
    2562: True,
    2563: True,
    2564: True,
    2565: True,
    2566: True,
    2567: False,
    2568: False,
    2569: False,
    2570: False,
  })



