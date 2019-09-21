
from datetime import datetime
from dateutil.relativedelta import relativedelta

def date_to_cmonth(date):
  if isinstance(date, str):
    date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
  assert isinstance(date, datetime)
  return (date.year-1970)*12+date.month

def date_to_cweek(date):
  # https://stackoverflow.com/questions/2600775
  assert isinstance(date, datetime)
  year, week = date.isocalendar()[:2]
  return (year-1970)*52+week

def cweek_to_date(cweek):
  year = cweek // 52 + 1970
  nweek = cweek % 52
  return datetime(year, 1, 1) + relativedelta(weeks=nweek-1)

def cmonth_to_date(cmonth):
  base_date = datetime(1970, 1, 1)
  date = base_date + relativedelta(months=cmonth-1)
  return date

def date_yearmonth(date):
  return '%d-%02d' % (date.year, date.month)

def yearmonth_date(ym):
  return datetime.strptime(ym, '%Y-%m')

# def encode_cmonth(date_code):
#   year, month = map(int, date_code.split('-'))
#   foo = (year-1970)*12+month
#   print(date_code, foo)
#   return foo

# def decode_cmonth(cmonth):
#   base_date = datetime(1970,1,1)
#   date = base_date + relativedelta(months=cmonth-1)
#   return '%d-%02d' % (date.year, date.month) # TODO use 
#   print("cmonth %s is %s" % (cmonth, '%d-%02d'))


# TODO turn this into actual tests
now = datetime.now()
month_start = datetime(year=now.year, month=now.month, day=1)
print(month_start, date_to_cmonth(month_start), cmonth_to_date(date_to_cmonth(month_start)))
assert cmonth_to_date(date_to_cmonth(month_start)) == month_start