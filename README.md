
# Assembler

Given a list of feature descriptions and a corresponding set of relational
tables, automatically assembles a data set with those features.

### Problems?

"customer.flex_plans" has pivots [customer, CMONTH(date)]. That seems wrong – why would CMONTH(date) be there?


### Setup

```
virtualenv venv -p python3.7
```

### Developer gui

- `assembler/globals` List of manipulation functions.
