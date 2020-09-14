# Covid Data Challenge

These modules (Orchestration.py and DataPreprocessing.py) can be used to process and join the two data sets given in the coding challenge.

## Requirements

1. pandas
2. numpy
3. os
4. requests
5. errno

## Usage
To run, simply call run like so at the command line:
```bash
> python Orchestration.py
```

## Learnings
1. Because both of these data sources are coming from different sources, they have their own nuances. For instance, the fips code was readily available in the covid data, but needed to be constructed in the population data. 
2. I've had issues with reading zip codes in the past as the leading zero drop off. Fips code is no different.

## Sanity Checks
1. Check to make sure there are no NULL values for the fips code
2. Confirm no unreasonable values (negative value or non-integer values) were in the case and death count columns
## Feature to Add
1. If I had more time, I would have performed the cumulative sum for both the case count and death toll using pandas instead of SQLite. 
2. Added logging to better know when and where a potential failure happened.
3. I would add more options for outputs. Right now, the output is simply being written to a CSV. 