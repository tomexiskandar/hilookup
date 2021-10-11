# hilookup
This module is built to solve complex word matching.

It is powered by a fuzzy matching module from https://github.com/seatgeek/fuzzywuzzy and it extends its functions to:

•	provide user based assumption on word parternising e.g. defining significant column,   word grouping and words order and their score weighting.

•	provide minimum and penalty rate for fuzzy score

•	provide a way to control score for example by degrading value of unimportant word

•	provide character cleansing

This module provides simple and complex data matching situations. At the moment this module can only infer a matching based upon text similarity. A client code (your code to implement this module) need to be developed (see hilookup_test.py in samples folder as an example) in order to use this module properly.

## How to get started
1. download the package under dist folder, choose one eg. hilookup-0.1.0.tar.gz
2. install the package in your machine using pip.
```
 pip install path/to/hilookup-0.1.0.tar.gz
 ```
 >other required packages need to be installed are pandas, openpyxl, fuzzywuzzy, python-Levenshtein
3. download the files under samples folder

- Release 1 - Food details file.xlsx (as the source/master data for this test)
- target_data.xlsx (as the target/user data to match to the source)
- hilookup_test.py (a python script to run the matching)
- results_[timestamp].xlsx (the result of this test). To present the results properly, column _rank and group must be sorted (Smallers to Largest) accordingly. 


[to be continued....]