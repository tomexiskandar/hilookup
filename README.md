# hilookup
This module is built to solve complex word matching.

It is powered by a fuzzy matching module from https://github.com/seatgeek/fuzzywuzzy and it extends its functions to:

•	provide user based assumption on word parternising e.g. defining significant column, word grouping and words order and their score weighting

•	provide minimum and penalty rate for fuzzy score

•	provide a way to control score for example by degrading value of unimportant word

•	provide character cleansing

This module provides low level and broader use of different data matching situations. At the moment this module can only infer a matching based upon text similarity. A client code (your code to implement this module) need to be developed in order to use this module properly.
[to be continued...]
