# hilookup
This module is built to solve complex word matching.

It is powered by a fuzzy matching module from https://github.com/seatgeek/fuzzywuzzy and it extends its functions to:

•	provide user based assumption on word parternising e.g. defining significant column, word grouping and words order and their score weighting

•	provide minimum and penalty rate for fuzzy score

•	provide a way to control score for example by degrading value of unimportant word

•	provide character cleansing

This module provides low level and broader use of different data matching situations. At the moment this module can only infer a matching based upon text similarity. A client code (your code to implement this module) need to be developed in order to use this module properly.

## Patternising your words

The outcome of this process is to create a pattern based upon your observations on dataset. The challenge is when the structure of the dataset is not consistent and then you should evaluate the best arguments/parameters (a set of values as input to HILookup to control its behaviour) and/or to re-work on your dataset if required.
Let’s start with the example from Spams of AFM Online. The hierarchy path column is the one we are interested in to compare with target/external dataset.
From a space data ‘\Sections\Spams\Foo Bar’ can be restructured in different ways and in order to hypothetically closer with its corresponding structure in the target by grouping them and ordering them. So without further ado, the possible pattern from the data above could be (a comma used as separator a single data structure):
1.	Foo Bar, Bar, Foo, Spams
2.	Foo Bar, Foo, Bar, Spams
3.	Sections, Spams, Foo Bar, Foo, Bar

[to be continued...]
