# Programming Assignment 4

- Mohit Sai Gutha  
- mohitsai@bu.edu  
- U48519832

## Key Components, Workflow and Design Decisions

### ColumnStats.hpp

#### Private Members

1. `unsigned bucketCount`: Stores the number of buckets in the histogram. Using unsigned ensures a non-negative count.
2. `int min`: Represents the minimum value in the column. Integer type allows for both positive and negative values.
3. `int max`: Represents the maximum value in the column. Integer type allows for both positive and negative values.
4. `int bucketWidth`: Stores the width of each bucket. Integer type is used for precise bucket calculations.
5. `std::vector<size_t> histogram`: Stores the frequency of values in each bucket. Vector allows for dynamic sizing, and size_t is used for counting large numbers of entries.
6. `size_t tupleCount`: Keeps track of the total number of values added. Size_t is used to handle large datasets.

### ColumnStats.cpp

- Initializes the ColumnStats object with the given number of buckets, minimum, and maximum values.
- Calculates the bucket width based on the range and number of buckets.
- Initializes the histogram vector with the specified number of buckets, all set to 0.
- Sets the initial tuple count to 0.

#### Constructor

- Initializes the ColumnStats object with the given number of buckets, minimum, and maximum values.
- Calculates the bucket width based on the range and number of buckets.
- Initializes the histogram vector with the specified number of buckets, all set to 0.
- Sets the initial tuple count to 0.


#### addValue

- Checks if the input value is within the specified range (min to max).
- Throws an out_of_range exception if the value is outside the range.
- Calculates the appropriate bucket index for the value and increments its count in the histogram.
- Increments the total tuple count.


#### estimateCardinality

- Estimates the number of tuples that satisfy a given predicate operation and value.
- Handles different predicate operations (EQ, GT, GE, LT, LE, NE) with separate logic for each.
- Uses the histogram data to estimate cardinality without scanning the entire dataset.
- Returns 0 if there are no tuples in the column.
- For EQ (equal) operation:
- - Estimates by dividing the bucket count by bucket width, assuming uniform distribution within the bucket.
- For GT (greater than) operation:
- - Handles edge cases where v is less than min (returns all tuples) or greater than max (returns 0).
- - For values within range, calculates partial counts for the bucket containing v and adds counts for all higher buckets.
- For GE (greater than or equal) operation:
- - Similar to GT, but includes the exact value v in the calculation.
- For LT (less than) operation:
- - Handles edge cases where v is less than or equal to min (returns 0) or greater than max (returns all tuples).
- - For values within range, calculates partial counts for the bucket containing v and adds counts for all lower buckets.
- For LE (less than or equal) operation:
- - Combines the results of EQ and LT operations for a more accurate estimate.
- For NE (not equal) operation:
- - Subtracts the EQ estimate from the total tuple count.

#### Limitations and Issues
Since it is not a part of the requirements of the assignment, I did not take into account where the input values would be invalid. Though it would be a good practice to implement checks regardless, I failed to do so for this particular assignment owing to time constraints. 


#### Notes

1. I worked on PA4 individually.
2. Initially I passed only two test cases, so I discussed some potential implementation techniques with my peers to figure out where my code was going wrong.
3. I used chatGPT 4o model on perplexity AI to debug, understand existing parts of code and also to help rephrase and refine parts of my documentation.
4. The analytical questions will be answered on gradescope.