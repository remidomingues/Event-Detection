Event detection
===============
Overview
--------
This project aims at evaluating clustering algorithms in the context of event detection applied to Tweets content.
The results of these algorithms are compared to the real data set.

This project could lead to the development of a first story detection software using a Twitter stream.


Technologies
------------
- Java 1.6 or higher
- SQLite
- Lucene core library: http://lucene.apache.org/core


Algorithms
----------
- **K-Means**: this algorithm has been pre-processed using Knime and the correct number of clusters. If the results are convincing, it will be replaced by similar algorithms which doesn't need a cluster number.
- A clustering algorithm based on **cosine similarity** here implemented in Java.