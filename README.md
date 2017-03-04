# A sequence clustering method

## seq2vec
Transform the unfixed-length temporal sequence into fixed vectors. The core idea is to select some
events for each role under a certain number, like top-5, so that each vector contains the same length
dimensions. The main issues are here below.

* **Event index**: If simply put the frequencies in, the initial levels show a huge difference from the 
higher ones on freq. 
To avoid it I scaling each freq within the range of its column with other freqs of the same events. Then 
scale it to (0, 100) for a more apparent effect during clustering.

* **Unaligned**: After all events are scaled, the top-k are remained in each level, which leads to a 
unaligend result. The vector is like
`motion1, val1, motion2, val2, user1`
If we take 40 levels, then it will be 40 * 5 * 2 dimensions, which include categorical ones.
Categorical is fine for classfying model like tree, but clustering need further processing.

## dimred
This step aims to solve 2 issues, the *alignment* and *dimensionally reduction*.
* **Alignment**: To align the data generated ahead of this, we need to expand each group (level here)
to a event union. Then the originally top-k motion will be expanded to the union size and the categorical
dims can be dropped.
But union may cause the a disaster of dimensions.

* **Dimensionally reduction**: Now dimred work have to be done. The final step is to cluster the whole
things so we consider clustering each dimension. Then each col gets a score like cohesion. Then sorted, 
reduce and keep the left. Now a reasonabel data is ready for clustering
During column clustering process, another degree of parallelism is provided out of RDD. Scala has parallel
collection lib that is cool for this. That causes the spark driver parallels the schedulation of many RDDs.
