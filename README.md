# Distributed Nearest Neighbours Mean Shift with Locality Sensitive Hashing DNNMS-LSH

This algorithm purpose is to provide an efficient distributed implementation to cluster large multivariate multidimensional data sets (Big Data)  Nearest neighbor mean shift (NNMS) defines clusters in terms of locally density regions in the data density. The main advantages of NNMS are that it can **detect automatically** the number of clusters in the data set and **detect non-ellipsoidal** clusters, in contrast to k-means clustering. Exact nearest neighbors calculations in the standard NNMS prevent from being used on Big Data so we introduce approximate nearest neighbors via Locality Sensitive Hashing (LSH), which are based on random scalar projections of the data. To further improve the scalability, we implement NNMS-LSH in the distributed Spark/Python ecosystem.

**This Python version doesn't include last improvements available in the Spark/Scala version of this algorithm.**

### Parameters

* **k** is the number of neighbours to look at in order to compute centroid.
* **nbseg**  is the number of segments on which the data vectors are projected during LSH. Its value should usually be larger than 20, depending on the data set.
* **nbblocs**  is a crucial parameter as larger values give faster but less accurate LSH approximate nearest neighbors, and as smaller values give slower but more accurate approximations.
* **cmin**  is the threshold under which clusters with fewer than cmin members are merged with the next nearest cluster.
* **normalisation** is a flag if the data should be first normalized (X-Xmin)/(Xmax-Xmin)  before clustering.
* **w** is a uniformisation constant for LSH.
* **npPart** is the default parallelism outside the gradient ascent.
* **yStarIter** is the maximum number of iterations in the gradient ascent in the mean shift update.
* **threshold_cluster** is the threshold under which two final mean shift iterates are considered to be in the same cluster.

## Usage

### Multivariate multidimensional clustering
Unlike the image analysis which has a specific data pre-processing before the mean shift, for general multivariate multidimensional data sets, it is recommended to normalize data so that each variable has a comparable magnitude to the other variables to improve the performance in the distance matrix computation used to determine nearest neighbors.