# NormalizedRatings_BD

Developing a Spark program to identify the T best products from a review dataset of an online retailer. 

DATA FORMAT: a review dataset is provided as a file with one review per row. A review consists of 4 comma-separated fields: ProductID (string), UserID (string), Rating (integer in [1,5] represented as a real), Timestamp (integer). An example of review is: B00005N7P0,AH2IFH762VY5U,5.0,1005177600 

TASK: Writing a program, which receives in input, as command-line arguments, two integers K and T, and path to a file storing a review dataset, and does the following things:

1-Reads the input set of reviews into an RDD of strings called RawData (each review is read as a single string), and subdivides it into K partitions.

2-Transform the RDD RawData into an RDD of pairs (String,Float) called normalizedRatings, so that for each string of RawData representing a review (ProductID,UserID,Rating,Timestamp), NormalizedRatings contains the pair (ProductID,NormRating), where NormRating=Rating-AvgRating and AvgRating is the average rating of all reviews by the user "UserID". To accomplish this step you can safely assume that there are a few reviews for each user. Note that normalizedRatings may contain several pairs for the same product, one for each existing review for that product! 

3-Transform the RDD normalizedRatings into an RDD of pairs (String,Float) called maxNormRatings which, for each ProductID contains exactly one pair (ProductID, MNR) where MNR is the maximum normalized rating of product "ProductID". The maximum should be computed either using the reduceByKey method or the mapPartitionsToPair/mapPartitions method. (Hint: get inspiration from the WordCountExample program). 

4-Print the T products with largest maximum normalized rating, one product per line. (Hint: use a combination of sortByKey and take methods.) 

