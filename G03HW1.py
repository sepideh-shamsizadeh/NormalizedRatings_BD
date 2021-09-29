from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand


def map1(RawData):

    """
    Mapping for Round1
    input:
        RawData: 'ProductID,UserID,Rating,Timestamp'
    output:
        results: (UserID,[ProductID,float(Rating)])
    """

    results = []
    ProductID, UserID, Rating, Timestamp = RawData.split(',')
    results.append(
        (UserID, [ProductID, float(Rating)])
    )

    return results


def seqFunc(zeroValue, pair):

    """
    seqFunc for aggregateByKey for reduce Round1
    input:
        zeroValue: define the structure of output with default values (0, ['', 0, '']))
        pair: ['ProductID', Rating]
    output:
        results: (number_of_occurrence,['ProductId',Rating,'Rating'])
    """

    result = (
        zeroValue[0] + 1,
        [
            zeroValue[1][0] + ' ' + pair[0],
            zeroValue[1][1] + pair[1],
            zeroValue[1][2] + ' ' + str(pair[1])
        ]
    )
    return result


def combFunc(d1, d2):

    """
    combFunc for aggregateByKey for reduce Round1
    input:
        d1: (number_of_occurrence,['ProductId',Rating,'Rating'])
        d2: (number_of_occurrence,['ProductId',Rating,'Rating'])
    output:
        results: combination of d1, d2 that is (number_of_occurrence,['ProductIds_related_UserID',summation_of_ratings,'ratings_for_UserID'])
     """

    result = (
        d2[0] + d1[0],
        [
            d2[1][0] + ' ' + d1[1][0],
            d2[1][1] + d1[1][1],
            d2[1][2] + ' ' + d1[1][2]
        ]
    )

    return result


def averaging(doc):

    """
    Computing the AvgRating is the average rating of all reviews by the user "UserID"
    input:
        doc: (number_of_occurrence,['ProductIds_related_UserID',summation_of_ratings,'ratings_for_UserID'])
    output:
        results: ['ProductIds_related_UserID',averaging_of_ratings,'ratings_for_UserID']
     """

    return [
        doc[1][0],
        doc[1][1] / doc[0],
        doc[1][2]
    ]


def map2(pair):

    """
    Mapping for Round2
    input:
        pair: ['ProductIds_related_UserID',averaging_of_ratings,'ratings_for_UserID']
    output:
        results: ('ProductID', NormRating)
    """

    results = []
    pids = pair[1][0].split(' ')

    while '' in pids:
        pids.remove('')

    ratings = pair[1][2].split(' ')

    while '' in ratings:
        ratings.remove('')

    for ProductID, rate in zip(pids, ratings):
        NormRating = float(rate) - pair[1][1]  # Compute NormRating=Rating-AvgRating
        results.append((ProductID, NormRating))

    return results


def map3(doc, K):

    """
    Mapping pairs from key ProductID to random key by considering K
    input:
        pair: ('ProductID', NormRating)
    output:
        results: (random_key,('ProductID', NormRating))
    """

    return [(rand.randint(0, K - 1), (doc[0], doc[1]))]


def gather_pairs(pairs):

    """
    Gathering pairs by same random key and compute maximum rating between them
    input:
        pair: ('ProductID', NormRating)
    output:
        results: (random_key,('ProductID', MNR))
    """

    pairs_dict = {}
    for p in pairs[1]:
        ProductID, NormRating = p[0], p[1]

        if ProductID not in pairs_dict.keys():
            pairs_dict[ProductID] = NormRating

        else:
            if pairs_dict[ProductID] < NormRating:
                pairs_dict[ProductID] = NormRating

    return [(key, pairs_dict[key]) for key in pairs_dict.keys()]


def normalize_rating(RawData):

    """
    NormalizedRatings contains the pair (ProductID,NormRating), where NormRating=Rating-AvgRating
    input :
        The RDD RawData representing a review (ProductID,UserID,Rating,Timestamp)
    output:
        (ProductID,NormRating)
    """

    normalizedRatings = (
        RawData.flatMap(map1)  # <-- MAP PHASE (R1)
        .aggregateByKey(seqFunc=seqFunc, combFunc=combFunc, zeroValue=(0, ['', 0, '']))  # <-- REDUCE PHASE (R1)
        .mapValues(averaging)  # <-- MAP PHASE (R2)
        .flatMap(map2)  # <-- MAP PHASE (R3), reduce R2 just propagate
    )

    return normalizedRatings


def maximum_rating(normalizedRatings, K):

    """
    Computing the maximum normalized rating(MNR) of product "ProductID"
    input:
        NormalizedRatings contains the pair (ProductID,NormRating), where NormRating=Rating-AvgRating
    output:
        (ProductID, MNR)
    """

    maxNormRatings = (
        normalizedRatings.flatMap(lambda x: map3(x, K))  # <-- MAP PHASE (R1)
        .groupByKey()  # <-- REDUCE PHASE (R1)
        .flatMap(gather_pairs)  # <-- MAP PHASE (R2)
        .reduceByKey(max)  # <-- REDUCE PHASE (R2)
    )

    return maxNormRatings


def main():
    # CHECKING NUMBER OF CMD LINE PARAMTERS
    assert len(sys.argv) == 4, "Usage: python G03HW1.py <K> <file_name>"

    # SPARK SETUP
    conf = SparkConf().setAppName('G03HW1').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # INPUT READING

    # 1. Read number of partitions
    K = sys.argv[1]
    assert K.isdigit(), "K must be an integer"
    K = int(K)

    # 2. Read number of T products with largest maximum normalized rating
    T = sys.argv[2]
    assert T.isdigit(), "T must be an integer"
    T = int(T)

    # 3. Read input file and subdivide it into K random partitions
    data_path = sys.argv[3]
    assert os.path.isfile(data_path), "File or folder not found"
    RawData = sc.textFile(data_path, minPartitions=K).cache()
    RawData.repartition(numPartitions=K)

    # Normalizing rates foreach ProductID by considering the average rating of all reviews by the user "UserID"
    normalizedRatings = normalize_rating(RawData)
    normalizedRatings.repartition(numPartitions=K)

    # Finding the maximum normalized rate for each ProductID
    maxNormRatings = maximum_rating(normalizedRatings, K)

    # the T products with largest maximum normalized rating, one product per line.
    results = maxNormRatings.sortByKey(ascending=True) \
        .takeOrdered(T, key=lambda x: -x[1])  # Sort by key and then sort by values

    print("INPUT PARAMETERS: K={} T={} file={}".format(K, T, data_path))
    print('OUTPUT:')

    for result in results:
        print(
            "Product {} maxNormRating {} ".format(result[0], result[1])
        )


if __name__ == "__main__":
    main()
