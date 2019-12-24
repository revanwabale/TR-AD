import argparse


from pyspark.sql import  SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

import pyspark.sql.functions as f


def getXvalue(x, inputdir,master):

    spark = SparkSession.builder.appName("ProblemSolver").master(master).getOrCreate()
    #spark.conf.set("spark.sql.shuffle.partitions", 11)
    # conf = (SparkConf()
    #         .setMaster("local")
    #         .setAppName("ProblemSolver")
    #         .set("spark.executor.memory", "1g"))

    def getResult():
        # Explicitly set schema
        schema = StructType([StructField("id", StringType(), True),
                            StructField("value", StringType(), True)])


        lines=spark.sparkContext.textFile(inputdir,5)   # time complexity = n/No.Of partition , space complexity = size of text file + size of pid columns
        sepLines=lines.map(lambda l: l.split(' '))
        df=sepLines.toDF(schema)

        df=df.withColumn("id", df["id"].cast(IntegerType()))
        df=df.withColumn("value", df["value"].cast(IntegerType()))
        #df.rdd.glom().count()
        #df=df.repartition(5)
        #print("Number of Partitions used to distribute this file : "+ str(df.rdd.getNumPartitions()))
        #print("2: " + str(len(df.rdd.repartition(5).glom().collect())))
        #df=df.withColumn("pid", f.spark_partition_id()).orderBy("value")
        #df.printSchema()
        #df.show()

        lookupDf = (df.select("value")
            .distinct()
            .sort(f.col("value").desc()).rdd.zipWithIndex()
            .map(lambda z: z[0] + (z[1], ))
            .toDF(["value", "dense_rank"]))                             # time complexity = uptp 100 rows[for evaluation to create df],

        #print("lookuop: ")
        #lookupDf.show()

        df = df.join(lookupDf, ["value"]).withColumn("dense_rank", f.col("dense_rank") + 1)  # time complexity = (n * n)/No.Of partition, + shuffle[network]
        #print("after join: ")
        #df.show()
        df = df.filter(df['dense_rank'] <= x)

        #df.show()
        df = df.select(df['id'])
        pandas_df = df.toPandas()
        return pandas_df

    return getResult()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--x", required=False, help="x value to search",  default=3, type=int)
    parser.add_argument("-inputFile", "--inputFile", required=False,
                        help="Large whitespace delim text inputFile Absolute Path",
                        #default="bqinput.txt",
                        default="exampleLargeFile.txt",
                        type=str)
    parser.add_argument("-master", "--master", required=False, help="master",  default='local[1]', type=str)


    args = vars(parser.parse_args())
    print('passed command line argument are  --> ', args)

    resultlist = getXvalue(int(args["x"]),args["inputFile"], args['master'])

    print(resultlist)

#bqinput.txt
# +---+-----+---+
# | id|value|idx|
# +---+-----+---+
# |  1|  200|  1|
# |  2|  200|  1|
# |  2|  115|  2|
# |  5|  111|  3|
# +---+-----+---+

#exampleLargeFile.txt
# +----------+-----+---+
# |        id|value|idx|
# +----------+-----+---+
# 1	1426828028	350
# 2	1426828056	231
# 3	1426828066	111


