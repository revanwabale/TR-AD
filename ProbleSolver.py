from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import TimestampType, StringType, StructType, StructField, IntegerType
import click
from pyspark.streaming import StreamingContext
from pyspark.sql.window import Window
import pyspark.sql.functions as f


@click.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', prompt='Your name',
              help='The person to greet.')
def hello(count, name):
    """Simple program that greets NAME for a total of COUNT times."""
    for y in range(count):
        click.echo('Hello %s!' % name)


@click.command()
@click.option('--x', prompt='x', required=True,
              help='Provide x value to find in the given file ..')
@click.option('--inputdir', prompt='inputdir',
              type=str,
              default="/Users/revan.wabale/PycharmProjects/TR-AD/inputData/bqInput.txt",
              #default="/Users/revan.wabale/PycharmProjects/TR-AD/exampleLargeFile.txt",
              help='Provide input directory under which file will be copied..')

def getXvalueStramer(x, inputdir):
    # Explicitly set schema
    schema = StructType([StructField("id", IntegerType(), True),
                      StructField("value", IntegerType(), True)])
    #LargeData = spark.read.text(inputPath).cache()

    conf = (SparkConf()
        .setMaster("local")
        .setAppName("ProblemSolver")
        .set("spark.executor.memory", "1g"))

    #spark = SparkSession.builder.appName("ProblemSolver").master('local[1]').getOrCreate()

    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)

    lines = ssc.textFileStream(inputdir)
    #df = spark.readStream.text(path=inputdir)
    #print(df.schema)
    #df.collect()
    #counts = lines.flatMap(lambda line: line.split(" ")) \
    #    .map(lambda m: (m, 1)) \
    #    .reduceByKey(lambda a, b: a+b)

    lines = lines.map(lambda l: l.split(' '))
    df = SQLContext.createDataFrame(lines, schema)
    df.pprint()
    #ssc.start()
    ssc.awaitTermination()

@click.command()
@click.option('--x', prompt='x', required=True,
              help='Provide x value to find in the given file ..')
@click.option('--inputdir', prompt='inputdir',
              type=str,
              default="bqinput.txt",
              #default="exampleLargeFile.txt",
              help='Provide input directory under which file will be copied..')
@click.option('--isunittest', prompt='isunittest',
              type=bool,
              default=False,
              #default="exampleLargeFile.txt",
              help='isunittest running <True/False>')

def getXvalue(x, inputdir, isunittest):

    spark = SparkSession.builder.appName("ProblemSolver").master('local[1]').getOrCreate()
    #spark.conf.set("spark.sql.shuffle.partitions", 11)
    # conf = (SparkConf()
    #         .setMaster("local")
    #         .setAppName("ProblemSolver")
    #         .set("spark.executor.memory", "1g"))

    def getResult():
        # Explicitly set schema
        schema = StructType([StructField("id", StringType(), True),
                            StructField("value", StringType(), True)])


        lines=spark.sparkContext.textFile(inputdir)
        sepLines=lines.map(lambda l: l.split(' '))
        df=sepLines.toDF(schema)
        df=df.withColumn("id", df["id"].cast(IntegerType()))
        df=df.withColumn("value", df["value"].cast(IntegerType()))
        #df.rdd.glom().count()
        df=df.repartition(5)
        print("Number of Partitions used to distribute this file : "+ str(df.rdd.getNumPartitions()))
        #print("2: " + str(len(df.rdd.repartition(5).glom().collect())))

        df=df.withColumn("pid", f.spark_partition_id()).orderBy("value")
        df.printSchema()
        df.show()

        lookupDf = (df.select("value")
            .distinct()
            .sort(f.col("value").desc()).rdd.zipWithIndex()
            .map(lambda z: z[0] + (z[1], ))
            .toDF(["value", "rank"]))

        df = df.join(lookupDf, ["value"]).withColumn("rank", f.col("rank") + 1)

        df.show()

        resultlist = df.filter(df['rank'] <= x).collect()
        return resultlist
    if isunittest:
        return getResult()
    elif isunittest is False:
        return click.echo(getResult())



#w = Window.partitionBy("pid").orderBy("value")
    #rankDf = df.withColumn("local_dense_rank", f.dense_rank().over(w))

    # df.createOrReplaceTempView("targetData")
    # result = spark.sql("SELECT id , value, "
    #                    "DENSE_RANK() OVER(ORDER BY value desc) as idx FROM targetData")
    # result.show()
    # r = result.filter(result['idx'] <= x)
    # r.show()

if __name__ == '__main__':
    #hello()

    getXvalue()

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
# |1426828011|    9|  1|
# |1426828028|  350|  2|
# |1426828037|   25|  3|
# +----------+-----+---+

# x	a	b
# 1	1426828028	350
# 2	1426828056	231
# 3	1426828066	111


