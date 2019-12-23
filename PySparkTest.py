
import unittest
import logging
from pyspark.sql import SparkSession
import pandas as pd
from pandas.testing import assert_frame_equal, assert_index_equal
from pandas.util.testing import assert_frame_equal


class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder.master('local[2]').
                appName('local-testing-pyspark-context').enableHiveSupport().getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

####
    '''
    @classmethod
    def test_data_frame(self):
        # Create the test data, with larger examples this can come from a CSV file
        # and we can use pd.read_csv(…)
        data_pandas = pd.DataFrame({'id':['1426828028', '1426828066', '1426828056'],
                                'value':['350','111','231']})
        # Turn the data into a Spark DataFrame, self.spark comes from our PySparkTest base class
        data_spark = self.spark.createDataFrame(data_pandas)
        # Invoke the unit we’d like to test
        results_spark = my_spark_function(data_spark)
        # Turn the results back to Pandas
        results_pandas = results_spark.toPandas()
        # Our expected results crafted by hand, again, this could come from a CSV
        # in case of a bigger example
        expected_results = pd.DataFrame({'id':['1426828028', '1426828066', '1426828056'],
                                         'value':['350','111','231']})
        # Assert that the 2 results are the same. We’ll cover this function in a bit
        assert_frame_equal(results_pandas, expected_results, ['id'])

        '''