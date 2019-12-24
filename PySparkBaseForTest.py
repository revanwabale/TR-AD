
import unittest
import logging
from pyspark.sql import SparkSession
from pandas.util.testing import assert_frame_equal


class PySparkBaseForTest(unittest.TestCase):

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

    @classmethod
    def assert_frame_equal_with_sort(cls, results, expected, keycolumns):
        results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
        expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
        assert_frame_equal(left=results_sorted, right=expected_sorted, check_dtype=False)

