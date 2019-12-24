from operator import add

from PySparkBaseForTest import PySparkBaseForTest
from ProbleSolver import getXvalue
import pandas as pd


class ProblemSolverTest(PySparkBaseForTest):
    def test_basic(self):
        test_rdd = self.spark.sparkContext.parallelize(['cat dog mouse','cat cat dog'], 2)
        results = test_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(add).collect()
        expected_results = [('cat', 3), ('dog', 2), ('mouse', 1)]
        self.assertEqual(set(results), set(expected_results))

    def test_provided_data_case1(self):
        absPathLargeInputFile = "exampleLargeFile.txt"
        x = 3
        test_case1 = "testCases/test_provided_data_case1.csv"
        expectedDfId = pd.read_csv(test_case1)

        result_pandas_df = getXvalue(x, absPathLargeInputFile, "local[1]")

        print(result_pandas_df)
        print(expectedDfId)
        self.assert_frame_equal_with_sort(result_pandas_df, expectedDfId, ['id'])

    def test_provided_data_case2(self):
        absPathLargeInputFile = "bqinput.txt"
        x = 3
        test_case2 = "testCases/test_provided_data_case2.csv"
        expectedDfId = pd.read_csv(test_case2)

        result_pandas_df = getXvalue(x, absPathLargeInputFile, "local[2]")

        print(result_pandas_df)
        print(expectedDfId)
        self.assert_frame_equal_with_sort(result_pandas_df, expectedDfId, ['id'])

