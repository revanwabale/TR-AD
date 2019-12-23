from operator import add

from PySparkTest import PySparkTest
from ProbleSolver import getXvalue
import pandas as pd

class SimpleTest(PySparkTest):
    def test_basic(self):
        test_rdd = self.spark.sparkContext.parallelize(['cat dog mouse','cat cat dog'], 2)
        results = test_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(add).collect()
        expected_results = [('cat', 3), ('dog', 2), ('mouse', 1)]
        self.assertEqual(set(results), set(expected_results))

    def test_provided_data_case1(self):
        from click.testing import CliRunner
        absPathLargeInputFile = "exampleLargeFile.txt"
        x = 3
        test_case1 = "testCases/test_provided_data_case1.csv"
        validDfId = pd.read_csv(test_case1)

        runner = CliRunner()
        list_result = runner.invoke(getXvalue([ x, absPathLargeInputFile, True]))
        #assert list_result.exit_code == 0

        #results = test_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(add).collect()
        #expected_results = [('cat', 3), ('dog', 2), ('mouse', 1)]

        print(list_result)
        #self.assertEqual(set(results), set(expected_results))