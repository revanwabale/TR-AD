
To Execute On cluster or Standalone Or local Mode, pls use below command:
-------------------------------------------
# Run application locally on 8 cores :

spark-submit \
  --executor-memory 1G \
  --total-executor-cores 1 \
  ProbleSolver.py \
  --master local[8]  \
  --x 3 \
  --inputFile exampleLargeFile.txt

# Run on a Spark standalone cluster in client deploy mode :

spark-submit \
  --executor-memory 1G \
  --total-executor-cores 1 \
  ProbleSolver.py \
  --master spark://10.186.195.130:4040  \  # replace url and port
  --x 3 \
  --inputFile exampleLargeFile.txt


# Run on a Spark standalone cluster in cluster deploy mode with supervise :

spark-submit \
  --executor-memory 1G \
  --total-executor-cores 1 \
  --deploy-mode cluster \
  --supervise \
  ProbleSolver.py \
  --master spark://10.186.195.130:4040  \  # replace url and port
  --x 3 \
  --inputFile exampleLargeFile.txt

# Run on a YARN cluster  :
export HADOOP_CONF_DIR=XXX
spark-submit \
  --executor-memory 2G \
  --num-executors 5 \
  --deploy-mode cluster \   # can be client for client mode
  ProbleSolver.py \
  --master yarn
  --x 3 \
  --inputFile exampleLargeFile.txt

###########################################


For Local Test, using unittest:

python -m unittest ProblemSolverTest.py

    2 test cases are covered , pls find the module name from ProblemSolverTest.py class:
    >test_provided_data_case1()
    >test_provided_data_case2()

-------------------------------------------