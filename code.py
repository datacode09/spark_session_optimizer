from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# Optimized Spark configuration
conf = SparkConf() \
    .setAppName("OptimizedSparkJob") \
    .setMaster("yarn") \  # Use YARN as the resource manager
    .set("spark.executor.instances", "10") \  # Number of executors
    .set("spark.executor.cores", "4") \  # Number of cores per executor
    .set("spark.executor.memory", "8g") \  # Memory per executor
    .set("spark.driver.memory", "4g") \  # Memory for the driver
    .set("spark.driver.cores", "2") \  # Cores for the driver
    .set("spark.yarn.queue", "default") \  # YARN queue
    .set("spark.network.timeout", "300s") \  # Network timeout for long tasks
    .set("spark.sql.autoBroadcastJoinThreshold", "10MB") \  # Optimize broadcast join size
    .set("spark.dynamicAllocation.enabled", "true") \  # Enable dynamic allocation
    .set("spark.dynamicAllocation.initialExecutors", "5") \  # Initial executors
    .set("spark.dynamicAllocation.maxExecutors", "20") \  # Maximum executors
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \  # Use Kryo serializer
    .set("spark.sql.execution.arrow.pyspark.enabled", "true")  # Enable PyArrow for performance

# Initialize SparkContext
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Log environment details for debugging
print("Spark Application Name: ", sc.appName)
print("Master URL: ", sc.master)
print("Executor Memory: ", conf.get('spark.executor.memory'))
