import main


spark = main._main()

text_file = "This is a word count problem and this is the first pyspark problem"

rdd = spark.sparkContext.parallelize(text_file.split(" "))

map_rdd = rdd.map(lambda x : (x, 1))

print(map_rdd.collect())


rdd_reduce_by_key  = map_rdd.reduceByKey(lambda x, y : x + y)

print(rdd_reduce_by_key.collect())
