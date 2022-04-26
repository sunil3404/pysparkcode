import main

spark = main._main()

sc = spark.sparkContext



inp = sc.textFile("excludefirstNlines.txt")

print(inp.collect())
