windowData = [ 
            ("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 4100),
            ("Maria", "Finance", 3000),
            ("James", "Sales", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Kumar", "Marketing", 2000),
            ("Saif", "Sales", 4100)
          ]
schema = ["name", "department", "salary"]

import main
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank, ntile, cume_dist


df = main._main().createDataFrame(data = windowData, schema = schema) 
df.show()

windowspec = Window.partitionBy("department").orderBy("salary")


print("*" * 50)
print('''Row Number fucntion is used to give sequential row number starting from 1
to the result of each window partition''')
print("*" * 50)
df.withColumn("row_number", row_number().over(windowspec)).show()

print("*" * 50)
print('''Rank fucntions used to give rank to the result within a window 
partition. This function leaves gap in rank when there are ties''')
print("*" * 50)
df.withColumn("rank", rank().over(windowspec)).show()

print("*" * 50)
print('''Dense Rank function use to get the result with rank of rows
within a window partition without any gaps''')
print("*" * 50)
df.withColumn("Dense_rank", dense_rank().over(windowspec)).show()

print("*" * 50)
print('''Percent rank functions gives sequential number in percentage''')
print("*" * 50)
df.withColumn("percent_rank", percent_rank().over(windowspec)).show()

print("*" * 50)
print(''' ntile function returns the relative rank of result rows within partition. In below we have used 2 as an argument to ntile hence it returns ranking
between 2 values 1 and 2''')
print("*" * 50)
df.withColumn("ntile", ntile(5).over(windowspec)).show()


print("*" * 50)
print(''' cume_dist() function is used to get cumulative distribution of values
withing a window partition''')
print("*" * 50)
df.withColumn("cume_dist", cume_dist().over(windowspec)).show()

print("*" * 50)
print('''lag function''')
print("*" * 50)
from pyspark.sql.functions import lag, lead
df.withColumn("lag", lag("salary", 2).over(windowspec)).show()

print("*" * 50)
print('''lead function''')
print("*" * 50)
df.withColumn("lag", lead("salary", 2).over(windowspec)).show()

print("*" * 50)
print('''window aggregate function''')
print("*" * 50)

windowSpecAggr = Window.partitionBy("department")
from pyspark.sql.functions import col, avg, sum, min, max, row_number
df.withColumn("row", row_number().over(windowspec)).withColumn("avg", avg(col("salary")).over(windowSpecAggr)).withColumn("sum", sum(col("salary")).over(windowSpecAggr)).withColumn("min", min(col("salary")).over(windowSpecAggr)).withColumn("max", max(col("salary")).over(windowSpecAggr)).where(col("row") == 1).select("department", "avg", "sum", "min", "max").show()
