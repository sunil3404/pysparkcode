import main

from pyspark.sql.functions import *

spark = main._main()


spark_date = [("1", "2020-02-01"), ("2", "2019-03-01"), ("3", "2021-03-01")]

df = spark.createDataFrame(data=spark_date, schema=("id", "input"))

print("*" * 50)
print("\n current_date() function is used to get the current systenm date. By default the date will be returned in YYYY-dd-mm")
print("*" * 50)

df.withColumn("current_date", current_date()).show()
df.select(current_date().alias("current_date")).show()

print("*" * 50)
print("date format function MM-dd-YYYY")
print("*" * 50)
df.withColumn("mm-dd-yyyy", date_format(col("input"), "dd-MM-YYYY")).show()

print("*" * 50)
print("to_date() converts string in date format 'yyyy-mm-dd'  to date type\nyyyy-mm-dd")
print("*" * 50)
df.select(col("input"), to_date(col("input"), "YYYY-mm-dd").alias("to_date")).show()


print("*" * 50)
print("datediff returns difference betwee to dates using datediff()")
print("*" * 50)
df.select(datediff(current_date(), col("input")).alias("datediff")).show()

print("*" * 50)
print("Months betwee - months_between()")
print("*" * 50)
df.select(months_between(current_date(), col("input")).alias("months_between")).show()

print("*" * 50)
print("trunc() function")
print("*" * 50)
df.select(col("input"), trunc(col("input"), "Month").alias("Month_trunc"), trunc(col("input"), "Year").alias("Year_trunc"), trunc(col("input"), 'Month').alias("Month_trunc")).show()

print("*" * 50)
print("add_months(), date_add(), date_sub()")
print("*" * 50)
df.select(col("input"), add_months(col("input"), 3).alias("3monthadd"), add_months(col("input"), -3).alias("-3monthadd"), date_add(col("input"), 4).alias("date_add4"), date_sub(col("input"), 4).alias("date_sub4")).show()

print("*" * 50)
print("year(), month(), next_day(), weekofyear()")
print("*" * 50)
df.select(col("input"), year(col("input")).alias("year"), month(col("input")).alias("month"), next_day(col("input"), "sunday").alias("next_day"), weekofyear(col("input")).alias("weekofyear")).show()

print("*" * 50)
print("dayofweek(), dayofmonth(), dayofyear()")
print("*" * 50)
df.select(col("input"), dayofweek(col("input")).alias("dayofweek"), dayofmonth(col("input")).alias("dayofmonth"), dayofyear(col("input")).alias("dayofyear")).show()

print("*" * 50)
print("current timestamp")
print("*" * 50)
df.select(col("input"), current_timestamp().alias("current_timestamp")).show(truncate=False)

data = [("1", "2020-02-01 11:01:19.06")]
df3 =  spark.createDataFrame(data=data, schema=["id", "input"])

print("*" * 50)
print("To timestamp")
print("*" * 50)
df3.select(col("input"), to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp")).show(truncate=False)


print("*" * 50)
print("hour(), minute(), second()")
print("*" * 50)
df3.select(col("input"), hour(col("input")).alias("hour"), minute(col(input)).alias("minute"), second(col("input")).alias("second")).show(truncate=False)

