import findspark
findspark.init()
import json
from pyspark.sql import SparkSession

with open("config.json", "r") as config_file:
    config = json.load(config_file)

spark = SparkSession.builder.appName("hivepush").enableHiveSupport().getOrCreate()

#Read data from titanic table from kaggle DB
df = spark.sql("select * from {}.{}".format(config.get("database_name"), config.get("titanic_table")))

#select the columns from the table
df = df.select(config.get('columns'))

#write the data into hive table
#df.write.partitionBy(config.get("sex")).mode(config["write_mode"]).saveAsTable("{}.{}".format(config['database_name'], config['partiton_table_name']))

#read the data back from the hive table
df2 = spark.sql("select * from {}.{}".format(config['database_name'], config['partition_table_name']))

#write data in format csv
df.write.partitionBy("class").format("csv").mode("overwrite").saveAsTable("kaggle.csv_file_format")

#writing data in orc format
df.write.partitionBy("class").format("orc").mode("overwrite").saveAsTable("kaggle.orc_file_format")

#writing data in avro format
df.write.partitionBy("class").format("avro").mode("overwrite").saveAsTable("kaggle.avro_file_format")
df.show(4)






