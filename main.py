import findspark
findspark.init()
import json

from pyspark.sql import SparkSession

with open ("config.json" , 'r') as config_file:
    config = json.load(config_file)

def _main():
    spark = SparkSession.builder.appName(config.get("appName")).getOrCreate()
    return spark


if __name__ == "__main__":
    pass

