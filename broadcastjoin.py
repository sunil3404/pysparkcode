import main
from pyspark.sql.functions import broadcast

spark = main._main()

emp = [("10", "sunil", "India"),
               ("20", "dinesh", "India"),
               ("30", "tony stark", "USA")
              ]
empschema = ["empid", "name", "country"]

dept = [("10", "Karnataka"), ("20", "KGF")]
deptschema = ["dept_id", "state"]

def returnDataFrames(data, schema):
    return spark.createDataFrame(data=data, schema = schema)

empdf  = returnDataFrames(emp, empschema)
deptdf = returnDataFrames(dept, deptschema)


print("*" * 50)
print("Base DataFrames")
print("*" * 50)
print(empdf.show())
print(deptdf.show())

print("*" * 50)
print("BroadcastJoin")
print("*" * 50)

print(empdf.join(broadcast(deptdf), empdf.empid == deptdf.dept_id).show())


