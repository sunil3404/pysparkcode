import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

emp = [(1,"Smith",-1,"2018","10","M",3000), \
       (2,"Rose",1,"2010","20","M",4000), \
       (3,"Williams",1,"2010","10","M",1000), \
       (4,"Jones",2,"2005","10","F",2000), \
       (5,"Brown",2,"2010","40","",-1), \
       (6,"Brown",2,"2010","50","",-1) \
      ]

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]

emp_columns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary"]
dept_columns = ["dept_name", "dept_id"]

spark  = SparkSession.builder.appName('Joins').getOrCreate()

empdf = spark.createDataFrame(data=emp, schema=emp_columns)
deptdf = spark.createDataFrame(data=dept, schema=dept_columns)

print("*" * 100)
print("Inner Join gets common values from both tables and drops the unmatched")
print("*" * 100)
innerjoin = empdf.join(deptdf, empdf.emp_dept_id == deptdf.dept_id, "inner")
print(innerjoin.show())

print("*" * 100)
print("Full Outer Join returns all rows from both datasets \nwhere join expression doesnt match it returns null on respective record columns")
print("*" * 100)
print(empdf.join(deptdf, empdf.emp_dept_id == deptdf.dept_id, "outer").show())

print("*" * 100)
print('''Left Outer Join returns all rows from the left dataset
regardless of match found on the right dataset
when join expression doesnt match it assigns null for that record
and drops records from right where match not found''')
print("*" * 100)
print(empdf.join(deptdf, empdf.emp_dept_id == deptdf.dept_id, "leftouter").show())


print("*" * 100)
print('''Right Outer Join returns all rows from the right dataset
regardless of match found on the left dataset
when join expression doesnt match it assigns null for that record
and drops records from left where match not found''')
print("*" * 100)
print(empdf.join(deptdf, empdf.emp_dept_id == deptdf.dept_id, "rightouter").show())

print("*" * 100)
print('''Left Semi join returns columns from the left dataset and ignores all the columns from the right dataset. This join
returns columns from left dataset for the records match in the right dataset on the join expression, 
records not matched are ignored from both left and right datasets''')
print("*" * 100)
print(empdf.join(deptdf, empdf.emp_dept_id == deptdf.dept_id, "leftsemi").show())

print("*" * 100)
print('''Left Anti Join returns only columns from the the left dataset for non-matched records''')
print("*" * 100)
print(empdf.join(deptdf, empdf.emp_dept_id == deptdf.dept_id, "leftanti").show())


print("*" * 100)
print("Pyspark self join")
print("*" * 100)
print(empdf.alias("emp1").join(empdf.alias("emp2"), col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner").\
        select(col("emp1.emp_id"), col("emp1.name"), \
        col("emp2.emp_id").alias("superior_emp_id"), \
        col("emp2.name").alias("superior_emp_name")).show())



print("*" * 100)
print("Join on Multiple dataframes")
print("*" * 100)
print('''df1.join(df2, df1.id1 == df2.id2, "inner") \ 
.join(df3, df1.id1 == df3.id3, "inner"''')
