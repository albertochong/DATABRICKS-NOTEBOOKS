# Databricks notebook source
# MAGIC %md
# MAGIC # Assertion-Utils-Test
# MAGIC The purpose of this notebook is to faciliate testing of assertions.

# COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

# COMMAND ----------

# MAGIC %run ./Class-Utility-Methods

# COMMAND ----------

# MAGIC %run ./Assertion-Utils

# COMMAND ----------

suite = TestSuite(
  TestCase(id="PythonTest-1", description=f"My first python test",  testFunction = lambda: True),
  TestCase(id=None,           description=f"My second python test", testFunction = lambda: False),
  TestCase(id=None,           description=None,                     testFunction = lambda: True),
  TestCase(id="PythonTest-4", description=f"My fourth python test", testFunction = lambda: False)
)

for testResult in list(suite.testResults):
  print(f"{testResult.test.id}: {testResult.status}")

# COMMAND ----------

dbTest("PythonTest-1", "cat", "cat")

try:
  dbTest("PythonTest-2", "cat", "dog")
except AssertionError:
  pass

# dbTest("PythonTest-3", 999, 666)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing compareFloats
# MAGIC 
# MAGIC ```compareFloats(floatA, floatB, tolerance)```

# COMMAND ----------

assert compareFloats(1, 1) == True

# No long or double equivilent to Scala
assert compareFloats(100.001, int(100)) == True
assert compareFloats(100.001, float(100)) == True

assert compareFloats(int(100), 100.001) == True
assert compareFloats(float(100), 100.001) == True

assert compareFloats(100.001, "blah") == False
assert compareFloats("blah", 100.001) == False

assert compareFloats(1.0, 1.0) == True

assert compareFloats(1.0, 1.2, .0001) == False
assert compareFloats(1.0, 1.02, .0001) == False
assert compareFloats(1.0, 1.002, .0001) == False
assert compareFloats(1.0, 1.0002, .0001) == False
assert compareFloats(1.0, 1.00002, .0001) == True
assert compareFloats(1.0, 1.000002, .0001) == True

assert compareFloats(1.2, 1.0, .0001) == False
assert compareFloats(1.02, 1.0, .0001) == False
assert compareFloats(1.002, 1.0, .0001) == False
assert compareFloats(1.0002, 1.0, .0001) == False
assert compareFloats(1.00002, 1.0, .0001) == True
assert compareFloats(1.000002, 1.0, .0001) == True

assert compareFloats(1, None) == False
assert compareFloats(None, 1) == False
assert compareFloats(None, None) == True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing compareSchemas
# MAGIC 
# MAGIC ```compareSchemas(schemaA, schemaB, keepOrder=True, keepNullable=False)```

# COMMAND ----------

from pyspark.sql.types import *
_expectedSchema = StructType([
  StructField("LastName", StringType(), True),
  StructField("MaxSalary", DoubleType(), True)
])

_studentSchema1 = StructType([
  StructField("MaxSalary", DoubleType(), True),
  StructField("LastName", StringType(), True)
])

_studentSchema2 = StructType([
  StructField("LastName", StringType(), True),
  StructField("MaxSalary", BooleanType(), True)
])

_studentSchema3 = StructType([
  StructField("LastName", StringType(), False),
  StructField("MaxSalary", DoubleType(), True)
])

_studentSchema4 = StructType([
  StructField("LastName", StringType(), True),
  StructField("MaxSalary", DoubleType(), True),
  StructField("Country", StringType(), True)
])

assert checkSchema(_expectedSchema, _studentSchema1, keepOrder=False) == True       # out of order, ignore order
assert checkSchema(_expectedSchema, _studentSchema1) == False                       # out of order, preserve order
assert checkSchema(_expectedSchema, _studentSchema2) == False                       # different types
assert checkSchema(_expectedSchema, _studentSchema3, keepNullable=False) == True    # drop nullable
assert checkSchema(_expectedSchema, _studentSchema4) == False                       # left side < right size
assert checkSchema(_studentSchema4, _expectedSchema) == False                       # left side > right side
assert checkSchema(None, _studentSchema3, keepNullable=False) == False              # Null schemaA
assert checkSchema(None, None, keepNullable=False) == True                          # Null schemaA and schemaB
assert checkSchema(_studentSchema2, None, keepNullable=False) == False              # Null schemaB

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing compareRows
# MAGIC 
# MAGIC ```compareRows(rowA, rowB)```

# COMMAND ----------

from pyspark.sql import Row
_rowA = Row(LastName="Duck", MaxSalary=10000)
_rowB = Row(LastName="Mouse", MaxSalary=60000)
_rowC = Row(MaxSalary=10000, LastName="Duck")
_rowD = Row(LastName="Duck", MaxSalary=10000, City="Anaheim")

assert compareRows(_rowA, _rowB) == False       # different schemas
assert compareRows(_rowA, _rowA) == True        # compare to self
assert compareRows(_rowA, _rowC) == True        # order reversed
assert compareRows(None, _rowA) == False        # Null rowA
assert compareRows(None, None) == True          # Null rowA and rowB
assert compareRows(_rowA, None) == False        # Null rowB
assert compareRows(_rowA, _rowD) == False       # _rowA smaller than _rowD
assert compareRows(_rowD, _rowA) == False       # _rowD bigger than _rowA

# note Python doesn't allow you to define rows without a schema
# _rowE = Row("Duck", 10000)

