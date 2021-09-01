// Databricks notebook source
// MAGIC %md
// MAGIC # Assertion-Utils-Test
// MAGIC The purpose of this notebook is to faciliate testing of assertions.

// COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

// COMMAND ----------

// MAGIC %run ./Class-Utility-Methods

// COMMAND ----------

// MAGIC %run ./Assertion-Utils

// COMMAND ----------

val suite = TestSuite(
  TestCase(id="ScalaTest-1",  description=s"My first scala test",  testFunction = () => true),
  TestCase(id=null,           description=s"My second scala test", testFunction = () => false),
  TestCase(id=null,           description=null,                    testFunction = () => true),
  TestCase(id="ScalaTest-4",  description=s"My fourth scala test", testFunction = () => false)
)

for (testResult <- suite.testResults) {
  println(s"${testResult.test.id}: ${testResult.status}")
}
println("-"*80)

// COMMAND ----------

dbTest("ScalaTest-1", "cat", "cat")

try {
  dbTest("ScalaTest-2", "cat", "dog")  
} catch {
  case _: AssertionError => ()
}
// dbTest("ScalaTest-3", 999, 666)  

// COMMAND ----------

// MAGIC %md
// MAGIC ## Testing compareFloats
// MAGIC 
// MAGIC ```compareFloats(floatA, floatB, tolerance)```

// COMMAND ----------

assert(compareFloats(1, 1.toInt) == true)

assert(compareFloats(100.001, 100.toInt) == true)
assert(compareFloats(100.001, 100.toLong) == true)
assert(compareFloats(100.001, 100.toFloat) == true)
assert(compareFloats(100.001, 100.toDouble) == true)

assert(compareFloats(100.toInt, 100.001) == true)
assert(compareFloats(100.toLong, 100.001) == true)
assert(compareFloats(100.toFloat, 100.001) == true)
assert(compareFloats(100.toDouble, 100.001) == true)

assert(compareFloats(100.001, "blah") == false)
assert(compareFloats("blah", 100.001) == false)

assert(compareFloats(1.0, 1.0) == true)

assert(compareFloats(1.0, 1.2, .0001) == false)
assert(compareFloats(1.0, 1.02, .0001) == false)
assert(compareFloats(1.0, 1.002, .0001) == false)
assert(compareFloats(1.0, 1.0002, .0001) == false)
assert(compareFloats(1.0, 1.00002, .0001) == true)
assert(compareFloats(1.0, 1.000002, .0001) == true)

assert(compareFloats(1.2, 1.0, .0001) == false)
assert(compareFloats(1.02, 1.0, .0001) == false)
assert(compareFloats(1.002, 1.0, .0001) == false)
assert(compareFloats(1.0002, 1.0, .0001) == false)
assert(compareFloats(1.00002, 1.0, .0001) == true)
assert(compareFloats(1.000002, 1.0, .0001) == true)

assert(compareFloats(1, null) == false)
assert(compareFloats(null, 1) == false)
assert(compareFloats(null, null) == true)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Testing compareSchemas
// MAGIC 
// MAGIC ```compareSchemas(schemaA, schemaB, keepOrder=True, keepNullable=False)```

// COMMAND ----------

import org.apache.spark.sql.types._ 
val _expectedSchema = StructType(List(
  StructField("LastName", StringType, true),
  StructField("MaxSalary", DoubleType, true)
  ))

val _studentSchema1 = StructType(List(
  StructField("MaxSalary", DoubleType, true),
  StructField("LastName", StringType, true)
  ))

val _studentSchema2 = StructType(List(
  StructField("LastName", StringType, true),
  StructField("MaxSalary", BooleanType, true)
  ))

val _studentSchema3 = StructType(List(
  StructField("LastName", StringType, false),
  StructField("MaxSalary", DoubleType, true)
))

val _studentSchema4 = StructType(List(
  StructField("LastName", StringType, true),
  StructField("MaxSalary", DoubleType, true),
  StructField("Country", StringType, true)
  ))

assert(checkSchema(_expectedSchema, _studentSchema1, keepOrder=false) == true)             // out of order, ignore order
assert(checkSchema(_expectedSchema, _studentSchema1) == false)                             // out of order, preserve order
assert(checkSchema(_expectedSchema, _studentSchema2) == false)                             // different types

assert(checkSchema(_expectedSchema, _studentSchema3, keepNullable=false) == true)          // drop nullable
assert(checkSchema(_expectedSchema, _studentSchema4) == false)                             // left side < right size
assert(checkSchema(_studentSchema4, _expectedSchema) == false)                             // left side > right side

assert(checkSchema(null, _studentSchema3, keepNullable=false) == false, "Null schemaA")    // Null schemaA
assert(checkSchema(null, null, keepNullable=false) == true, "Null schemaA and schemaB")    // Null schemaA and schemaB
assert(checkSchema(_studentSchema2, null, keepNullable=false) == false, "Null schemaB")    // Null schemaB

// COMMAND ----------

// MAGIC %md
// MAGIC ## Testing compareRows
// MAGIC 
// MAGIC ```compareRows(rowA, rowB)```

// COMMAND ----------

val bla = Row("Duck", 10000.0)
bla.schema

// COMMAND ----------

val df1 = Seq(("Duck", 10000.0), ("Mouse", 60000.0)).toDF("LastName", "MaxSalary")
val _rowA = df1.collect()(0)
val _rowB = df1.collect()(1)

val df2 = Seq((10000.0, "Duck")).toDF("MaxSalary", "LastName")
val _rowC = df2.collect()(0)

val df3 = Seq(("Duck", 10000.0, "Anaheim")).toDF("LastName", "MaxSalary", "City")
val _rowD = df3.collect()(0)

// no schema!
val _rowE =  Row("Duck", 10000.0)
val _rowF =  Row("Mouse", 60000.0)

assert(compareRows(_rowA, _rowB) == false)                   // different schemas
assert(compareRows(_rowA, _rowA) == true)                    // compare to self
assert(compareRows(_rowA, _rowC) == true)                    // order reversed
assert(compareRows(null, _rowA) == false, "null _rowA")      // Null rowA
assert(compareRows(null, null) == true, "null _rowA, rowB")  // Null rowA and rowB
assert(compareRows(_rowA, null) == false, "null _rowB")      // Null rowB
assert(compareRows(_rowA, _rowD) == false)                   // _rowA smaller than _rowD
assert(compareRows(_rowD, _rowA) == false)                   // _rowD smaller than _rowA 
assert(compareRows(_rowE, _rowF) == false)                   // no schemas, different
assert(compareRows(_rowE, _rowE) == true)                    // no schemas, same
assert(compareRows(_rowE, _rowA) == true)                    // no schemas / schema, same content
assert(compareRows(_rowE, _rowB) == false)                    // no schemas / schema, different content

