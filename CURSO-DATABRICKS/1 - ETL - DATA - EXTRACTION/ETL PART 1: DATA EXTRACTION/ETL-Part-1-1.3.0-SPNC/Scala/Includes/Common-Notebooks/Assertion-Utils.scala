// Databricks notebook source

//*******************************************
// Deprecated test functions
//*******************************************

def dbTest[T](id: String, expected: T, result: => T): Unit = {
  val eventId = "Test-" + (if (id != null) id else java.util.UUID.randomUUID)
  val evaluation = (result == expected)
  val status = if (evaluation) "passed" else "failed"
  daLogger.logEvent(id, s"$eventId\nDBTest Assertion\n$status\n1")

  assert(evaluation, s"$result does not equal $expected expected")
}

//*******************************************
// TEST SUITE CLASSES
//*******************************************

// Test case
case class TestCase(description:String,
                    testFunction:()=>Any,
                    id:String=null,
                    dependsOn:Seq[String]=Nil,
                    escapeHTML:Boolean=false,
                    points:Int=1)

// Test result
case class TestResult(test: TestCase, skipped:Boolean = false, debug:Boolean = false) {
  val exception: Option[Throwable] = {
    if (skipped)
      None
    else if (debug) {
      if (test.testFunction() != false)
        None
      else 
        Some(new AssertionError("Test returned false"))
    }
    else {
      try {
        assert(test.testFunction() != false, "Test returned false")
        None
      } catch {
        case e: Exception => Some(e)
        case e: AssertionError => Some(e)
      }
    }
  }

  val passed: Boolean = !skipped && exception.isEmpty

  val message: String = {
    exception.map(ex => {
      val msg = ex.getMessage()
      if (msg == null || msg.isEmpty()) ex.toString() else msg
    }).getOrElse("")
  }
  
  val status: String = if (skipped) "skipped" else if (passed) "passed" else "failed"
  
  val points: Int = if (passed) test.points else 0
}

// Test Suite
case class TestSuite(testCases : TestCase*) {
  import scala.collection.mutable.ListBuffer
  import scala.xml.Utility.escape
  
  def runTests(debug:Boolean = false) = {
    val failedTests = scala.collection.mutable.Set[String]()
    testCases.map(test => {
      val skip = test.dependsOn.exists(failedTests contains _)
      val result = TestResult(test, skip, debug)
      //YUCK: A map operation with side effects
      if (!result.passed && test.id != null) failedTests += test.id

      val eventId = "Test-" + (if (result.test.id != null) result.test.id 
                          else if (result.test.description != null) result.test.description.replaceAll("[^a-zA-Z0-9_]", "").toUpperCase() 
                          else java.util.UUID.randomUUID)
      val message = s"${eventId}\n${result.test.description}\n${result.status}\n${result.points}"
      daLogger.logEvent(eventId, message)
      
      result
    })
  }

  lazy val testResults = runTests()

//   lazy val allPassed
  
  private def display(cssClass:String="results", debug:Boolean=false) : Unit = {
    val testResults = if (!debug) this.testResults else runTests(debug=true)
    val lines = ListBuffer[String]()
    lines += """
<style>
  table { text-align: left; border-collapse: collapse; margin: 1em; caption-side: bottom; font-family: Sans-Serif; font-size: 16px}
  caption { text-align: left; padding: 5px }
  th, td { border: 1px solid #ddd; padding: 5px }
  th { background-color: #ddd }
  .passed { background-color: #97d897 }
  .failed { background-color: #e2716c }
  .skipped { background-color: #f9d275 }
  .results .points { display: none }
  .results .message { display: none }
  .results .passed::before  { content: "Passed" }
  .results .failed::before  { content: "Failed" }
  .results .skipped::before { content: "Skipped" }
  .grade .passed  .message:empty::before { content:"Passed" }
  .grade .failed  .message:empty::before { content:"Failed" }
  .grade .skipped .message:empty::before { content:"Skipped" }
</style>
    """.trim
    lines += s"<table class='$cssClass'>"
    lines += "  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>"
    for (result <- testResults) {
      val resultHTML = s"<td class='result ${result.status}'><span class='message'>${result.message}</span></td>"
      val descriptionHTML = if (result.test.escapeHTML) escape(result.test.description) else result.test.description
      lines += s"  <tr><td class='points'>${result.points}</td><td class='test'>$descriptionHTML</td>$resultHTML</tr>"
    }
    lines += s"  <caption class='points'>Score: $score</caption>"
    lines += "</table>"
    val html = lines.mkString("\n")
    displayHTML(html)
  }
  
  def displayResults() : Unit = {
    display("results")
  }
  
  def grade() : Int = {
    display("grade")
    score
  }
  
  def debug() : Unit = {
    display("grade", debug=true)
  }
  
  lazy val score = testResults.map(_.points).sum
  
  val maxScore : Integer = testCases.map(_.points).sum

  lazy val percentage = 100.0 * score / maxScore
}

try {
  // Test to see if it is already set - if not, an exception will be thrown
  spark.conf.get("dbacademy.certifications.return.result")
} catch {
  // The value is not set (overwridden elsewhere) so set it now.
  case e:Exception => spark.conf.set("dbacademy.certifications.return.result", "true")
}

// These are part of the mechanisim for testing and auto-grading.
// These are initialized to zero so that we always have some value.
spark.conf.set("dbacademy.certifications.scala.score", "0")
spark.conf.set("dbacademy.certifications.scala.points", "0")
spark.conf.set("dbacademy.certifications.python.score", "0")
spark.conf.set("dbacademy.certifications.python.points", "0")

// Function to return results
def returnResults():Unit = {
  val result = "Scala: %s/%s\tPython: %s/%s".format(
    spark.conf.get("dbacademy.certifications.scala.score"), 
    spark.conf.get("dbacademy.certifications.scala.points"),
    spark.conf.get("dbacademy.certifications.python.score"), 
    spark.conf.get("dbacademy.certifications.python.points"))

  dbutils.notebook.exit(result)
}


//*******************************************
// SCORE FUNCTIONS AND PLACEHOLDER GENERATION
//*******************************************


// OK Imports
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType

val FILL_IN = null
val EMPTY_DF = spark.emptyDataFrame
val EMPTY_ARRAY = Array(Row())
val EMPTY_SCHEMA = org.apache.spark.sql.types.StructType(List())
val EMPTY_ROW = Row()
val EMPTY_LONG = 0L
val EMPTY_INT: Integer = null

val EMPTY_DATASET = spark.createDataset(Seq(""))

/*
  Supresses NullPointerException for display(FILL_IN).
  We do this by overriding display(DataFrame) and importing this new version.
*/

def computeFinalScore(scores: scala.collection.mutable.Map[String, (Int, String)], runtimeStart: Long): Unit = {
  val runtimeDuration = (System.currentTimeMillis() - runtimeStart)/1000.0/60.0
  println("-"*80)
  
  var totalScore = 0
  for (key <- scores.keys.toSeq.sorted) {
    val value = scores(key)
    totalScore += value._1 
    println(f"[${value._1}%2d%%] ${key}%-20s ${value._2}")
  }
  val points = 10
  val score = totalScore*points/100.0

  println(f"\nResults: ${totalScore}%2d%%")
  println(f"Final Score: ${score}%.1f/$points%.1f")
  println(f"Run Time: $runtimeDuration%.2f min")
  println("-"*80)
  
  spark.conf.set("dbacademy.certifications.scala.score", score.toString)
  spark.conf.set("dbacademy.certifications.scala.points", points.toString)
  
  if (spark.conf.get("dbacademy.certifications.return.result") == "true") {
    dbutils.notebook.exit("%s/%s".format(score, points))
  }
}


def testsPassed(tests: TestSuite): Boolean = {
  tests.testResults.map(_.passed).filter(_ == false).size == 0
}

//*******************************************
// GET QUERY STRING
//*******************************************

def getQueryString(df: Dataset[Row]): String = {
  df.explain.toString
}


//*******************************************
// COMPARISON FUNCTIONS
//*******************************************

def compareFloats(valueA: Any, valueB: Any, tolerance: Double=0.01): Boolean = {
  // Usage: compareFloats(valueA, valueB) (uses default tolerance of 0.01)
  //        compareFloats(valueA, valueB, tolerance=0.001)
  
  import scala.math
  try{
       if (valueA == null && valueB == null) {
         true
       } else if (valueA == null || valueB == null) {
         false
       } else {
         math.abs(valueA.asInstanceOf[Number].doubleValue - valueB.asInstanceOf[Number].doubleValue) <= tolerance 
       }
  } catch {
    case e: ClassCastException => {
      false
   }   
  }
}


def compareRows(rowA: org.apache.spark.sql.Row, rowB: org.apache.spark.sql.Row): Boolean = {
  // Usage: compareRows(rowA, rowB)
  // compares two rows as unordered Maps

  if (rowA == null && rowB == null) {
    true
    
  } else if (rowA == null || rowB == null) {
    false
    
  // for some reason, the schema didn't make it
  } else if (rowA.schema == null || rowB.schema == null) {
    rowA.toSeq.toSet == rowB.toSeq.toSet
    
  } else {
    rowA.getValuesMap[String](rowA.schema.fieldNames.toSeq) == rowB.getValuesMap[String](rowB.schema.fieldNames.toSeq)
  }
}


def compareDataFrames(dfA: org.apache.spark.sql.DataFrame, dfB: org.apache.spark.sql.DataFrame): Boolean = {
  // Usage: compareDataFrames(dfA, dfB)
  //        rows have to be ordered
  
  if (dfA == null && dfB == null) {
    true
    
  } else if (dfA == null || dfB == null) {
    false
    
  } else { 
  
    // if the sizes are different, the DFs aren't equal  
    if (dfA.count != dfB.count) {
      false
    }

    // These appropaches didn't work (well)
  
    // dfA.exceptAll(dfB).isEmpty && dfB.exceptAll(dfA).isEmpty
    // sloooooow and doesn't look at row order
  
    // (dfA.rdd zip dfB.rdd)
    // doesn't always work because rows can be in different partitions
  
    val kv1 = dfA.rdd.zipWithIndex().map { case (v, i) => i -> v }
    val kv2 = dfB.rdd.zipWithIndex().map { case (v, i) => i -> v }
    val kv12= kv1.leftOuterJoin(kv2).map {
      case (i, (v1, v2)) => i -> (v1, v2.get)
    }

    kv12.sortByKey().values.map(r => compareRows(r._1, r._2)).reduce(_ && _) 
  }
}


def checkSchema(schemaA: org.apache.spark.sql.types.StructType, schemaB: org.apache.spark.sql.types.StructType, keepOrder: Boolean=true, keepNullable: Boolean=false): Boolean = {
  // Usage: checkSchema(schemaA, schemaC, keepOrder=false, keepNullable=false)
  
  if (schemaA == null && schemaB == null) {
    return true
    
  } else if (schemaA == null || schemaB == null) {
    return false
  
  } else {
    var schA = schemaA.toSeq
    var schB = schemaB.toSeq

    if (keepNullable == false) {   
      schA = schemaA.map(_.copy(nullable=true)) 
      schB = schemaB.map(_.copy(nullable=true)) 
    }
  
    if (keepOrder == true) {
      schA == schB
    } else {
      schA.toSet == schB.toSet
    }
  }
}

displayHTML("Initializing Databricks Academy's testing framework...")

