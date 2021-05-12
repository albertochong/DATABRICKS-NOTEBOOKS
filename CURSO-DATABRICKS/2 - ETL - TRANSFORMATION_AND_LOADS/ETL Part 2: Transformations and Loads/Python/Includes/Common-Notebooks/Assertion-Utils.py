# Databricks notebook source

# These imports are OK to provide for students
import pyspark
from typing import Callable, Any, Iterable, List, Set, Tuple

#############################################
# Deprecated test functions
#############################################

def dbTest(id, expected, result):
  import uuid
  
  if id: eventId = "Test-"+id 
  else: eventId = "Test-"+str(uuid.uuid1())

  evaluation = str(expected) == str(result)
  status = "passed" if evaluation else "failed"
  daLogger.logEvent(id, f"{eventId}\nDBTest Assertion\n{status}\n1")

  assert evaluation, f"{result} does not equal expected {expected}"
  
#############################################
# Test Suite classes
#############################################

# Test case
class TestCase(object):
  __slots__=('description', 'testFunction', 'id', 'dependsOn', 'escapeHTML', 'points')
  def __init__(self,
               description:str,
               testFunction:Callable[[], Any],
               id:str=None,
               dependsOn:Iterable[str]=[],
               escapeHTML:bool=False,
               points:int=1):
    
    self.description=description
    self.testFunction=testFunction
    self.id=id
    self.dependsOn=dependsOn
    self.escapeHTML=escapeHTML
    self.points=points

# Test result
class TestResult(object):
  __slots__ = ('test', 'skipped', 'debug', 'passed', 'status', 'points', 'exception', 'message')
  def __init__(self, test, skipped = False, debug = False):
    try:
      self.test = test
      self.skipped = skipped
      self.debug = debug
      if skipped:
        self.status = 'skipped'
        self.passed = False
        self.points = 0
      else:
        assert test.testFunction() != False, "Test returned false"
        self.status = "passed"
        self.passed = True
        self.points = self.test.points
      self.exception = None
      self.message = ""
    except Exception as e:
      self.status = "failed"
      self.passed = False
      self.points = 0
      self.exception = e
      self.message = repr(self.exception)
      if (debug and not isinstance(e, AssertionError)):
        raise e

# Decorator to lazy evaluate - used by TestSuite
def lazy_property(fn):
    '''Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

# Test suite class
class TestSuite(object):
  def __init__(self, *testCases: Iterable[TestCase]) -> None:
    self.testCases = testCases
    
  @lazy_property
  def testResults(self) -> List[TestResult]:
    return self.runTests()
  
  def runTests(self, debug=False) -> List[TestResult]:
    import re
    import uuid
    failedTests = set()
    def runTest(test: TestCase) -> TestResult:
      skip = any(testId in failedTests for testId in test.dependsOn)
      result = TestResult(test, skip, debug)
      # YUCK: A map operation with side effects
      if (not result.passed and test.id != None):
        failedTests.add(test.id)

      if result.test.id: eventId = "Test-"+result.test.id 
      elif result.test.description: eventId = "Test-"+re.sub("[^a-zA-Z0-9_]", "", result.test.description).upper()
      else: eventId = "Test-"+str(uuid.uuid1())

      message = f"{eventId}\n{result.test.description}\n{result.status}\n{result.points}"
      daLogger.logEvent(eventId, message)

      return result
    
    return list(map(runTest, self.testCases))

  def _display(self, cssClass:str="results", debug=False) -> None:
    from html import escape
    testResults = self.testResults if not debug else self.runTests(debug=True)
    lines = []
    lines.append("""
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
    """.strip())
    lines.append("<table class='"+cssClass+"'>")
    lines.append("  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>")
    for result in testResults:
      resultHTML = "<td class='result "+result.status+"'><span class='message'>"+result.message+"</span></td>"
      descriptionHTML = escape(result.test.description) if (result.test.escapeHTML) else result.test.description
      lines.append("  <tr><td class='points'>"+str(result.points)+"</td><td class='test'>"+descriptionHTML+"</td>"+resultHTML+"</tr>")
    lines.append("  <caption class='points'>Score: "+str(self.score)+"</caption>")
    lines.append("</table>")
    html = "\n".join(lines)
    displayHTML(html)
  
  def displayResults(self) -> None:
    self._display("results")
  
  def grade(self) -> int:
    self._display("grade")
    return self.score
  
  def debug(self) -> None:
    self._display("grade", debug=True)
  
  @lazy_property
  def score(self) -> int:
    return __builtins__.sum(map(lambda result: result.points, self.testResults))
  
  @lazy_property
  def maxScore(self) -> int:
    return __builtins__.sum(map(lambda result: result.testCase.points, self.testResults))

  @lazy_property
  def percentage(self) -> float:
    return 100.0 * score / maxScore

  
#############################################
# Test Suite utilities
#############################################

from pyspark import SparkContext
from pyspark.sql import (SparkSession, DataFrame)

def createPlaceHolders():
  from pyspark.sql.types import Row, StructType
  
  return (None, sqlContext.createDataFrame(sc.emptyRDD(), StructType([])), [], Row(), StructType([]), None)

(FILL_IN, EMPTY_DF, EMPTY_LIST, EMPTY_ROW, EMPTY_SCHEMA, EMPTY_INT) = createPlaceHolders()
  
### Supresses NullPointerException for display(FILL_IN).
### We do this by overriding display(DataFrame) and importing this new version.

def computeFinalScore(scores: dict, runtimeStart: float) -> None:
  '''
  Computes final scores and prints the results
  '''
  import time
  runtimeDuration = (__builtins__.int(__builtins__.round(time.time() * 1000)) - runtimeStart)/1000.0/60.0

  print("-"*80)

  totalScore = 0
  for key in sorted(scores.keys()):
    (score, notes) = scores[key]
    print("[{:>2}%] {:<20} {}".format(score, key, notes))
    totalScore += score

  points = 10
  score = totalScore*points/100.0

  print("\nResults: {}%".format(totalScore))
  print("Final Score: {:.1f}/{:.1f}".format(score, points))
  print("Run Time: {:.2f} min".format(runtimeDuration))
  print("-"*80)

  spark.conf.set("dbacademy.certifications.python.score", str(score))
  spark.conf.set("dbacademy.certifications.python.points", str(points))
  
  if spark.conf.get("dbacademy.certifications.return.result") == "true":
    dbutils.notebook.exit("{}/{}".format(score, points))

  
def testsPassed(tests: TestSuite) -> bool:
  return False not in [i.passed for i in tests.testResults]

# *******************************************
#  GET QUERY STRING
# *******************************************

# from pyspark.sql import DataFrame

def getQueryString(df: DataFrame) -> str:
  # redirect sys.stdout to a buffer
  import sys, io
  stdout = sys.stdout
  sys.stdout = io.StringIO()

  # call module
  df.explain(extended=True)

  # get output and restore sys.stdout
  output = sys.stdout.getvalue()
  sys.stdout = stdout

  return output


#############################################
# Test Suite comparison functions
#############################################

from pyspark.sql import Row, DataFrame

def compareFloats(valueA, valueB, tolerance=0.01):
  # Usage: compareFloats(valueA, valueB) (uses default tolerance of 0.01)
  #        compareFloats(valueA, valueB, tolerance=0.001)
 
  from builtins import abs 
  try:
    if (valueA == None and valueB == None):
         return True
      
    else:
         return abs(float(valueA) - float(valueB)) <= tolerance 
      
  except:
    return False
  
  
def compareRows(rowA: Row, rowB: Row):
  # Usage: compareRows(rowA, rowB)
  # compares two Dictionaries
  
  if (rowA == None and rowB == None):
    return True
  
  elif (rowA == None or rowB == None):
    return False
  
  else: 
    return rowA.asDict() == rowB.asDict()


def compareDataFrames(dfA: DataFrame, dfB: DataFrame):
  from functools import reduce
  # Usage: compareDataFrames(dfA, dfB)
    
  if (dfA == None and dfB == None):
    return True
  else:  
    # right off the bat, if the sizes are different, the DFs aren't equal
    n = dfA.count()
  
    if (n != dfB.count()):
      return False
  
    # These appropaches didn't work (well)
  
    # dfA.exceptAll(dfB).isEmpty && dfB.exceptAll(dfA).isEmpty
    # sloooooow and doesn't look at row order
  
    # (dfA.rdd.zip(dfB.rdd))
    # doesn't always work because rows can be in different partitions
  
    kv1 = dfA.rdd.zipWithIndex().map(lambda t : (t[1], t[0])).collectAsMap()
    kv2 = dfB.rdd.zipWithIndex().map(lambda t : (t[1], t[0])).collectAsMap()
  
    kv12 = [kv1, kv2]
    d = {}

    for k in kv1.keys():
      d[k] = tuple(d[k] for d in kv12)
  
    return reduce(lambda a, b: a and b, [compareRows(rowTuple[0], rowTuple[1]) for rowTuple in d.values()])

  
def checkSchema(schemaA, schemaB, keepOrder=True, keepNullable=False): 
  # Usage: checkSchema(schemaA, schemaB, keepOrder=false, keepNullable=false)
  
  from pyspark.sql.types import StructField
  
  if (schemaA == None and schemaB == None):
    return True
  
  elif (schemaA == None or schemaB == None):
    return False
  
  else:
    schA = schemaA
    schB = schemaB

    if (keepNullable == False):  
        schA = [StructField(s.name, s.dataType) for s in schemaA]
        schB = [StructField(s.name, s.dataType) for s in schemaB]
  
    if (keepOrder == True):
      return [schA] == [schB]
    else:
      return set(schA) == set(schB)
  
displayHTML("Initializing Databricks Academy's testing framework...")

