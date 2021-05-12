# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Class-Utility-Methods-Test
# MAGIC The purpose of this notebook is to faciliate testing of courseware-specific utility methos.

# COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

# COMMAND ----------

# MAGIC %md
# MAGIC a lot of these tests evolve around the current DBR version.
# MAGIC 
# MAGIC It shall be assumed that the cluster is configured properly and that these tests are updated with each publishing of courseware against a new DBR

# COMMAND ----------

courseDbrMajor = 6
courseDbrMinor = 3

# COMMAND ----------

# MAGIC %run ./Class-Utility-Methods

# COMMAND ----------

def functionPassed(result):
  if not result:
    raise AssertionError("Test failed")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getTags`

# COMMAND ----------

def testGetTags():
  
    testTags = getTags()
    
    # Setup tests
    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    # Test that getTags returns correct type
    testsPassed.append(None)
    try:
        from py4j.java_collections import JavaMap
        assert isinstance(getTags(), JavaMap)
        passedTest(True)
    except:
        passedTest(False, "The correct type is not returned by getTags")
        
    # Test that getTags does not return an empty dict
    testsPassed.append(None)
    try:
        assert len(testTags) > 0
        passedTest(True)
    except:
        passedTest(False, "A non-empty dict is returned by getTags")
    
    # Print final info and return
    if all(testsPassed):
        print('All {} tests for getTags passed'.format(len(testsPassed)))
        return True
    else:
        raise Exception('{} of {} tests for getTags passed'.format(testsPassed.count(True), len(testsPassed)))

functionPassed(testGetTags()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getTag()`

# COMMAND ----------

def testGetTag():
    
    # Setup tests
    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    # Test that getTag returns null when defaultValue is not set and tag is not present
    testsPassed.append(None)
    try:
        assert getTag("thiswillneverbeincluded") == None
        passedTest(True)
    except:
        passedTest(False, "NoneType is not returned when defaultValue is not set and tag is not present for getTag")
        
    # Test that getTag returns defaultValue when it is set and tag is not present
    testsPassed.append(None)
    try:
        assert getTag("thiswillneverbeincluded", "default-value") == "default-value"
        passedTest(True)
    except:
        passedTest(False, "defaultValue is not returned when defaultValue is set and tag is not present for getTag")
        
    # Test that getTag returns correct value when default value is not set and tag is present
    testsPassed.append(None)
    try:
        orgId = getTags()["orgId"]
        assert isinstance(orgId, str)
        assert len(orgId) > 0
        assert orgId == getTag("orgId")
        passedTest(True)
    except:
        passedTest(False, "A non-empty dict is returned by getTags")
    
    # Print final info and return
    if all(testsPassed):
        print('All {} tests for getTag passed'.format(len(testsPassed)))
        return True
    else:
        raise Exception('{} of {} tests for getTag passed'.format(testsPassed.count(True), len(testsPassed)))

functionPassed(testGetTag()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `getDbrMajorAndMinorVersions()`

# COMMAND ----------

def testGetDbrMajorAndMinorVersions():
    
    (major,minor) = getDbrMajorAndMinorVersions()
    assert major == courseDbrMajor, f"Found {major}"
    assert minor == courseDbrMinor, f"Found {minor}"
        
    return True
      
functionPassed(testGetDbrMajorAndMinorVersions()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `getPythonVersion()`

# COMMAND ----------

def testGetPythonVersion():
    
    # Setup tests
    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    # Test output for structure
    testsPassed.append(None)
    try:
        pythonVersion = getPythonVersion()
        assert isinstance(pythonVersion, str)
        assert len(pythonVersion.split(".")) >= 2
        passedTest(True)
    except:
        passedTest(False, "pythonVersion does not match expected structure")
        
    # Test output for correctness
    testsPassed.append(None)
    try:
        pythonVersion = getPythonVersion()
        assert pythonVersion[0] == "2" or pythonVersion[0] == "3"
        passedTest(True)
    except:
        passedTest(False, "pythonVersion does not match expected value")
        

    # Print final info and return
    if all(testsPassed):
        print('All {} tests for getPythonVersion passed'.format(len(testsPassed)))
        return True
    else:
        raise Exception('{} of {} tests for getPythonVersion passed'.format(testsPassed.count(True), len(testsPassed)))

functionPassed(testGetPythonVersion()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getUsername()`

# COMMAND ----------

def testGetUsername():
  username = getUsername()
  assert isinstance(username, str)
  assert username != ""
  
  return True
    
functionPassed(testGetUsername()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getUserhome`

# COMMAND ----------

def testGetUserhome():
  userhome = getUserhome()
  assert isinstance(userhome, str)
  assert userhome != ""
  assert userhome == "dbfs:/user/" + getUsername()
    
  return True

functionPassed(testGetUserhome()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `assertDbrVersion`

# COMMAND ----------

def testAssertDbrVersion():
  
    (majorVersion, minorVersion) = getDbrMajorAndMinorVersions()
    major = int(majorVersion)
    minor = int(minorVersion)

    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    ## Test input missing
    
    # expMajor missing, expMinor not missing
    testsPassed.append(None)
    try:
        assertDbrVersion(expMinor = 3)
        passedTest(False, 'Missing expMajor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'Missing expMajor does not raise TypeError')
      
    # expMajor not missing, expMinor missing
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = 3)
        passedTest(False, 'Missing expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'Missing expMinor does not raise TypeError')
    
    # expMajor missing, expMinor missing
    testsPassed.append(None)
    try:
        assertDbrVersion()
        passedTest(False, 'Missing expMajor and missing expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'Missing expMajor and missing expMinor does not raise TypeError')
    
    # expMajor = None, expMinor not missing
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = None, expMinor = 3)
        passedTest(False, 'NoneType expMajor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'NoneType expManor does not raise TypeError')
    
    # expMajor not missing, expMinor = None
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major, expMinor = None)
        passedTest(False, 'NoneType expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'NoneType expMinor does not raise TypeError')
    
    # expMajor = None, expMinor = None
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = None, expMinor = None)
        passedTest(False, 'NoneType expMajor and NoneType expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'NoneType expMajor and NoneType expMinor does not raise TypeError')
    
    
    ## Test input types
    
    # expMajor = int, expMinor = int
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = 0, expMinor = 0)
        passedTest(True)
    except:
        passedTest(False, 'int expMajor and int expMinor raise exception')
    
    # expMajor = int, expMinor = float
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = 0, expMinor = 0.1)
        passedTest(False, 'int expMajor and float expMinor does not raise exception')
    except:
        passedTest(True)
    
    # expMajor = int, expMinor = string
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = 0, expMinor = str(minor))
        passedTest(False, 'int (0) expMajor and numeral string does not raise exception')
    except:
        passedTest(True)
        
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = 0, expMinor = "text")
        passedTest(False, 'int expMajor and text string raise exception')
    except:
        passedTest(True)
        
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major, expMinor = str(minor))
        passedTest(False, 'int (major) expMajor and numeral string do not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'int (major) expMajor and numeral string do not raise TypeError')
        
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major, expMinor = "text")
        passedTest(False, 'int (major) expMajor and text string do not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'int (major) expMajor and text string do not raise TypeError')
    
    # expMajor = int, expMinor = Boolean
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major, expMinor = True)
        passedTest(False, 'int expMajor and Boolean expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'int expMajor and Boolean expMinor does not raise TypeError')
    
    # expMajor = float, expMinor = int
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = 0.0, expMinor = minor)
        passedTest(False, 'float expMajor and int expMinor does not raise exception')
    except:
        passedTest(True)
    
    # expMajor = float, expMinor = float
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = 0.0, expMinor = 0.0)
        passedTest(False, 'float expMajor and int expMinor does not raise exception')
    except:
        passedTest(True)
    
    # expMajor = float, expMinor = string
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = 0.0, expMinor = str(minor))
        passedTest(False, 'float (0.0) expMajor and numeral string does not raise exception')
    except:
        passedTest(True)
        
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = 0.0, expMinor = "text")
        passedTest(False, 'float expMajor (0.0) and text string does not raise exception')
    except:
        passedTest(True)
        
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = float(major), expMinor = str(minor))
        passedTest(False, 'float (major) expMajor and numeral string do not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'float (major) expMajor and numeral string do not raise TypeError')
        
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = float(major), expMinor = "text")
        passedTest(False, 'float (major) expMajor and text string do not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'float (major) expMajor and text string do not raise TypeError')
    
    # expMajor = float, expMinor = Boolean
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = float(major), expMinor = True)
        passedTest(False, 'float expMajor and Boolean expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'float expMajor and Boolean expMinor does not raise TypeError')
    
    # expMajor = string, expMinor = int
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = "0", expMinor = 0)
        passedTest(False, 'string ("0") expMajor and int expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'string ("0") expMajor and int expMinor does not raise TypeError')
    
    # expMajor = string, expMinor = float
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = "0", expMinor = 0.0)
        passedTest(False, 'string ("0") expMajor and float expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'string ("0") expMajor and float expMinor does not raise TypeError')
    
    # expMajor = string, expMinor = string
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = "0", expMinor = "0")
        passedTest(False, 'string ("0") expMajor and string ("0") expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'string ("0") expMajor and string("0") expMinor does not raise TypeError')
        
    # expMajor = string, expMinor = Boolean
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = "0", expMinor = True)
        passedTest(False, 'string ("0") expMajor and Boolean expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'string ("0") expMajor and Boolean expMinor does not raise TypeError')
        
    # expMajor = Boolean, expMinor = int
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = True, expMinor = 0)
        passedTest(False, 'Boolean expMajor and int expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'Boolean expMajor and int expMinor does not raise TypeError')
    
    # expMajor = Boolean, expMinor = float
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = True, expMinor = 0.0)
        passedTest(False, 'Boolean expMajor and float expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'Boolean expMajor and float expMinor does not raise TypeError')
    
    # expMajor = Boolean, expMinor = string
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = True, expMinor = "0")
        passedTest(False, 'Boolean expMajor and string expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'Boolean expMajor and string expMinor does not raise TypeError')
    
    # expMajor = Boolean, expMinor = Boolean
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = True, expMinor = True)
        passedTest(False, 'Boolean expMajor and Boolean expMinor does not raise exception')
    except TypeError:
        passedTest(True)
    except:
        passedTest(False, 'Boolean expMajor and Boolean expMinor does not raise TypeError')
    
    
    ## Test input comparisons
    
    # expMajor < trueMajor, expMinor < trueMinor
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major - 1, expMinor = minor - 1)
        passedTest(True)
    except:
        passedTest(False, 'expMajor < trueMajor and expMinor < trueMinor raises exception')
    
    # expMajor < trueMajor, expMinor == trueMinor
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major - 1, expMinor = minor)
        passedTest(True)
    except:
        passedTest(False, 'expMajor < trueMajor and expMinor == trueMinor raises exception')
    
    # expMajor < trueMajor, expMinor > trueMinor
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major - 1, expMinor = minor + 1)
        passedTest(True)
    except:
        passedTest(False, 'expMajor < trueMajor and expMinor > trueMinor raises exception')
    
    # expMajor == trueMajor, expMinor < trueMinor
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major, expMinor = minor - 1)
        passedTest(True)
    except:
        passedTest(False, 'expMajor == trueMajor and expMinor < trueMinor raises exception')
    
    # expMajor == trueMajor, expMinor == trueMinor
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major, expMinor = minor)
        passedTest(True)
    except:
        passedTest(False, 'expMajor == trueMajor and expMinor == trueMinor raises exception')
    
    # expMajor == trueMajor, expMinor > trueMinor
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major, expMinor = minor + 1)
        passedTest(False, 'expMajor == trueMajor and expMinor > trueMinor does not raise exception')
    except:
        passedTest(True)
    
    # expMajor > trueMajor, expMinor < trueMinor
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major + 1, expMinor = minor - 1)
        passedTest(False, 'expMajor > trueMajor and expMinor < trueMinor does not raise exception')
    except:
        passedTest(True)
        
    # expMajor > trueMajor, expMinor == expMinor
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major + 1, expMinor = minor)
        passedTest(False, 'expMajor > trueMajor and expMinor == trueMinor does not raise exception')
    except:
        passedTest(True)
        
    # expMajor > trueMajor, expMinor > trueMajor
    testsPassed.append(None)
    try:
        assertDbrVersion(expMajor = major + 1, expMinor = minor + 1)
        passedTest(False, 'expMajor > trueMajor and expMinor > trueMinor does not raise exception')
    except:
        passedTest(True)
        
    ## Test output
    
    # Test output type
    testsPassed.append(None)
    try:
        retval = assertDbrVersion(expMajor = major, expMinor = minor)
        assert isinstance(retval, str)
        passedTest(True)
    except:
        passedTest(False, 'return value is not a string')
        
    # Test output value
    testsPassed.append(None)
    try:
        retval = assertDbrVersion(expMajor = major, expMinor = minor)
        assert retval == '.'.join([str(major), str(minor)])
        passedTest(True)
    except:
        passedTest(False, 'return value is not the dot-join of the major and minor verion numbers')
        
    ## Print final info and return
    if all(testsPassed):
        print('All {} tests for assertDbrVersion passed'.format(len(testsPassed)))
        return True
    else:
        raise Exception('{} of {} tests for assertDbrVersion passed'.format(testsPassed.count(True), len(testsPassed)))
        
functionPassed(testAssertDbrVersion())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `assertIsMlRuntime`

# COMMAND ----------

# def testAssertIsMlRuntime():

#   assertIsMlRuntime("6.3.x-ml-scala2.11")
#   assertIsMlRuntime("6.3.x-cpu-ml-scala2.11")

#   try:
#     assertIsMlRuntime("5.5.x-scala2.11")
#     assert False, "Expected to throw an ValueError"
#   except AssertionError:
#     pass

#   try:
#     assertIsMlRuntime("5.5.xml-scala2.11")
#     assert False, "Expected to throw an ValueError"
#   except AssertionError:
#     pass

#   return True

# functionPassed(testAssertIsMlRuntime())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Legacy Functions
# MAGIC 
# MAGIC Note: Legacy functions will not be tested. Use at your own risk.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `createUserDatabase`

# COMMAND ----------

def testCreateUserDatabase(): 

  courseType = "wa"
  username = "mickey.mouse@disney.com"
  moduleName = "Testing-Stuff 101"
  lessonName = "TS 03 - Underwater Basket Weaving"
  
  # Test that correct database name is returned
  expectedDatabaseName = "mickey_mouse_disney_com" + "_" + "testing_stuff_101" + "_" + "ts_03___underwater_basket_weaving" + "_db" + "p" + "wa"
  
  databaseName = getDatabaseName(courseType, username, moduleName, lessonName)
  assert databaseName == expectedDatabaseName, "Expected {}, found {}".format(expectedDatabaseName, databaseName)
  
  actualDatabaseName = createUserDatabase(courseType, username, moduleName, lessonName)
  assert actualDatabaseName == expectedDatabaseName, "Expected {}, found {}".format(expectedDatabaseName, databaseName)

  assert spark.sql(f"SHOW DATABASES LIKE '{expectedDatabaseName}'").first()["databaseName"] == expectedDatabaseName
  assert spark.sql("SELECT current_database()").first()["current_database()"] == expectedDatabaseName
  
  return True

functionPassed(testCreateUserDatabase())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `getExperimentId()`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `classroomCleanup()`

# COMMAND ----------

classroomCleanup(daLogger, "sp", getUsername(), getModuleName(), getLessonName(), False)

# COMMAND ----------

classroomCleanup(daLogger, "il", getUsername(), getModuleName(), getLessonName(), True)

# COMMAND ----------

classroomCleanup(daLogger, "sp", getUsername(), getModuleName(), getLessonName(), False)

# COMMAND ----------

classroomCleanup(daLogger, "il", getUsername(), getModuleName(), getLessonName(), True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `showStudentSurvey()`

# COMMAND ----------

html = renderStudentSurvey()
print(html)

# COMMAND ----------

showStudentSurvey()

# COMMAND ----------


