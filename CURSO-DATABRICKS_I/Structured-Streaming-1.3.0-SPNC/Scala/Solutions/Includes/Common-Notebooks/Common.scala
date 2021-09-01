// Databricks notebook source
val courseType = "sp"

displayHTML("Preparing the Scala environment...")

// COMMAND ----------

// MAGIC %run ./Class-Utility-Methods

// COMMAND ----------

// MAGIC %run ./Utility-Methods

// COMMAND ----------

val moduleName = getModuleName()
val lessonName = getLessonName()
val username = getUsername()
val userhome = getUserhome()
val workingDir = getWorkingDir(courseType)
val databaseName = createUserDatabase(courseType, username, moduleName, lessonName)

displayHTML(s"""
Defining custom variables for this lesson...
<ul>
  <li><span style="color: #339966; font-weight:bold">moduleName</span>: <span style="font-style:italic; font-weight:bold">%s</span></li>
  <li><span style="color: #339966; font-weight:bold">lessonName</span>: <span style="font-style:italic; font-weight:bold">%s</span></li>
  <li><span style="color: #339966; font-weight:bold">username</span>: <span style="font-style:italic; font-weight:bold">%s</span></li>
  <li><span style="color: #339966; font-weight:bold">userhome</span>: <span style="font-style:italic; font-weight:bold">%s</span></li>
  <li><span style="color: #339966; font-weight:bold">workingDir</span>: <span style="font-style:italic; font-weight:bold">%s</span></li>
</ul>

Created a lesson-specific database:
<ul>
  <li><span style="color: #339966; font-weight:bold">databaseName</span>: <span style="font-style:italic; font-weight:bold">%s</span></li>
</ul>
""".format(moduleName,lessonName,username,userhome,workingDir,databaseName)
)

// COMMAND ----------

// MAGIC %run ./Assertion-Utils

// COMMAND ----------

// MAGIC %run ./Dummy-Data-Generator

// COMMAND ----------

// MAGIC %run ./Dataset-Mounts

// COMMAND ----------

assertDbrVersion(6, 3)

// COMMAND ----------

classroomCleanup(daLogger, courseType, username, moduleName, lessonName, false)

