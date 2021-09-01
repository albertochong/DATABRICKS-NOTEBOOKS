-- Databricks notebook source
CREATE database IF NOT exists DB_SQLANALYST;
use DB_SQLANALYST

-- COMMAND ----------

-- view strucuture of table
describe people

-- COMMAND ----------

--select * from  people
SELECT firstName, middleName, lastName, birthdate
FROM people
WHERE year(birthdate) > 1960 AND gender = 'F'

-- COMMAND ----------

SELECT year(birthDate) as birthYear,  firstName, lastName,count (*) AS total
FROM people
WHERE (firstName = 'Aletha' OR firstName = 'Laila') AND gender = 'F'  
  AND year(birthDate) > 1960
GROUP BY birthYear, firstName,lastName
ORDER BY birthYear, firstName,lastName

-- COMMAND ----------

select * from ssa_names

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW WomenBornAfter1990 AS
  SELECT firstName, middleName, lastName, year(birthDate) AS birthYear, salary 
  FROM people
  WHERE year(birthDate) > 1990 AND gender = 'F'

-- COMMAND ----------

select * from WomenBornAfter1990

-- COMMAND ----------

SELECT round(avg(salary)) AS averageSalary
FROM people

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW SSADistinctNames AS 
  SELECT DISTINCT firstName AS ssaFirstName 
  FROM ssa_names;

CREATE OR REPLACE TEMPORARY VIEW PeopleDistinctNames AS 
  SELECT DISTINCT firstName 
  FROM people;
  
SELECT firstName 
FROM PeopleDistinctNames 
INNER JOIN SSADistinctNames ON firstName = ssaFirstName

-- COMMAND ----------

SELECT count(firstName) 
FROM PeopleDistinctNames
WHERE firstName IN (
  SELECT ssaFirstName FROM SSADistinctNames
)

-- COMMAND ----------

--describe blog
select * from blog limit 2

--SELECT title, 
--       CAST(dates.publishedOn AS timestamp) AS publishedOn 
--FROM blog limit 2

-- COMMAND ----------

--Use higher order functions - TRANSFORM
SELECT authors, categories,
TRANSFORM (categories, category -> LOWER(category)) AS lwr_categories
FROM blog limit 2

-- COMMAND ----------

--Use higher order functions - FILTER
SELECT title, categories,
  FILTER (categories, category -> category = "Apache Spark") filtered
FROM blog


-- COMMAND ----------

-- Use higher order functions - EXISTS
SELECT authors, title,
  EXISTS (authors, author -> author = "Reynold Xin" 
    OR author = "Ion Stoica") selected
FROM blog

-- COMMAND ----------


