-- Databricks notebook source
use db_sqlanalyst;



-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW HomicidesBoston AS
select INCIDENT_NUMBER, OFFENSE_CODE_GROUP, district, year, MONTH, DAY_OF_WEEK, STREET, longitude, latitude
from crime_data_boston 
WHERE lower(OFFENSE_CODE_GROUP) = 'homicide'

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW HomicidesChigago AS  
select caseNumber, primaryType, block, arrest, district, year, MONTH(date) as Month, longitude, latitude
from crime_data_chicago
where lower(primaryType) like 'homicide%'

--select * from crime_data_chicago

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW HomicidesNY AS 
select complaintNumber, offenseDescription, year(reportDate) year, MONTH(reportDate) as Month, longitude, latitude
from crime_data_ny
WHERE lower(offenseDescription) LIKE 'murder%' OR lower(offenseDescription) LIKE 'homicide%';

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW HomicidesBoston_Chicago_NewYork AS
select MONTH, OFFENSE_CODE_GROUP, 'Boston' as city from HomicidesBoston
union all
select MONTH, primaryType, 'Chicago' as city from HomicidesChigago
union all
select MONTH, offenseDescription, 'NY' as city from HomicidesNY


-- COMMAND ----------

--Get total himicides per city and month
select city, month, count(*) as HomicidesQty 
from HomicidesBoston_Chicago_NewYork
group by city, month
order by city, month

-- COMMAND ----------

--get total homicides per month
select month, count(*) as HomicidesQty 
from HomicidesBoston_Chicago_NewYork
group by month
order by month

-- COMMAND ----------

SELECT distinct(primaryType) FROM crime_data_chicago 

-- COMMAND ----------


