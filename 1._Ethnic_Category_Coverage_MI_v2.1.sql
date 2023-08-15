-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ethnic Cat Coverage MI Job
-- MAGIC 
-- MAGIC This workbook produces the data for tables 1,2,3a and 3b for the fortnightly ethnic cat MI production. 
-- MAGIC The code automatically rounds and suppresses the data and adds in CCG code and CCG name.
-- MAGIC 

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import datetime
-- MAGIC first_time = datetime.datetime.now()
-- MAGIC first_time

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # GET RPED VALUE TO USE IN MAPPING ETC.
-- MAGIC dbutils.widgets.get("RPED")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # PRACTICE CCG MAPPING

-- COMMAND ----------

-- map open and active practices to CCGs
CREATE OR REPLACE TEMPORARY VIEW CURRENT_PRACTICE_CCG_MAPPING AS
SELECT a.CODE AS PRACTICE_CODE
, a.COMMISSIONER_ORGANISATION_CODE AS CCG_CODE
, b.NAME AS CCG_NAME		
, b.NATIONAL_GROUPING		
, b.HIGH_LEVEL_HEALTH_GEOGRAPHY		
, b.ADDRESS_LINE_1		
, b.ADDRESS_LINE_2		
, b.ADDRESS_LINE_3		
, b.ADDRESS_LINE_4		
, b.ADDRESS_LINE_5		
, b.POSTCODE		
, b.OPEN_DATE		
, b.CLOSE_DATE		
, b.ORGANISATION_SUB_TYPE_CODE		
, b.DSS_RECORD_START_DATE		
, b.DSS_RECORD_END_DATE		
, b.DSS_SYSTEM_CREATED_DATE		
, b.DSS_SYSTEM_UPDATED_DATE
FROM ods_table AS a
LEFT JOIN ods_ccg_table AS b
ON a.COMMISSIONER_ORGANISATION_CODE = b.CODE
WHERE a.OPEN_DATE <= '$RPED'
AND (a.CLOSE_DATE IS NULL OR a.CLOSE_DATE > '$RPED')
AND a.DSS_RECORD_START_DATE <= '$RPED'
AND (a.DSS_RECORD_END_DATE IS NULL OR a.DSS_RECORD_END_DATE > '$RPED')
AND b.OPEN_DATE <= '$RPED'
AND (b.CLOSE_DATE IS NULL OR b.CLOSE_DATE > '$RPED')
AND b.DSS_RECORD_START_DATE <= '$RPED'
AND (b.DSS_RECORD_END_DATE IS NULL OR b.DSS_RECORD_END_DATE > '$RPED')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # create spark df of dataset
-- MAGIC data = spark.table('gdppr_table')
-- MAGIC 
-- MAGIC # create spark df of snomed ethnicity reference data
-- MAGIC ethnicity_ref_data = spark.table('ethnicity_mappings_table')
-- MAGIC 
-- MAGIC # join nhs data dictionary ethnic groups onto ethnicity snomed journals
-- MAGIC data_join = data.join(ethnicity_ref_data, data.CODE == ethnicity_ref_data.ConceptId,how='left') 
-- MAGIC 
-- MAGIC # import functions
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC ## remove non ethnicity snomed codes i.e. nulls
-- MAGIC snomed_no_null = data_join.where(col("PrimaryCode").isNotNull())
-- MAGIC 
-- MAGIC # remove unknown snomed ethnicity 
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "Z")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "z")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "X")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "x")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "99")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "9")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != " ")
-- MAGIC 
-- MAGIC # turn into temp view
-- MAGIC snomed_no_null.createOrReplaceTempView('gdppr_ethnicity_journals')

-- COMMAND ----------

-- Group by nhs number, record date, and ethnicity to get one journal per ethnicity, per date, per patient (removes duplicate journals)
CREATE OR REPLACE TEMPORARY VIEW JOURNAL_GROUPBY AS
SELECT NHS_NUMBER
, RECORD_DATE
, PrimaryCode AS ETHNICITY
, COUNT(*) AS COUNT_RECORDS
FROM gdppr_ethnicity_journals
GROUP BY NHS_NUMBER
, RECORD_DATE
, PrimaryCode

-- COMMAND ----------

-- get most recent record date per patient
CREATE OR REPLACE TEMPORARY VIEW RECENT_JOURNAL_DATE AS
SELECT NHS_NUMBER
, MAX(RECORD_DATE) AS RECENT_ETHNICITY_DATE
FROM JOURNAL_GROUPBY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get most recent ethnicity journal for each patient
CREATE OR REPLACE TEMPORARY VIEW GDPPR_JOURNAL_ETHNICITY AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.RECORD_DATE AS RECORDED_DATE
, "GDPPR_JOURNAL" AS SOURCE
, 1 AS PRIORITY
FROM JOURNAL_GROUPBY AS a
INNER JOIN RECENT_JOURNAL_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.RECORD_DATE = b.RECENT_ETHNICITY_DATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC PATIENT

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # import functions
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC ## remove blank ethnic field
-- MAGIC field_no_null = data_join.filter(data_join.ETHNIC != "")
-- MAGIC 
-- MAGIC # remove unknowns - not stated
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "Z")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "z")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "X")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "x")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "99")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "9")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != " ")
-- MAGIC 
-- MAGIC ## remove null ethnic field
-- MAGIC field_no_null = field_no_null.where(col("ETHNIC").isNotNull())
-- MAGIC 
-- MAGIC # turn into temp view
-- MAGIC field_no_null.createOrReplaceTempView('gdppr_ethnicity_patients')

-- COMMAND ----------

-- Group by nhs number, reporting period end date, and ethnic field to get one ethnicity per date, per patient (removes duplicates)
CREATE OR REPLACE TEMPORARY VIEW PATIENT_GROUPBY AS
SELECT NHS_NUMBER
, REPORTING_PERIOD_END_DATE
, ETHNIC AS ETHNICITY
, COUNT(*) AS COUNT_RECORDS
FROM gdppr_ethnicity_patients
GROUP BY NHS_NUMBER
, REPORTING_PERIOD_END_DATE
, ETHNIC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PATIENT_RECENT_DATE AS
SELECT NHS_NUMBER
, MAX(REPORTING_PERIOD_END_DATE) AS RECENT_ETHNICITY_DATE
FROM PATIENT_GROUPBY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get most recent ethnicity for each patient
CREATE OR REPLACE TEMPORARY VIEW GDPPR_PATIENT_ETHNICITY AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.REPORTING_PERIOD_END_DATE AS RECORDED_DATE
, "GDPPR_PATIENT" AS SOURCE
, 2 AS PRIORITY
FROM PATIENT_GROUPBY AS a
INNER JOIN PATIENT_RECENT_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.REPORTING_PERIOD_END_DATE = b.RECENT_ETHNICITY_DATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # GDPPR ONLY

-- COMMAND ----------

-- append gdppr and hes ethnicity records together
CREATE OR REPLACE TEMPORARY VIEW GDPPR_RECENT_ETHNICITY_JOINED AS

SELECT NHS_NUMBER
, ETHNICITY
, RECORDED_DATE
, SOURCE
, PRIORITY
FROM GDPPR_JOURNAL_ETHNICITY

UNION

SELECT NHS_NUMBER
, ETHNICITY
, RECORDED_DATE
, SOURCE 
, PRIORITY
FROM GDPPR_PATIENT_ETHNICITY

-- COMMAND ----------

-- get most recent date per patient
CREATE OR REPLACE TEMPORARY VIEW GDPPR_RECENT_DATE AS
SELECT NHS_NUMBER
, MAX(RECORDED_DATE) AS RECENT_ETHNICITY_DATE
FROM GDPPR_RECENT_ETHNICITY_JOINED
GROUP BY NHS_NUMBER


-- COMMAND ----------

-- get most recent ethnicity record for each patient
CREATE OR REPLACE TEMPORARY VIEW GDPPR_ETHNICITY_RECENT AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.RECORDED_DATE
, a.SOURCE
, a.PRIORITY
FROM GDPPR_RECENT_ETHNICITY_JOINED AS a
INNER JOIN GDPPR_RECENT_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.RECORDED_DATE = b.RECENT_ETHNICITY_DATE 

-- COMMAND ----------

-- find highest priority source for each patient (this is the minimum as GDPPR journal is priority 1, GDPPR patient, priority 2 etc.)
CREATE OR REPLACE TEMPORARY VIEW GDPPR_MIN_PRIORITY AS
SELECT NHS_NUMBER
, MIN(PRIORITY) AS MIN_PRIORITY
FROM GDPPR_ETHNICITY_RECENT
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get prioritised ethnicity record for each patient (remove lower priority source records)
CREATE OR REPLACE TEMPORARY VIEW GDPPR_PRIORITISED_ETHNICITY AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.RECORDED_DATE
, a.SOURCE
, a.PRIORITY
FROM GDPPR_ETHNICITY_RECENT AS a
INNER JOIN GDPPR_MIN_PRIORITY AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.PRIORITY = b.MIN_PRIORITY


-- COMMAND ----------

-- count ethnicities per NHS number as any with more than one need to be nulled and removed (for now)
CREATE OR REPLACE TEMPORARY VIEW GDPPR_DUPLICATE_ETHNICITY_NHSNUM AS
SELECT NHS_NUMBER
, COUNT(*) AS COUNT_ETHNICITIES
FROM GDPPR_PRIORITISED_ETHNICITY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- select only NHS numbers with one ethnicity i.e. remove patients who have more than one

CREATE OR REPLACE TEMPORARY VIEW ETHNICITY_ASSET_GDPPR AS
SELECT NHS_NUMBER
, ETHNICITY AS ETHNIC_CATEGORY_CODE
, RECORDED_DATE AS DATE_OF_ATTRIBUTION
, SOURCE AS DATA_SOURCE
FROM GDPPR_PRIORITISED_ETHNICITY
WHERE NHS_NUMBER IN (SELECT NHS_NUMBER FROM GDPPR_DUPLICATE_ETHNICITY_NHSNUM WHERE COUNT_ETHNICITIES = 1)

-- COMMAND ----------

-- create list of gdppr patients who are not deceased (gdppr does not include opt outs so they will be removed where they came in from HES)
CREATE OR REPLACE TEMPORARY VIEW GDPPR_ALIVE_PATIENTS AS
SELECT DISTINCT NHS_NUMBER
FROM gdppr_table
WHERE DATE_OF_DEATH IS null

-- COMMAND ----------

/* ##########################################################
###########  THIS IS THE FINAL TABLE  #######################
############################################################# */

-- pull only living patients

CREATE OR REPLACE TEMPORARY VIEW GDPPR_ETHNICITY_ASSET_V1 AS
SELECT NHS_NUMBER
, ETHNIC_CATEGORY_CODE
, DATE_OF_ATTRIBUTION
, DATA_SOURCE
FROM ETHNICITY_ASSET_GDPPR
WHERE NHS_NUMBER IN (SELECT NHS_NUMBER FROM GDPPR_ALIVE_PATIENTS)


-- COMMAND ----------

-- PULL LIST OF PATIENTS AND THEIR PRACTICE(S) AND CCG(S)
CREATE OR REPLACE TEMPORARY VIEW GDPPR_PATIENT_PRACTICE_CCG AS
SELECT a.NHS_NUMBER
, a.PRACTICE
, b.CCG_CODE
, b.CCG_NAME
FROM gdppr_table AS a
LEFT JOIN CURRENT_PRACTICE_CCG_MAPPING AS b
ON a.PRACTICE = b.PRACTICE_CODE
WHERE a.DATE_OF_DEATH IS NULL
GROUP BY a.NHS_NUMBER
, a.PRACTICE
, b.CCG_CODE
, b.CCG_NAME

-- COMMAND ----------

-- PULL IN PATIENTS PRACTICES/CCGs - MAY BE DUPLICATES DUE TO PATIENTS REGISTERED AT MORE THAN ONE PRACTICE
CREATE OR REPLACE TEMPORARY VIEW GDPPR_ETHNICITY_ASSET_PRAC_CCG AS
SELECT a.*
, b.PRACTICE
, b.CCG_CODE
, b.CCG_NAME
FROM GDPPR_ETHNICITY_ASSET_V1 AS a
LEFT JOIN GDPPR_PATIENT_PRACTICE_CCG AS b
ON a.NHS_NUMBER = b.NHS_NUMBER


-- COMMAND ----------

-- count ethnicities per NHS number as any with more than one need to be nulled and removed (for now)
CREATE OR REPLACE TEMPORARY VIEW GDPPR_DUPLICATE_ETHNICITY_NHSNUM_2 AS
SELECT CONCAT(NHS_NUMBER,PRACTICE,CCG_CODE) AS ID
, COUNT(DISTINCT ETHNIC_CATEGORY_CODE) AS COUNT_ETHNICITIES
FROM GDPPR_ETHNICITY_ASSET_PRAC_CCG
GROUP BY CONCAT(NHS_NUMBER,PRACTICE,CCG_CODE) 

-- COMMAND ----------

-- select only NHS numbers with one ethnicity i.e. remove patients who have more than one

CREATE OR REPLACE TEMPORARY VIEW ETHNICITY_ASSET_GDPPR_ONLY_FINAL AS
SELECT NHS_NUMBER
, PRACTICE
, CCG_CODE
, CCG_NAME
, ETHNIC_CATEGORY_CODE
, DATE_OF_ATTRIBUTION
, DATA_SOURCE
FROM GDPPR_ETHNICITY_ASSET_PRAC_CCG
WHERE CONCAT(NHS_NUMBER,PRACTICE,CCG_CODE) IN (SELECT ID FROM GDPPR_DUPLICATE_ETHNICITY_NHSNUM_2 WHERE COUNT_ETHNICITIES = 1)

-- COMMAND ----------

-- PULL IN PATIENTS PRACTICES AND CCGS - MAY BE DUPLICATES DUE TO PATIENTS REGISTERED AT MORE THAN ONE PRACTICE, INNER JOIN TO REMOVE DECEASED
CREATE OR REPLACE TEMPORARY VIEW ETHNICITY_ASSET_PRAC_HES_GDPPR AS
SELECT a.NHS_NUMBER
, a.ETHNIC_CATEGORY_CODE
, a.DATE_OF_ATTRIBUTION
, a.DATA_SOURCE
, b.PRACTICE
, b.CCG_CODE
, b.CCG_NAME
FROM ethnic_cat_code_table AS a
INNER JOIN GDPPR_PATIENT_PRACTICE_CCG AS b
ON a.NHS_NUMBER = b.NHS_NUMBER

-- COMMAND ----------

-- count ethnicities per NHS number as any with more than one need to be nulled and removed (for now)
CREATE OR REPLACE TEMPORARY VIEW DUPLICATE_ETHNICITY_NHSNUM_2 AS
SELECT CONCAT(NHS_NUMBER,PRACTICE,CCG_CODE) AS ID
, COUNT(DISTINCT ETHNIC_CATEGORY_CODE) AS COUNT_ETHNICITIES
FROM ETHNICITY_ASSET_PRAC_HES_GDPPR
GROUP BY CONCAT(NHS_NUMBER,PRACTICE,CCG_CODE) 

-- COMMAND ----------

-- select only NHS numbers with one ethnicity i.e. remove patients who have more than one

CREATE OR REPLACE TEMPORARY VIEW ETHNICITY_ASSET_GDPPR_HES_FINAL AS
SELECT NHS_NUMBER
, PRACTICE
, CCG_CODE
, CCG_NAME
, ETHNIC_CATEGORY_CODE
, DATE_OF_ATTRIBUTION
, DATA_SOURCE
FROM ETHNICITY_ASSET_PRAC_HES_GDPPR
WHERE CONCAT(NHS_NUMBER,PRACTICE,CCG_CODE) IN (SELECT ID FROM DUPLICATE_ETHNICITY_NHSNUM_2 WHERE COUNT_ETHNICITIES = 1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CHECKS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LIVING PATIENTS 
-- MAGIC USES THIS VIEW:
-- MAGIC ```GDPPR_PATIENT_PRACTICE_CCG ```

-- COMMAND ----------

-- COUNT LIVING PATIENTS PER CCG
CREATE OR REPLACE TEMPORARY VIEW LIVING_PATIENTS_CCG AS
SELECT CCG_CODE
, CCG_NAME
, COUNT(NHS_NUMBER) AS COUNT
--, ROUND(COUNT(DISTINCT NHS_NUMBER)/5,0)*5 AS ROUNDED_COUNT
FROM GDPPR_PATIENT_PRACTICE_CCG
GROUP BY CCG_CODE
, CCG_NAME
ORDER BY CCG_NAME

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GDPPR ONLY

-- COMMAND ----------

-- COUNT NUMBER OF PATIENTS PER CCG PER ETHNIC CAT
CREATE OR REPLACE TEMPORARY VIEW GDPPR_ONLY AS
SELECT CCG_CODE
, CCG_NAME
, ETHNIC_CATEGORY_CODE
, COUNT(NHS_NUMBER) AS COUNT
FROM ETHNICITY_ASSET_GDPPR_ONLY_FINAL
GROUP BY CCG_CODE
, CCG_NAME
, ETHNIC_CATEGORY_CODE
ORDER BY CCG_NAME
, ETHNIC_CATEGORY_CODE 

-- COMMAND ----------

-- COUNT NUMBER OF PATIENTS PER CCG PER ETHNIC CAT
CREATE OR REPLACE TEMPORARY VIEW GDPPR_ONLY_TABLE_1 AS
SELECT CCG_NAME
, CCG_CODE
, SUM(COUNT) AS KNOWN
FROM GDPPR_ONLY
GROUP BY CCG_NAME
, CCG_CODE
ORDER BY CCG_CODE

-- COMMAND ----------

-- COUNT NUMBER OF PATIENTS PER CCG PER ETHNIC CAT
CREATE OR REPLACE TEMPORARY VIEW GDPPR_AND_HES AS
SELECT CCG_CODE
, CCG_NAME
, ETHNIC_CATEGORY_CODE
, COUNT(NHS_NUMBER) AS COUNT
FROM ETHNICITY_ASSET_GDPPR_HES_FINAL
GROUP BY CCG_CODE
, CCG_NAME
, ETHNIC_CATEGORY_CODE
ORDER BY CCG_NAME
, ETHNIC_CATEGORY_CODE 

-- COMMAND ----------

-- COUNT NUMBER OF PATIENTS PER CCG PER ETHNIC CAT
CREATE OR REPLACE TEMPORARY VIEW GDPPR_AND_HES_TABLE_2 AS
SELECT CCG_NAME
, CCG_CODE
, SUM(COUNT) AS KNOWN
FROM GDPPR_AND_HES
GROUP BY CCG_NAME
, CCG_CODE
ORDER BY CCG_CODE 


-- COMMAND ----------

-- TABLE 1 (unrounded)
CREATE OR REPLACE TEMPORARY VIEW TABLE_1 AS
SELECT a.CCG_NAME
, b.CCG_CODE
, b.KNOWN AS KNOWN_ETHNIC_CATEGORY
, a.COUNT AS ALIVE_PATIENTS
, a.COUNT-b.KNOWN AS UNKNOWN_ETHNIC_CATEGORY
FROM LIVING_PATIENTS_CCG AS a
LEFT JOIN GDPPR_ONLY_TABLE_1 AS b
ON a.CCG_NAME = b.CCG_NAME
WHERE a.CCG_NAME IS NOT NULL
ORDER BY b.CCG_CODE

-- COMMAND ----------

-- TABLE 2 (unrounded)
CREATE OR REPLACE TEMPORARY VIEW TABLE_2 AS
SELECT a.CCG_NAME
, b.CCG_CODE
, b.KNOWN AS KNOWN_ETHNIC_CATEGORY
, a.COUNT AS ALIVE_PATIENTS
, a.COUNT-b.KNOWN AS UNKNOWN_ETHNIC_CATEGORY
FROM LIVING_PATIENTS_CCG AS a
LEFT JOIN GDPPR_AND_HES_TABLE_2 AS b
ON a.CCG_NAME = b.CCG_NAME
WHERE a.CCG_NAME IS NOT NULL
ORDER BY b.CCG_CODE

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #pivot data for table 3a and 3b
-- MAGIC #gdppr = spark.table('GDPPR_ONLY')
-- MAGIC gdppr_hes = spark.table('GDPPR_AND_HES')
-- MAGIC table_2 = spark.table('TABLE_2')
-- MAGIC 
-- MAGIC #gdppr_pivot = gdppr.groupby(gdppr.CCG_NAME).pivot("ETHNIC_CATEGORY_CODE").sum("COUNT").sort("CCG_NAME")
-- MAGIC gdppr_hes_pivot = gdppr_hes.groupby("CCG_NAME", "CCG_CODE").pivot("ETHNIC_CATEGORY_CODE").sum("COUNT").sort("CCG_NAME")
-- MAGIC 
-- MAGIC # join table 2 to table pivot to create table 3a
-- MAGIC table_3a = gdppr_hes_pivot.join(table_2, gdppr_hes_pivot.CCG_NAME == table_2.CCG_NAME, how='left').select(gdppr_hes_pivot["*"],table_2["UNKNOWN_ETHNIC_CATEGORY"])
-- MAGIC table_3a.createOrReplaceTempView('TABLE_3a')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Add suppression and rounding

-- COMMAND ----------

-- MAGIC %py
-- MAGIC ## create suppression function
-- MAGIC def suppress_value(valuein: int, rc: str = "*", upper: int = 100000000000) -> str:
-- MAGIC     """
-- MAGIC     Suppress values less than or equal to 7, round all other values to 5.
-- MAGIC     This function suppresses value if it is less than or equal to 7.
-- MAGIC     If value is 0 then it will remain as 0.
-- MAGIC     All other values will be rounded to the nearest 5.
-- MAGIC     
-- MAGIC     Parameters
-- MAGIC     ----------
-- MAGIC     valuein : int
-- MAGIC         Metric value
-- MAGIC     rc : str
-- MAGIC         Replacement character if value needs suppressing
-- MAGIC     upper : int
-- MAGIC         Upper limit for suppression of numbers
-- MAGIC     Returns
-- MAGIC     -------
-- MAGIC     out : str
-- MAGIC         Suppressed value (*), 0 or valuein if greater than 7
-- MAGIC     ExamplesA
-- MAGIC     --------
-- MAGIC     >>> suppress_value(3)
-- MAGIC     '*'
-- MAGIC     >>> suppress_value(24)
-- MAGIC     '25'
-- MAGIC     >>> suppress_value(0)
-- MAGIC     '0'
-- MAGIC     """
-- MAGIC     base = 5
-- MAGIC 
-- MAGIC     if not isinstance(valuein, int):
-- MAGIC         raise ValueError("The input is not an integer.")
-- MAGIC 
-- MAGIC     if valuein < -10:
-- MAGIC         raise ValueError("The input is less than -10.")
-- MAGIC     elif valuein == 0:
-- MAGIC         valueout = str(valuein)
-- MAGIC     elif valuein >= 1 and valuein <= 7:
-- MAGIC         valueout = rc
-- MAGIC     elif valuein > 7 and valuein <= upper:
-- MAGIC         valueout = str(base * round(valuein / base))
-- MAGIC     else:
-- MAGIC         raise ValueError("The input is greater than 100000000000.")
-- MAGIC     return valueout

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # import and view table 1 unrounded
-- MAGIC import pandas as pd
-- MAGIC table1_unrounded = spark.table('TABLE_1').toPandas().sort_values(by=['CCG_NAME'])
-- MAGIC 
-- MAGIC # add total row at the bottom for known, alive and unknown, fill nulls with total i.e. fill CCG name with total, sort by CCG name
-- MAGIC table1_unrounded_with_totals = table1_unrounded.append(table1_unrounded[['KNOWN_ETHNIC_CATEGORY', 'ALIVE_PATIENTS', 'UNKNOWN_ETHNIC_CATEGORY']].sum().rename('TOTAL'))#.fillna({'CCG_NAME': 'TOTAL'}).sort_values(by=['CCG_NAME'])
-- MAGIC 
-- MAGIC # rename CCG_name row for totals as TOTAL
-- MAGIC table1_unrounded_with_totals['CCG_NAME'] = table1_unrounded_with_totals[['CCG_NAME']].fillna(value='TOTAL')
-- MAGIC table1_unrounded_with_totals['CCG_CODE'] = table1_unrounded_with_totals[['CCG_CODE']].fillna(value='ZZZTOTAL')
-- MAGIC 
-- MAGIC ## CONVERT TO INTEGER and apply suppressino function
-- MAGIC table1_unrounded_with_totals.CCG_NAME = table1_unrounded_with_totals.CCG_NAME.astype(str)
-- MAGIC table1_unrounded_with_totals.CCG_CODE = table1_unrounded_with_totals.CCG_CODE.astype(str)
-- MAGIC table1_unrounded_with_totals[['KNOWN_ETHNIC_CATEGORY','ALIVE_PATIENTS','UNKNOWN_ETHNIC_CATEGORY']] = table1_unrounded_with_totals[['KNOWN_ETHNIC_CATEGORY','ALIVE_PATIENTS','UNKNOWN_ETHNIC_CATEGORY']].fillna(0.0).astype(int).applymap(suppress_value)
-- MAGIC #table1_unrounded_with_totals.KNOWN_ETHNIC_CATEGORY = table1_unrounded_with_totals.KNOWN_ETHNIC_CATEGORY.astype(int).apply(suppress_value)
-- MAGIC #table1_unrounded_with_totals.ALIVE_PATIENTS = table1_unrounded_with_totals.ALIVE_PATIENTS.astype(int).apply(suppress_value)
-- MAGIC #table1_unrounded_with_totals.UNKNOWN_ETHNIC_CATEGORY = table1_unrounded_with_totals.UNKNOWN_ETHNIC_CATEGORY.astype(int).apply(suppress_value)
-- MAGIC 
-- MAGIC ## add on % rounded to 3dp
-- MAGIC table1_rounded = table1_unrounded_with_totals.sort_values(by=['CCG_NAME'])
-- MAGIC table1_rounded['PERCENTAGE'] = round(table1_rounded['KNOWN_ETHNIC_CATEGORY'].astype(int)/table1_rounded['ALIVE_PATIENTS'].astype(int),3)
-- MAGIC 
-- MAGIC ## print final table 1
-- MAGIC ##table1_rounded

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # import table 2 unrounded as pandas df
-- MAGIC import pandas as pd
-- MAGIC table2_unrounded = table_2.toPandas().sort_values(by=['CCG_NAME'])
-- MAGIC 
-- MAGIC # add total row at the bottom for known, alive and unknown, fill nulls with total i.e. fill CCG name with total, sort by CCG name
-- MAGIC table2_unrounded_with_totals = table2_unrounded.append(table2_unrounded[['KNOWN_ETHNIC_CATEGORY', 'ALIVE_PATIENTS', 'UNKNOWN_ETHNIC_CATEGORY']].sum().rename('TOTAL')).fillna({'CCG_NAME': 'TOTAL', 'CCG_CODE':'ZZZTOTAL'}).sort_values(by=['CCG_NAME'])
-- MAGIC 
-- MAGIC # rename CCG_name row for totals as TOTAL
-- MAGIC table2_unrounded_with_totals['CCG_NAME'] = table2_unrounded_with_totals[['CCG_NAME']].fillna(value='TOTAL')
-- MAGIC table2_unrounded_with_totals['CCG_CODE'] = table2_unrounded_with_totals[['CCG_CODE']].fillna(value='ZZZTOTAL')
-- MAGIC 
-- MAGIC ## CONVERT TO INTEGER + apply suppresison function
-- MAGIC table2_unrounded_with_totals.CCG_NAME = table2_unrounded_with_totals.CCG_NAME.astype(str)
-- MAGIC table2_unrounded_with_totals.CCG_CODE = table2_unrounded_with_totals.CCG_CODE.astype(str)
-- MAGIC table2_unrounded_with_totals[['KNOWN_ETHNIC_CATEGORY','ALIVE_PATIENTS','UNKNOWN_ETHNIC_CATEGORY']] = table2_unrounded_with_totals[['KNOWN_ETHNIC_CATEGORY','ALIVE_PATIENTS','UNKNOWN_ETHNIC_CATEGORY']].fillna(0.0).astype(int).applymap(suppress_value)
-- MAGIC 
-- MAGIC #table2_unrounded_with_totals.KNOWN_ETHNIC_CATEGORY = table2_unrounded_with_totals.KNOWN_ETHNIC_CATEGORY.astype(int).apply(suppress_value)
-- MAGIC #table2_unrounded_with_totals.ALIVE_PATIENTS = table2_unrounded_with_totals.ALIVE_PATIENTS.astype(int).apply(suppress_value)
-- MAGIC #table2_unrounded_with_totals.UNKNOWN_ETHNIC_CATEGORY = table2_unrounded_with_totals.UNKNOWN_ETHNIC_CATEGORY.astype(int).apply(suppress_value)
-- MAGIC 
-- MAGIC ## add on percentage rounded to 3dp
-- MAGIC table2_rounded = table2_unrounded_with_totals.sort_values(by=['CCG_NAME'])
-- MAGIC table2_rounded['PERCENTAGE'] = round(table2_rounded['KNOWN_ETHNIC_CATEGORY'].astype(int)/table2_rounded['ALIVE_PATIENTS'].astype(int),3)
-- MAGIC 
-- MAGIC ## print final table 2
-- MAGIC ##table2_rounded

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # import table 3a unrounded as pandas df
-- MAGIC import pandas as pd
-- MAGIC table3a_unrounded = table_3a.toPandas().sort_values(by=['CCG_NAME'])
-- MAGIC 
-- MAGIC # add total row at the bottom, rename index as TOTAL, sort by CCG name
-- MAGIC table3a_unrounded_with_totals = table3a_unrounded.append(table3a_unrounded[['A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','T','W','UNKNOWN_ETHNIC_CATEGORY']].sum().rename('TOTAL'))#.sort_values(by=['CCG_NAME'])
-- MAGIC 
-- MAGIC # rename CCG_name row for totals as TOTAL
-- MAGIC table3a_unrounded_with_totals['CCG_NAME'] = table3a_unrounded_with_totals[['CCG_NAME']].fillna(value='TOTAL').astype(str)
-- MAGIC table3a_unrounded_with_totals['CCG_CODE'] = table3a_unrounded_with_totals[['CCG_CODE']].fillna(value='ZZZTOTAL').astype(str)
-- MAGIC 
-- MAGIC # fill nulls with 0, convert to integer and apply suppression function
-- MAGIC table3a_unrounded_with_totals[['A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','T','W','UNKNOWN_ETHNIC_CATEGORY']] = table3a_unrounded_with_totals[['A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','T','W','UNKNOWN_ETHNIC_CATEGORY']].fillna(0.0).astype(int).applymap(suppress_value)
-- MAGIC 
-- MAGIC # display final table 3a
-- MAGIC table3a_rounded = table3a_unrounded_with_totals.sort_values(by=['CCG_NAME'])
-- MAGIC #table3a_rounded.tail()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # function to turn string to integer if its possible
-- MAGIC def try_to_int(obj):
-- MAGIC     try:
-- MAGIC         return str(int(float(obj)))
-- MAGIC     except (ValueError, TypeError):
-- MAGIC         return obj

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # convert 3a to integers if not *
-- MAGIC table3a_as_int = table3a_rounded.applymap(try_to_int)
-- MAGIC 
-- MAGIC # join living patients on 
-- MAGIC table3a_with_alive = pd.merge(table3a_as_int,table2_rounded[['CCG_NAME','ALIVE_PATIENTS']], on='CCG_NAME', how='left')
-- MAGIC # convert * to null for now
-- MAGIC table3a_with_alive[['A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','T','W','UNKNOWN_ETHNIC_CATEGORY', 'ALIVE_PATIENTS']] = table3a_with_alive[['A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','T','W','UNKNOWN_ETHNIC_CATEGORY', 'ALIVE_PATIENTS']].apply(pd.to_numeric, errors = 'coerce')
-- MAGIC 
-- MAGIC # calculate % and round to 3dp
-- MAGIC table3a_with_alive[['A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','T','W','UNKNOWN_ETHNIC_CATEGORY']] = table3a_with_alive[['A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','T','W','UNKNOWN_ETHNIC_CATEGORY']].div(table3a_with_alive.ALIVE_PATIENTS, axis=0).round(3)
-- MAGIC 
-- MAGIC # drop alive patients column and fill nulls with * again
-- MAGIC table3b = table3a_with_alive.drop('ALIVE_PATIENTS', 1).fillna('*')
-- MAGIC 
-- MAGIC # print final table 3b
-- MAGIC ##table3b

-- COMMAND ----------

-- MAGIC %py
-- MAGIC table1_rounded = table1_rounded.astype(str)
-- MAGIC table2_rounded = table2_rounded.astype(str)
-- MAGIC table3a_rounded = table3a_rounded.astype(str)
-- MAGIC table3b = table3b.astype(str)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## MAPPING

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW CCG_MAPPING_PUBLICATION AS
SELECT CCG_CODE
, CCG_NAME		
, NATIONAL_GROUPING		
, HIGH_LEVEL_HEALTH_GEOGRAPHY		
, ADDRESS_LINE_1		
, ADDRESS_LINE_2		
, ADDRESS_LINE_3		
, ADDRESS_LINE_4		
, ADDRESS_LINE_5		
, POSTCODE		
, OPEN_DATE		
, CLOSE_DATE		
, ORGANISATION_SUB_TYPE_CODE		
, DSS_RECORD_START_DATE		
, DSS_RECORD_END_DATE		
, DSS_SYSTEM_CREATED_DATE		
, DSS_SYSTEM_UPDATED_DATE
FROM CURRENT_PRACTICE_CCG_MAPPING
GROUP BY CCG_CODE
, CCG_NAME		
, NATIONAL_GROUPING		
, HIGH_LEVEL_HEALTH_GEOGRAPHY		
, ADDRESS_LINE_1		
, ADDRESS_LINE_2		
, ADDRESS_LINE_3		
, ADDRESS_LINE_4		
, ADDRESS_LINE_5		
, POSTCODE		
, OPEN_DATE		
, CLOSE_DATE		
, ORGANISATION_SUB_TYPE_CODE		
, DSS_RECORD_START_DATE		
, DSS_RECORD_END_DATE		
, DSS_SYSTEM_CREATED_DATE		
, DSS_SYSTEM_UPDATED_DATE
ORDER BY CCG_CODE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # EXPORT

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # convert to spark df for export
-- MAGIC table1_rounded_sdf = spark.createDataFrame(table1_rounded)
-- MAGIC table2_rounded_sdf = spark.createDataFrame(table2_rounded)
-- MAGIC table3a_rounded_sdf = spark.createDataFrame(table3a_rounded)
-- MAGIC table3b_rounded_sdf = spark.createDataFrame(table3b)
-- MAGIC mapping = spark.table('CCG_MAPPING_PUBLICATION')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from dsp.common.exports import create_excel_for_download
-- MAGIC from dsp.common.digitrials import cover_page, interpretation_page, footnotes_page, add_digitrials_styling
-- MAGIC 
-- MAGIC pre_pages = [{
-- MAGIC 'function': cover_page,
-- MAGIC 'sheet_name': 'Ethnicity Asset',
-- MAGIC 'options': {
-- MAGIC 'applicant_name': 'name',
-- MAGIC 'organisation': 'NHS Digital',
-- MAGIC 'ref_number': '',
-- MAGIC 'title': 'Ethnicity Asset',
-- MAGIC 'table_title': 'Ethnic Category breakdowns, GDPPR only, GDPPR+HES'
-- MAGIC }},
-- MAGIC {
-- MAGIC 'function': interpretation_page,
-- MAGIC 'sheet_name': 'Ethnic_category_breakdown',
-- MAGIC 'options': {
-- MAGIC 'organisation': 'NHS Digital',
-- MAGIC 'tables': [{
-- MAGIC 'name': 'Ethnic_category_breakdown',
-- MAGIC 'interpretation': 'Ethnic_category_breakdown'
-- MAGIC },
-- MAGIC {
-- MAGIC 'name': 'this can be customised',
-- MAGIC 'interpretation': 'More of these can be added if you want'
-- MAGIC }]
-- MAGIC }}
-- MAGIC ]
-- MAGIC 
-- MAGIC post_pages = [{
-- MAGIC 'function': footnotes_page,
-- MAGIC 'sheet_name': 'Footnotes',
-- MAGIC 'options': {
-- MAGIC 'footnotes': [{
-- MAGIC 'title': 'Footnotes',
-- MAGIC 'content': 'This is footnote 1'
-- MAGIC },
-- MAGIC {
-- MAGIC 'title': 'Another footnote',
-- MAGIC 'content': 'These can be customised'
-- MAGIC }]
-- MAGIC }
-- MAGIC }]
-- MAGIC '''
-- MAGIC You can add multiple dataframes in the dataframes section. Each dataframe gets outputted to a separate sheet
-- MAGIC '''
-- MAGIC data = create_excel_for_download(dataframes=[table1_rounded_sdf, table2_rounded_sdf, table3a_rounded_sdf, table3b_rounded_sdf, mapping],
-- MAGIC pre_pages=pre_pages,
-- MAGIC post_pages=post_pages,
-- MAGIC style_function=add_digitrials_styling,
-- MAGIC max_mb=20
-- MAGIC )
-- MAGIC filename = 'excel_download.xlsx'
-- MAGIC displayHTML(f"<h4>your file {filename} is ready to <a href='data:application/octet-stream;base64,{data.decode()}' download='{filename}'>download</a></h4>")

-- COMMAND ----------

-- double check that RPED entered is the same as the most recent in the data
SELECT MAX(REPORTING_PERIOD_END_DATE) 
FROM gdppr_table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC ## display how long the process took to run
-- MAGIC later_time = datetime.datetime.now()
-- MAGIC difference = later_time - first_time
-- MAGIC seconds_in_day = 24 * 60 * 60
-- MAGIC divmod(difference.days * seconds_in_day + difference.seconds, 60)

-- COMMAND ----------


