-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Access Database Sample Queries
-- MAGIC
-- MAGIC This notebook contains some of the work to demonstrate moving an Access database to Databricks.
-- MAGIC
-- MAGIC We save the Access tables as CSVs and then import them into "Data" on Databricks. This notebook implements some of the queries in the original database.
-- MAGIC
-- MAGIC ### Example - Displaying a Table
-- MAGIC
-- MAGIC We display an arbitrary table from the database

-- COMMAND ----------

SELECT * FROM default.anamorphs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Example - "Find duplicates for journalsTemp" Query
-- MAGIC
-- MAGIC Migrating the find duplicates query from the Access database

-- COMMAND ----------

SELECT first(journals_temp.journal_abbreviation) AS journal_abbreviation, count(journals_temp.journal_abbreviation) AS NumberOfDups
FROM journals_temp
GROUP BY journals_temp.journal_abbreviation
HAVING (((count(journals_temp.journal_abbreviation))>1));


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Example - "SBML_geo_lookup" Query
-- MAGIC
-- MAGIC Migrating the SBML_geo_lookup query from the Access database

-- COMMAND ----------

SELECT hp_locality_links.fk_host_pathogen_id, localities.geographical_abbreviation AS Locality, localities.country
FROM localities INNER JOIN hp_locality_links ON localities.pk_location_id=hp_locality_links.fk_location_id
ORDER BY Locality, fk_host_pathogen_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Example - "hostsbytaxa" Query
-- MAGIC
-- MAGIC Migrating the hostsbytaxa query from the Access database

-- COMMAND ----------

SELECT DISTINCT hosts.fk_higher_taxa_id, hosts.host_genus
FROM hosts
WHERE (((hosts.host_genus)<>"*Unspecified"))
ORDER BY hosts.host_genus;
