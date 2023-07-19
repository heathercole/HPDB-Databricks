# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Access Database Sample Form
# MAGIC
# MAGIC This notebook contains a sample form based on the "fm_SBML" form in the original Access database.
# MAGIC
# MAGIC The form may not behave exactly the same as the original, it's intended as a demo of the functionality.
# MAGIC
# MAGIC ## 1. Setup
# MAGIC
# MAGIC This code sets up the widgets that appear on the top of the notebook and on any dashboards created.

# COMMAND ----------

# We pull the list of locales / countries directly from the original database tables
df_locales = spark.sql("SELECT * FROM localities").toPandas() # Example of using SQL from Python.
countries = df_locales['country'].tolist()
countries.append('-') # Adds a blank option so that users aren't forced to pick a country.
countries = list(set(countries))
countries.sort()
locality = df_locales['geographical_abbreviation'].tolist() # Repeats the above for locales (provinces/states)
locality.append('-')
locality = list(set(locality))
locality.sort()

dbutils.widgets.text("Pathogen Genus", "")
dbutils.widgets.text("Pathogen Species", "")
dbutils.widgets.text("Host Genus", "")
dbutils.widgets.text("Host Species", "")
dbutils.widgets.dropdown("Country", "-", countries)
dbutils.widgets.dropdown("Locale", "-", locality)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Search Function
# MAGIC
# MAGIC This code is run automatically whenever a widget is modified above. It outputs a table showing the search results.

# COMMAND ----------

# Current behaviour is we concatenate "Pathogen Genus" and "Pathogen Species" if both are filled out. The result is then used to execute the search. Exact behaviour can be changed.
p_g = dbutils.widgets.get("Pathogen Genus") # Example of retrieving a value from the widget.
p_s = dbutils.widgets.get("Pathogen Species")
if p_s != "":
    pathogen = p_g + " " + p_s
else:
    pathogen = p_g

# Same as above, but for hosts.
h_g = dbutils.widgets.get("Host Genus")
h_s = dbutils.widgets.get("Host Species")
if h_s != "":
    host = h_g + " " + h_s
else:
    host = h_g

# We also set the country and locale variables, assuming they're set.
country = dbutils.widgets.get("Country")
if country == "-":
    country = ""
local = dbutils.widgets.get("Locale")
if local == "-":
    local=""

# This is a sample search query. It can be modified to better replicate the original forms, if needed.
query = "SELECT * FROM sbml_phcitq_temp WHERE Pathogen LIKE '%{}%' AND Host LIKE '%{}%' AND country LIKE '%{}%' AND Locality LIKE '%{}%';".format(pathogen, host, country, local)
res = spark.sql(query).toPandas()

# We display the output as a table.
try:
    display(res)
except ValueError: # If the table is empty, an error occurs. We instead display all results to resolve the error.
    query = "SELECT * FROM sbml_phcitq_temp"
    res = spark.sql(query).toPandas()
    display(res)
