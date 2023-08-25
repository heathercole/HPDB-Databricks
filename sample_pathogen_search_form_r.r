# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Access Database Sample Form (R version)
# MAGIC
# MAGIC This notebook contains a sample form based on the "fm_SBML" form in the original Access database.
# MAGIC
# MAGIC The form may not behave exactly the same as the original, it's intended as a demo of the functionality.
# MAGIC
# MAGIC ## 1. Setup Databricks
# MAGIC
# MAGIC We'll load a library to use our Spark Dataframes and start a new session.

# COMMAND ----------

# Load necessary libraries
library(SparkR)

# Initialize Spark session
sparkR.session()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 2. Setup Widgets
# MAGIC
# MAGIC This code sets up the widgets that appear on the top of the notebook and on any dashboards created.

# COMMAND ----------

# We pull the list of locales / countries directly from the original database tables
df_locales <- sql("SELECT * FROM localities")
countries <- collect(select(df_locales, "country"))[[1]]
countries <- c(countries, "-") # Adds a blank option so that users aren't forced to pick a country.
countries <- unique(countries)
countries <- sort(countries)
locality <- collect(select(df_locales, "geographical_abbreviation"))[[1]] # Repeats the above for locales (provinces/states)
locality <- c(locality, "-")
locality <- unique(locality)
locality <- sort(locality)

# Convert to R lists
countries_list <- as.list(countries)
locality_list <- as.list(locality)

# Create widgets
sparkR.session()
sc <- sparkR.session()
dbutils.widgets.text("Pathogen Genus", "")
dbutils.widgets.text("Pathogen Species", "")
dbutils.widgets.text("Host Genus", "")
dbutils.widgets.text("Host Species", "")
dbutils.widgets.dropdown("Country", "-", countries_list)
dbutils.widgets.dropdown("Locale", "-", locality_list)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Search Function
# MAGIC
# MAGIC This code is run automatically whenever a widget is modified above. It outputs a table showing the search results.

# COMMAND ----------

# Current behaviour is we concatenate "Pathogen Genus" and "Pathogen Species" if both are filled out. The result is then used to execute the search. Exact behaviour can be changed.
p_g <- dbutils.widgets.get("Pathogen Genus") # Example of retrieving a value from the widget.
p_s <- dbutils.widgets.get("Pathogen Species")
if (p_s != "") {
    pathogen <- paste(p_g, p_s, sep = " ")
} else {
    pathogen <- p_g
}

# Same as above, but for hosts.
h_g <- dbutils.widgets.get("Host Genus")
h_s <- dbutils.widgets.get("Host Species")
if (h_s != "") {
    host <- paste(h_g, h_s, sep = " ")
} else {
    host <- h_g
}

# We also set the country and locale variables, assuming they're set.
country <- dbutils.widgets.get("Country")
if (country == "-") {
    country <- ""
}
local <- dbutils.widgets.get("Locale")
if (local == "-") {
    local <- ""
}

# This is a sample search query. It can be modified to better replicate the original forms if needed.
query <- paste("SELECT * FROM sbml_phcitq_temp WHERE Pathogen LIKE '%", pathogen, "%' AND Host LIKE '%", host, "%' AND country LIKE '%", country, "%' AND Locality LIKE '%", local, "%';", sep = "")
res <- sql(query)

# Display the output as a table.
tryCatch({
  display(res)
}, error = function(e) {
  query <- "SELECT * FROM sbml_phcitq_temp"
  res <- sql(query)
  display(res)
})
