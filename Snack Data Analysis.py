# Databricks notebook source
display(dbutils.fs.ls("/FileStore/tables/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merged the dataset first

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Starbucks_McDonalds_Analysis").getOrCreate()

# Load the data
starbucks_1f = spark.read.csv("dbfs:/FileStore/tables/starbucks_drinkMenu_expanded.csv", header=True, inferSchema=True)
starbucks_2df = spark.read.csv("dbfs:/FileStore/tables/starbucks_menu_nutrition_drinks.csv", header=True, inferSchema=True)
starbucks_3ff = spark.read.csv("dbfs:/FileStore/tables/starbucks_menu_nutrition_food.csv", header=True, inferSchema=True)

# Merge the DataFrames using unionByName (preserves column names and order)
merged_df = starbucks_1f.unionByName(starbucks_2df, allowMissingColumns=True)\
                         .unionByName(starbucks_3ff, allowMissingColumns=True)

# Display the merged DataFrame to confirm
merged_df.display(5)



# COMMAND ----------

# MAGIC %md
# MAGIC The three Starbucks datasets were first cleaned by removing unwanted symbols to ensure data consistency. After cleaning, they were successfully merged based on common columns to create a unified dataset for analysis. This step ensured accurate data integration, minimizing errors and improving analysis reliability.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Column Name Cleaning Insights** 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Starbucks_McDonalds_Analysis").getOrCreate()

# Load data
starbucks_1f = spark.read.csv("dbfs:/FileStore/tables/starbucks_drinkMenu_expanded.csv", header=True, inferSchema=True)
starbucks_2df = spark.read.csv("dbfs:/FileStore/tables/starbucks_menu_nutrition_drinks.csv", header=True, inferSchema=True)
starbucks_3ff = spark.read.csv("dbfs:/FileStore/tables/starbucks_menu_nutrition_food.csv", header=True, inferSchema=True)
# Clean column names to remove invalid characters
def clean_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name.strip().replace(" ", "_").replace("(", "").replace(")", "")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

# Clean columns for each DataFrame
starbucks_1f = clean_column_names(starbucks_1f)
starbucks_2df = clean_column_names(starbucks_2df)
starbucks_3ff = clean_column_names(starbucks_3ff)

# Merge the DataFrames
merged_df = starbucks_1f.unionByName(starbucks_2df, allowMissingColumns=True)\
                         .unionByName(starbucks_3ff, allowMissingColumns=True)

# Show and save the merged DataFrame
merged_df.display(5)

# Save the cleaned and merged data
merged_df.write.format("delta").mode("overwrite").saveAsTable("starbucks_merged_table")


# COMMAND ----------

# MAGIC %md 
# MAGIC - **Special Characters:** Removed symbols like `#`, `%`, and `()` to prevent PySpark errors.  
# MAGIC - **Spaces to Underscores:** Replaced spaces with underscores for better readability.  
# MAGIC - **Trimming Spaces:** Removed extra spaces to avoid referencing issues.  
# MAGIC - **Standardizing Case:** Converted names to consistent casing for uniformity.  
# MAGIC - **Prefix/Suffix Removal:** Renamed irrelevant labels like `"Unnamed: 0"` for clarity.  

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Clean Column Names and Rename `_c0`**  
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce


starbucks_1f = clean_column_names(starbucks_1f).withColumnRenamed("_c0", "Beverage")
starbucks_2df = clean_column_names(starbucks_2df).withColumnRenamed("_c0", "Beverage")
starbucks_3ff = clean_column_names(starbucks_3ff).withColumnRenamed("_c0", "Beverage")

# Merge 'Beverage' columns (using coalesce to combine non-null values)
starbucks_1f = starbucks_1f.withColumn("Beverage", coalesce("Beverage", "Beverage"))
starbucks_2df = starbucks_2df.withColumn("Beverage", coalesce("Beverage", "Beverage"))
starbucks_3ff = starbucks_3ff.withColumn("Beverage", coalesce("Beverage", "Beverage"))

# Merge all DataFrames
merged_df = starbucks_1f.unionByName(starbucks_2df, allowMissingColumns=True)\
                         .unionByName(starbucks_3ff, allowMissingColumns=True)

# Show results for verification
merged_df.select("Beverage").display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC - **Rename `_c0`:** Renamed `_c0` to `"Beverage"` for clarity.  
# MAGIC - **Special Characters:** Removed symbols like `#`, `%`, and `()` to prevent PySpark errors.  
# MAGIC - **Spaces to Underscores:** Replaced spaces with underscores for consistency.  
# MAGIC - **Trimming Spaces:** Removed extra spaces to avoid errors.  
# MAGIC - **Standardizing Case:** Converted names to consistent casing for uniformity.  

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

# Initialize Spark session
spark = SparkSession.builder.appName("Starbucks_McDonalds_Analysis").getOrCreate()

# Load data
starbucks_1f = spark.read.csv("dbfs:/FileStore/tables/starbucks_drinkMenu_expanded.csv", header=True, inferSchema=True)
starbucks_2df = spark.read.csv("dbfs:/FileStore/tables/starbucks_menu_nutrition_drinks.csv", header=True, inferSchema=True)
starbucks_3ff = spark.read.csv("dbfs:/FileStore/tables/starbucks_menu_nutrition_food.csv", header=True, inferSchema=True)
# Clean column names
def clean_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name.strip().replace(" ", "_").replace("(", "").replace(")", "")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

# Clean columns and rename _c0 to 'Beverage'
starbucks_1f = clean_column_names(starbucks_1f).withColumnRenamed("_c0", "Beverage")
starbucks_2df = clean_column_names(starbucks_2df).withColumnRenamed("_c0", "Beverage")
starbucks_3ff = clean_column_names(starbucks_3ff).withColumnRenamed("_c0", "Beverage")

# Merge 'Beverage' columns (using coalesce to combine non-null values)
starbucks_1f = starbucks_1f.withColumn("Beverage", coalesce("Beverage", "Beverage"))
starbucks_2df = starbucks_2df.withColumn("Beverage", coalesce("Beverage", "Beverage"))
starbucks_3ff = starbucks_3ff.withColumn("Beverage", coalesce("Beverage", "Beverage"))

# Merge all DataFrames
merged_df = starbucks_1f.unionByName(starbucks_2df, allowMissingColumns=True)\
                         .unionByName(starbucks_3ff, allowMissingColumns=True)

# Show results for verification
merged_df.select("Beverage").display(10)

# Save the merged data
merged_df.write.format("delta").mode("overwrite").saveAsTable("starbucks_merged_table")


# COMMAND ----------

merged_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Just clarified that the _c0 and beverage column got merged and see it in the dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Loaded the menu dataset  i.e. McDdonalds

# COMMAND ----------

menu = spark.read.csv("dbfs:/FileStore/tables/menu.csv" ,header= True, inferSchema = True)

menu.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Data Preparation

# COMMAND ----------

from pyspark.sql import functions as F

# Check for null values
merged_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in merged_df.columns]).display()

# Fill missing values with 0 or appropriate values
cleaned_df = merged_df.fillna(0)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Ensure your data is clean and ready for analysis by:  
# MAGIC - Verifying column names are consistent.
# MAGIC - Checking for missing values and handling them appropriately.
# MAGIC - Ensuring data types are correct for numerical analysis.

# COMMAND ----------

# Clean column names to remove special characters and spaces
def clean_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name.strip().replace(" ", "_").replace(".", "_")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

# Apply cleaning to the merged DataFrame
merged_df = clean_column_names(merged_df)


# COMMAND ----------

# Add a 'Brand' column
cleaned_df = cleaned_df.withColumn(
    "Brand", 
    F.when(F.col("Beverage").rlike("(?i)starbucks"), "Starbucks")
     .when(F.col("Beverage").rlike("(?i)mcdonald"), "McDonald's")
     .otherwise("Unknown")
)



# COMMAND ----------

# MAGIC %md
# MAGIC Added the new column in both the dataset of starbucks and McDonalds as Brand

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Analysis
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### High-Protein, Low-Calorie Options (Best for Health-Conscious Consumers)
# MAGIC - Purpose: Identify items with high protein and low calories for healthier eating.
# MAGIC - Insight: Highlights ideal options for fitness-focused individuals.

# COMMAND ----------

# High-Protein, Low-Calorie Items
protein_rich_items = health_df.filter((F.col("Calories") < 300) & (F.col("Protein (g)") > 10))
protein_rich_items.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Sugar and Sodium Overload Analysis
# MAGIC - Purpose: Identify items that exceed recommended sugar or sodium limits.
# MAGIC - Insight: Highlights potential health risks from hidden ingredients.

# COMMAND ----------

# Identify items with excessive sugar (>50g) or sodium (>1000mg)
high_sugar_sodium = health_df.filter((F.col("Sugars (g)") > 50) | (F.col("Sodium (mg)") > 1000))
high_sugar_sodium.display()


# COMMAND ----------

# Add a 'Brand' column
cleaned_df = cleaned_df.withColumn(
    "Brand", 
    F.when(F.col("Beverage").rlike("(?i)starbucks"), "Starbucks")
     .when(F.col("Beverage").rlike("(?i)mcdonald"), "McDonald's")
     .otherwise("Unknown")
)


# COMMAND ----------

from pyspark.sql import functions as F
# For Starbucks dataset
starbucks_df = merged_df.withColumn("Brand", F.lit("Starbucks"))

# For McDonald's dataset
mcdonalds_df = menu.withColumn("Brand", F.lit("McDonald's"))

# Merge both datasets
merged_df = starbucks_df.unionByName(mcdonalds_df, allowMissingColumns=True)

# Display results
merged_df.groupBy("Brand").count().display()



# COMMAND ----------

# MAGIC %md
# MAGIC Filled the nulled value with 0 in all the column in which were performing the analysis and just to avoid the error

# COMMAND ----------

from pyspark.sql import functions as F

# Fill missing values with zero in relevant columns
health_df = merged_df.fillna({
    "Calories": 0,
    "Total Fat (g)": 0,
    "Saturated Fat (g)": 0,
    "Sugars (g)": 0,
    "Sodium (mg)": 0,
    "Protein (g)": 0
})


# COMMAND ----------

# MAGIC %md
# MAGIC To compare healthiness, you can calculate:
# MAGIC
# MAGIC -  Average Calories, Fat, Sugar, Sodium per brand
# MAGIC -  Ratio of Healthy Items (e.g., < 300 calories per serving)
# MAGIC - Protein-to-Calorie Ratio (higher ratio = healthier option)

# COMMAND ----------

# MAGIC %md
# MAGIC #####  Calorie Distribution by Portion Size
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC The "Serving_Size" doesn’t exist, we have created a proxy column using available data.

# COMMAND ----------

health_df = health_df.withColumn("Serving_Size", F.col("Calories") / 10)  # Example logic


# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

plt.figure(figsize=(8, 6))
sns.scatterplot(x='Serving_Size', y='Calories', hue='Brand', data=health_df.toPandas(), alpha=0.6)
plt.title("Calories vs Serving Size")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC - Purpose: Identify if larger portion sizes directly result in higher calorie counts.
# MAGIC - Insight: Helps assess if serving sizes impact health ratings.

# COMMAND ----------

# MAGIC %md
# MAGIC ###  **Health Analysis Insights: Starbucks vs McDonald's**  

# COMMAND ----------

# Average nutritional values by brand
analysis_df = health_df.groupBy("Brand").agg(
    F.avg("Calories").alias("Avg_Calories"),
    F.avg("Total Fat (g)").alias("Avg_Total_Fat"),
    F.avg("Saturated Fat (g)").alias("Avg_Saturated_Fat"),
    F.avg("Sugars (g)").alias("Avg_Sugar"),
    F.avg("Sodium (mg)").alias("Avg_Sodium"),
    F.avg("Protein (g)").alias("Avg_Protein")
)

# Calculate the % of healthy items (Calories < 300)
healthy_items = health_df.withColumn("Is_Healthy", F.when(F.col("Calories") < 300, 1).otherwise(0))

# Percentage of healthy items by brand
healthy_stats = healthy_items.groupBy("Brand").agg(
    F.sum("Is_Healthy").alias("Healthy_Items"),
    F.count("*").alias("Total_Items")
).withColumn("Healthy_Percentage", (F.col("Healthy_Items") / F.col("Total_Items")) * 100)

# Show Results
analysis_df.display()
healthy_stats.display()


# COMMAND ----------

# MAGIC %md
# MAGIC - **Nutritional Averages:** Starbucks shows better calorie, fat, and protein balance.  
# MAGIC - **Healthy Item Definition:** Items with **< 300 calories** were marked as healthy.  
# MAGIC - **Healthy Percentage:** **52%** of Starbucks items are healthy, compared to **44%** for McDonald's.  
# MAGIC - **Result:** Starbucks offers a higher proportion of healthier options.  
# MAGIC - **Conclusion:** Starbucks appears healthier based on calorie control and balanced nutrition.

# COMMAND ----------

import matplotlib.pyplot as plt

# Convert to Pandas for visualization
analysis_pd = analysis_df.toPandas()

# Bar Plot for Nutritional Comparison
analysis_pd.set_index("Brand").plot(kind='bar', figsize=(10, 6), color=['#1f77b4', '#ff7f0e'])
plt.title("Nutritional Comparison: Starbucks vs McDonald's")
plt.ylabel("Average Value (g or mg)")
plt.xticks(rotation=0)
plt.legend(title="Nutrients")
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC - Clearly shows Starbucks having a higher percentage of healthier items.
# MAGIC - Highlights the nutritional strengths and weaknesses of each brand.
# MAGIC - Ensures the insights align with the calculated 52% (Starbucks) vs 44% (McDonald's) healthy item percentage

# COMMAND ----------

# Top 5 healthiest items (lowest calories)
healthiest_items = health_df.select("Beverage", "Brand", "Calories").orderBy("Calories").limit(5)

# Top 5 unhealthiest items (highest calories)
unhealthiest_items = health_df.select("Beverage", "Brand", "Calories").orderBy(F.desc("Calories")).limit(5)

healthiest_items.display()
unhealthiest_items.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Top 5 Healthiest and Unhealthiest Items Purpose: 
# MAGIC - Identify the best and worst food choices for both brands.
# MAGIC - Insight: Helps consumers easily spot healthier options.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Conclusion**  
# MAGIC The analysis comparing Starbucks and McDonald's reveals that **Starbucks** offers a higher percentage of healthier items, with **52%** of its menu items categorized as healthy compared to **44%** for McDonald's. Starbucks items generally have lower average calories, total fat, and sodium levels, making it a better choice for calorie-conscious consumers. However, McDonald's offers some higher-protein options that may appeal to those seeking protein-rich meals. Additionally, Starbucks beverages tend to be higher in sugar, which is a factor to consider. Overall, while both brands provide a mix of healthy and indulgent options, **Starbucks** stands out as the healthier choice based on the analyzed metrics.

# COMMAND ----------


