# Databricks notebook source  

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()
df = spark.read.csv("/Workspace/Users/haripriya13divi@gmail.com/Employee.csv", header=True, inferSchema=True)
df.select("name","city").show()

 

# COMMAND ----------

df.select(df.city,df.department).show()

# COMMAND ----------

df.select(df.columns[0:4]).show()

# COMMAND ----------

df.withColumn("Annual_Salary",df.salary*12).show()

# COMMAND ----------

from pyspark.sql.functions import col,  lit


df.withColumn("Country",lit(1*df.salary)).show()

# COMMAND ----------

from pyspark.sql.functions import col  

df.withColumn("Salary",col("salary").cast("double"))

# COMMAND ----------



df1=df.withColumnRenamed("emp_id","Emploee_ID") \
    .withColumnRenamed("city","City") \
    .withColumnRenamed("department","Department") \
    .withColumnRenamed("salary","Salary") \
    .withColumnRenamed("experience","Experience") \
    .withColumnRenamed("name","Name") \
    
df2=df1.withColumn("Annual_Salary",df1.Salary*12) \
    .withColumn("Bonus",col("Annual_Salary")*.04) \
    .withColumn("Country",lit("India")) \
    .withColumn("Total_paid",col("Bonus")+ col("Annual_Salary")) \
    .withColumn("Total_paid",col("Total_paid").cast("Double"))
df2.show()



# COMMAND ----------

df2.filter((df2.City=="Chennai") | (df2.Department=="IT")).show()

# COMMAND ----------

df2.filter(df2.Name.endswith("e")) \
    .filter(df2.Name.startswith("C")) \
    .show()


# COMMAND ----------

df2.filter(df2.Name.like("%ar%"))     \
    .show()

# COMMAND ----------

# df.show()
# df.distinct().show()
df.dropDuplicates(["emp_id"]).show()


# COMMAND ----------

df.sort("salary", "experience").show()

# COMMAND ----------

df2.orderBy(df2.Annual_Salary.desc()) \
    .groupBy("Department") \
    .sum("Annual_Salary") \
    .show()

# COMMAND ----------

df2.groupBy("Name","Emploee_ID","Department").sum("Annual_Salary").show()


# COMMAND ----------

dep_df = spark.read.csv("/Workspace/Users/haripriya13divi@gmail.com/Dept.csv", header=True)
dep_df.show()
join_df = df.join(dep_df,df.emp_id==dep_df.id,"left_semi")
join_df.show()


# COMMAND ----------

emp2 = spark.read.csv("/Workspace/Users/haripriya13divi@gmail.com/emp2.csv" , header = True , inferSchema = True).show()

# COMMAND ----------

# emp2 = spark.read.csv("/Workspace/Users/haripriya13divi@gmail.com/emp2.csv" , header = True , inferSchema = True)
df.union(emp2).distinct().show()

# COMMAND ----------

# df.na.fill("").show()
dff= df.fillna(0,["salary"]) \
    .na.fill("India",["city"])
   
dff.dropna(subset=["name","experience"], how='any').dropDuplicates(["name"])
dff.show()
