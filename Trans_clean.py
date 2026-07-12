# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, lit
from pyspark.sql.types import *

df = spark.read.csv("/Workspace/Users/haripriya13divi@gmail.com/Employee.csv",header = True, inferSchema = True)
#claendata
df_clean = df.fillna("India",["city"]) \
             .fillna(10000,["salary"]) \
             .fillna(0,["experience"])
df_clean2 =df_clean.dropDuplicates(["name"]) \
                    .dropna(subset=["name"])

df_city = df_clean2.filter(df_clean2.city=="India")
df_add_annual_salary = df_clean2.withColumn("CTC",df_clean2.salary*12).sort(col("CTC").desc())
df_add_bonus = df_add_annual_salary.withColumn("Bonus",df.salary*0.10)
df1 =df_add_bonus.withColumn("Country",lit("India")).select("name","emp_id","department","salary") 
df1.groupBy("department").sum("salary").show()


