# Databricks notebook source
#Parametri ricevuti da ADF
#dbutils.widgets.text("Param1", "", "")
#p_Param1 = dbutils.widgets.get("Param1") 
#if p_Param1 == "":
#    raise RuntimeError("The 'Param1' parameter is mandatory")
    
dbutils.widgets.removeAll()


# COMMAND ----------

# MAGIC %run ./Include/Globals

# COMMAND ----------

# MAGIC %run ./Include/PurviewIntegration

# COMMAND ----------

# MAGIC %run ./Include/SetupStorage

# COMMAND ----------

# MAGIC %run ./Include/Log

# COMMAND ----------

#import necessario per ottenere current timestamp
from pyspark.sql import functions as F

purviewIntegrationEnabled = spark.conf.get("var.purviewIntegrationEnabled") == "Y"

sourceFolder = "stage"
destinationFolder = "staging"
destinationSchema = "stage"

# COMMAND ----------

#Storage account utilizzato dal modello:
mainStorageAccount = spark.conf.get("var.mainStorageAccount")
#Container utilizzato dal modello:
mainStorageContainer = spark.conf.get("var.mainStorageContainer")
#Path di base da utilizzare come riferimento allo storage:
mainStorageBasePathSpark = spark.conf.get("var.mainStorageBasePathSpark")

# COMMAND ----------

#Trace:
exec_instance = ExecutionInstance.createNewInstance("ETL_Stage", "ETL_Stage", "ETL_Stage")
executionInstance_ID = str(exec_instance.executionInstanceID)
etlErrors = []

#=====================================================================================================================================================
#       SPECIFY IN THE ARRAY BELOW WHICH TABLES TO HISTORY BY WRITING TABLE'S NAME
#=====================================================================================================================================================
stageTableNamesToHistory = ['ava_pipeline','opportunity']


for tablename in stageTableNamesToHistory:
    
    task_audit = TaskAudit.createNewTaskAudit(exec_instance.executionInstanceID, f"{destinationSchema}.{tableName}")
    
    #create a new folder for historical data
    historicalFolder = f'{destinationFolder}/{destinationSchema}.{tablename}_History'

    #  Get the current date
    current_date_value = F.current_date()

    # Get the first day of the current month
    first_day_of_month = F.trunc(current_date_value, 'month')

    # Get the last day of the current month
    last_day_current_month = F.last_day(F.current_date())

    # Calculate the first day of the previous month
    first_day_of_previous_month = F.trunc(F.add_months(current_date_value, -1), 'month')

    # Calculate the last day of the previous month
    last_day_of_previous_month = F.last_day(F.add_months(current_date_value, -1))

    try:
        #read History table
        df_History = spark.read.format("delta").load(f'{mainStorageBasePathSpark}{destinationFolder}/{destinationSchema}.{tablename}_History')        
       
        # read directly staging folder the full table to History
        df_f = spark.read.format("delta").load(f'{mainStorageBasePathSpark}{destinationFolder}/{destinationSchema}.{tablename}')

        # Filter the History DataFrame where 'competenceDate' is within the current month
        df_filtered = df_History.filter(
                (F.col("InsertDate") >= first_day_of_month) &
                (F.col("InsertDate") <= last_day_current_month)
        )

        if df_filtered.count() == 0:

            #append to History Table
            df_Source_filtered = df_f.withColumn("InsertDate", current_date_value).withColumn("CompetenceDate", last_day_of_previous_month)

            df_History_without_current_month_records = df_History.filter(
                (F.col("CompetenceDate") <= first_day_of_previous_month)
            )

            #Uninsco l'istantanea creata del mese scorso alla tabella di History
            df_with_updated_values = df_History_without_current_month_records.unionAll(df_Source_filtered)

            #overwrite the delta table 
            df_History.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f'{mainStorageBasePathSpark}{historicalFolder}')
            
        else:
            #step 1 cancel current month records
            df_History_without_current_month_records = df_History.filter(
                (F.col("CompetenceDate") <= last_day_of_previous_month)
            )

            df_To_append_daily = df_f.withColumn("InsertDate", current_date_value).withColumn("CompetenceDate", first_day_of_previous_month)

            #append current table to history 
            df_with_updated_values = df_History_without_current_month_records.unionAll(df_To_append_daily)

            df_with_updated_values.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f'{mainStorageBasePathSpark}{historicalFolder}')
            

            df_with_updated_values.write.mode("overwrite").saveAsTable(f'{destinationSchema}.{tablename}_History')


            
        #Adding the 2 requested colums 
        #df_f.withColumn("InsertDate", F.current_date()w).withColumn("CompetenceDate", F.last_day(F.add_months(F.current_date(), -1))).write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f'{mainStorageBasePathSpark}{historicalFolder}')

        #df_f = df_f.withColumn("CompetenceDate", F.last_day(F.add_months(F.current_date(), -1)))

        #read History table
        #df_History = spark.read.format("delta").load(f'{mainStorageBasePathSpark}{destinationFolder}/{destinationSchema}.{tablename}_History')

    
        #df_History = spark.read.format("delta").load(f'{mainStorageBasePathSpark}{destinationFolder}/{destinationSchema}.{tablename}_History')        
       
        #write into datalake
        #df_f.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f'{mainStorageBasePathSpark}{historicalFolder}')

        #df_History.show()



      





       



        task_audit.updateTaskAudit("S", "")

        # # df_f = spark.read.parquet(fileFullPath)

        # # df_f.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("path",f'{mainStorageBasePathSpark}{destinationFolder}/{destinationSchema}.{tableName}').saveAsTable(f'{destinationSchema}.{tableName}_History')


      
    

    except Exception as e:
        
        print(e)
        etlErrors.append(f"{destinationSchema}.{tableName}: {e}")






# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.stage.ava_pipeline_history
# MAGIC order by 59 desc
