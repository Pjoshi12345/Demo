# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
data = [('James','Smith','M',"Sales",30,"192.169.12.2","https//"),('Anna','Rose','F',"Admin",41,"192.169.67.2","https//"),
  ('Robert','Williams','M',"Sales",62,"192.169.34.56","https//"), 
]
columns = ["firstname","lastname","gender","dept","salary","Ipaddress","Url"]
df =spark.createDataFrame(data=data,schema=columns)
df.show()
df.filter(col('firstname').like("%mes%")).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
data = [('Aditya','Patil','M',"Marketing",30,"192.169.23.45","https//"),('Neha','Kumar','F',"Fin",34,"192.169.36.123","https//"),
  ('Ram','Jain','M',"Marketing",56,"192.169.36.23","https//"), 
]
columns = ["firstname","lastname","gender","dept","salary","Ipaddress","Url"]
anotherdf =spark.createDataFrame(data=data,schema=columns)
anotherdf.show()

# COMMAND ----------

# DBTITLE 1,Substitution
def Substitution(deptname):
    DeptDic={'Sales':'selas','Admin':'nimda','IT':'ti'}
    value = DeptDic.get(deptname)
    return value
#     for key, value in stateDic.items():
#         if deptname == 'Sales':            
#             return(value)
#         elif deptname == 'Admin':
#             value = stateDic.get(deptname)
#             return(value)
#         elif deptname == 'IT':
#             return(value)
#         else:
#             return('-')

# COMMAND ----------

def Nullification(deptname):
    return None

# COMMAND ----------

def Ipaddress_De_identification(text):
    result = ""
    position=4
   # transverse the plain text
    for i in range(len(text)):
        char = text[i]
      # Encrypt uppercase characters in plain text
        if(char.isupper()):
            result += chr((ord(char) + position - 65) % 26 + 65)
      # Encrypt lowercase characters in plain text
        else:
            result += chr((ord(char) + position - 97) % 26 + 97)
    return result

# COMMAND ----------

import base64

def Url_De_identification_InBase64(text):
    #  the string to be encrypted 
    str_encrypt = text
    #  encryption method 
    #  convert to byte type 
    base64_encrypt = base64.b64encode(str_encrypt.encode())
    return base64_encrypt

# COMMAND ----------

import hashlib
def EmailAddress_De_identification_InBinary(text):
    hash = hashlib.md5(text.encode())
    hash.update(text.encode("utf8"))
    value = hash.digest()
            
    return repr(value)

# COMMAND ----------

import hmac
import hashlib
def Phone_De_identification_InHmac(str_encrypt):
    key="abc"
    # keyhash
    mac = hmac.new(key.encode(encoding="utf-8"),str_encrypt.encode("utf8"),hashlib.md5)
    value=mac.hexdigest()  # 
    return value

# COMMAND ----------

from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType
udf_dept_desc = udf(lambda x:Substitution(x),StringType())
udf_dept_nullification_desc = udf(lambda x:Nullification(x),StringType())
udf_IpAddr_Deidentification = udf(lambda x:Ipaddress_De_identification(x),StringType())
udf_Url_Deidentification = udf(lambda x:Url_De_identification_InBase64(x),StringType())
udf_EmailAddress_Deidentification_InBinary = udf(lambda x:EmailAddress_De_identification_InBinary(x),StringType())
udf_SIN_Deidentification_InHmac = udf(lambda x:Phone_De_identification_InHmac(x),StringType())

# COMMAND ----------

df.withColumn("Dept",udf_dept_desc(col("Dept"))).select("firstname","lastname","gender","Dept").show()
df.withColumn("Salary1",udf_dept_nullification_desc(col("Salary"))).select("firstname","lastname","gender","Salary","Salary1").show()
df.withColumn("Ipaddress",udf_IpAddr_Deidentification(col("Ipaddress"))).select("firstname","lastname","gender","Salary","Ipaddress").show()
df.withColumn("Converted First Name",udf_Url_Deidentification(col("Url"))).select("firstname","lastname","gender","Converted First Name").show()
df.withColumn("Converted last name",udf_EmailAddress_Deidentification_InBinary(col("lastname"))).select("firstname","lastname","gender","Converted last Name").show()
df.withColumn("Hmac Dept",udf_SIN_Deidentification_InHmac(col("Dept"))).select("firstname","lastname","gender","Hmac Dept").show()


# COMMAND ----------

import pyspark.sql
from pyspark.sql.functions import filter
from pyspark.sql.functions import when

#Reading Common Record Format File from ADLS
#Superset Dataframe 
#file_location = r"/mnt/staging/Common Record Format.txt"
file_location_another = r"/FileStore/CustomerData_2.csv"
file_location = r"/FileStore/TableMapping.csv"
file_type = "csv"

#Schema extraction
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","


customerschema = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
columns  = [col.strip() for col in customerschema.columns]
customerschema = customerschema.toDF(*columns)
customerschema.show()
newrdd = customerschema.rdd
keypair_rdd = newrdd.map(lambda x : (x[0],x[1]))
dict = keypair_rdd.collectAsMap()
print(dict)


# COMMAND ----------

import pyspark.sql
from pyspark.sql.functions import filter
from pyspark.sql.functions import when

file_location_another = r"/FileStore/FieldRuleMappingClm.csv"
file_type = "csv"

#Schema extraction
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","


schemamapping = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location_another)
columns  = [col.strip() for col in schemamapping.columns]
schemamapping = schemamapping.toDF(*columns)
schemamapping.show()
newrdd = schemamapping.rdd
keypair_rdd = newrdd.map(lambda x : (x[0],x[1]))
dict2 = keypair_rdd.collectAsMap()
print(dict2)

# COMMAND ----------

import pyspark.sql
from pyspark.sql.functions import filter
from pyspark.sql.functions import when

#Reading Common Record Format File from ADLS
#Superset Dataframe 
file_location_another = r"/FileStore/FieldRuleMappingMbr.csv"
file_type = "csv"

#Schema extraction
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","


schemamapping = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location_another)
columns  = [col.strip() for col in schemamapping.columns]
schemamapping = schemamapping.toDF(*columns)
schemamapping.show()
newrdd = schemamapping.rdd
keypair_rdd = newrdd.map(lambda x : (x[0],x[1]))
dict3 = keypair_rdd.collectAsMap()
print(dict3)

# COMMAND ----------

def genericRuleEngine(dict,df):
    for key1 in dict.keys():        
        if dict[key1] == 'udf_Url_Deidentification':
                    df= df.withColumn("Hashed_url"+key1, udf_IpAddr_Deidentification(col(key1)))                                  
        elif dict[key1] == 'udf_EmailAddress_Deidentification_InBinary':
                    df = df.withColumn("Hashed_Email"+key1,udf_EmailAddress_Deidentification_InBinary(col(key1)))        
        elif dict[key1] == 'udf_dept_nullification_desc':
            df= df.withColumn("Hashed_url"+key1,udf_dept_nullification_desc(col(key1)))         
        elif dict[key1] == 'udf_SIN_Deidentification_InHmac':                                        
            df = df.withColumn("Hashed_Email"+key1, udf_dept_nullification_desc(col(key1)))                                                         
    return df

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, ArrayType, IntegerType,DateType,TimestampType
from pyspark.sql.functions import col,lit
import pyspark.sql.functions as f
from pyspark.sql.functions import *
import datetime

def dictionarydata_deidentification(dict1,df,dict2,disct3):
    columns = ["clientname","tablename","CRF_Columns_list"]
    schema = StructType([
        StructField('clientname', StringType(), True),
        StructField('tablename', StringType(), True),
        StructField('CRF_Columns_list', ArrayType(StringType(), True), True),
        StructField('Timestamp', StringType(), True)])             
    
    sampledata=[("asd","xyz",["ABC","XYZ"],"ts")]
    df9=spark.createDataFrame(data=sampledata,schema=schema)
                         
    test_word ="mbr"
    test_word2 ="clm"
        
    mbr_matching = [s for s in dict1.keys() if test_word in s]
    clm_matching = [s for s in dict1.keys() if test_word2 in s]
            
    for table in mbr_matching :
        CRF_Columns_list = list(dict3.keys())                
        
        value = dict.get(table)        
        df = genericRuleEngine(dict3,df)            
        df2 = df9.withColumn("clientname", lit(value)) \
                                         .withColumn("tablename", lit(table)) \
                                         .withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in CRF_Columns_list])).withColumn("Timestamp", current_timestamp())
 
        df2.write.format("delta").mode("append").saveAsTable("Logevents")
    
    for table in clm_matching :
        CRF_Columns_list = list(dict2.keys())        
        value = dict.get(table)        
        df = genericRuleEngine(dict2,anotherdf)
        display(df)
        df2 = df9.withColumn("clientname", lit(value)) \
                                         .withColumn("tablename", lit(table)) \
                                         .withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in CRF_Columns_list])).withColumn("Timestamp", current_timestamp())
 
        df2.write.format("delta").mode("append").saveAsTable("Logevents")
    
    return df

# COMMAND ----------

result=dictionarydata_deidentification(dict,df,dict2,dict3)
display(result) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Logevents
# MAGIC --drop table Logevents
# MAGIC --DELETE FROM Logevents where clientname="HealthFirst"

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StructType,StructField, StringType, ArrayType, IntegerType,DateType,TimestampType
from array import array
from pyspark.sql.functions import *
import datetime

columns = ["clientname","tablename","CRF_Columns_list","TimeStamp"]
schema = StructType([
        StructField('clientname', StringType(), True),
        StructField('tablename', StringType(), True),
        StructField("CRF_Columns_list", ArrayType(IntegerType(), True), True),
        StructField('Timestamp', StringType(), True)])

sampledata=[("asd","xyz",[1,2],"ts")]
df10=spark.createDataFrame(data=sampledata,schema=schema)
list = [1,2,3,4]
final_df = df10.withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in list])).show()
final_df1 = df10.withColumn("Timestamp", current_timestamp() ).show()




# COMMAND ----------

from datetime import datetime, timezone

utc_dt = datetime.now(timezone.utc) # UTC time
dt = utc_dt.astimezone() 
print(dt)

import pytz

tz = pytz.timezone('US/Central')
berlin_now = datetime.now(tz)
print(berlin_now.strftime("%Y-%m-%d %H:%M:%S"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --DESCRIBE HISTORY LogEvents
# MAGIC --SELECT max(version) -1 as previousVersion  FROM (DESCRIBE HISTORY yourTblName)
# MAGIC 
# MAGIC 
# MAGIC --select * from LogEvents Version as of 7
# MAGIC 
# MAGIC --RESTORE TABLE LogEvents  TO VERSION AS OF  6
# MAGIC select * from LogEvents 
# MAGIC --describe detail LogEvents

# COMMAND ----------

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, "/path/to/delta-table")
deltaTable.restoreToVersion(0)

# COMMAND ----------

delta_table_path = "/user/hive/warehouse/logevents"
df = spark.read.format("delta").option("versionAsOf", 6).load(delta_table_path)
df.show()

# COMMAND ----------

commit(
  actions: Seq[Action],
  op: DeltaOperations.Operation): Long

# COMMAND ----------


