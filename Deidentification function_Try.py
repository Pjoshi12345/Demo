# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data = [('James','Smith','M',"Sales",30,"192.169.12.2","https//"),('Anna','Rose','F',"Admin",41,"192.169.67.2","https//"),
  ('Robert','Williams','M',"Sales",62,"192.169.34.56","https//"), 
]
columns = ["firstname","lastname","gender","dept","salary","Ipaddress","Url"]
df =spark.createDataFrame(data=data,schema=columns)
df.show()
df.filter(col('firstname').like("%mes%")).show()
# fn=df.select("firstname").collect()
# cnt=0
# for i in fn:
#     fn[cnt]=fn[cnt+1]
    
#     print(i.firstname)
# i=0;
# a, b = fn.index('James'), fn.index('Robert')
# fn[b], fn[a] = fn[a], fn[b]
# fn.show()
# a, b = i.index('password2'), i.index('password1')
#i[b], i[a] = i[a], i[b]

# COMMAND ----------

from itertools import chain
from pyspark.sql import DataFrame


def _sort_transpose_tuple(tup):
    x, y = tup
    return x, tuple(zip(*sorted(y, key=lambda v_k: v_k[1], reverse=False)))[0]


def transpose(X):
    """Transpose a PySpark DataFrame.

    Parameters
    ----------
    X : PySpark ``DataFrame``
        The ``DataFrame`` that should be tranposed.
    """
    # validate
    if not isinstance(X, DataFrame):
        raise TypeError('X should be a DataFrame, not a %s' 
                        % type(X))

    cols = X.columns
    n_features = len(cols)

    # Sorry for this unreadability...
    return X.rdd.flatMap( # make into an RDD
        lambda xs: chain(xs)).zipWithIndex().groupBy( # zip index
        lambda val_idx: val_idx[1] % n_features).sortBy( # group by index % n_features as key
        lambda grp_res: grp_res[0]).map( # sort by index % n_features key
        lambda grp_res: _sort_transpose_tuple(grp_res)).map( # maintain order
        lambda key_col: key_col[1]).toDF() # return to DF

# COMMAND ----------

 from pyspark.sql.functions import spark_partition_id
#df0 = spark.range(3)
df1 = df.withColumn("partition_id_before", spark_partition_id())
df1.show()
df2 = df1.repartition(10).withColumn("partition_id_after", spark_partition_id())
df2.show()

# COMMAND ----------

transpose(df).show()

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
    asd = Url_De_identification_InBase64(text)
    #  binary system 
    print(" asd ",asd )
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

#Reading Common Record Format File from ADLS
#Superset Dataframe 
#file_location = r"/mnt/staging/Common Record Format.txt"
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
#file_location = r"/mnt/staging/Common Record Format.txt"
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
        print(key1)
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

list=[1,2]
for i in list:
    df= genericRuleEngine(dict3,df) #mbr
    df= genericRuleEngine(dict2,df) #clm
    display(df)

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

    #for key in dict1.keys():                     
    test_word ="mbr"
    test_word2 ="clm"
        
    mbr_matching = [s for s in dict1.keys() if test_word in s]
    clm_matching = [s for s in dict1.keys() if test_word2 in s]
        
    print(mbr_matching)
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
        print("inside mbr for")
        value = dict.get(table)
        print(value)
        df = genericRuleEngine(dict2,df)
        
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
# MAGIC --drop table Logevents;
# MAGIC select * from Logevents
# MAGIC --DELETE FROM Logevents where clientname="HealthFirst"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table Logevents

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, ArrayType, IntegerType,DateType,TimestampType
from pyspark.sql.functions import col,lit
import pyspark.sql.functions as f
from pyspark.sql.functions import *
import datetime

def dictionarydata_deidentification2(dict1,df,dict2,disct3):
    columns = ["clientname","tablename","CRF_Columns_list"]
    schema = StructType([
        StructField('clientname', StringType(), True),
        StructField('tablename', StringType(), True),
        StructField('CRF_Columns_list', ArrayType(StringType(), True), True),
        StructField('Timestamp', StringType(), True)])             
    
    sampledata=[("asd","xyz",["ABC","XYZ"],"ts")]
    df9=spark.createDataFrame(data=sampledata,schema=schema)

    for key in dict1.keys(): 
        test_word ="mbr"
        test_word2 ="clm"
        
        mbr_matching = [s for s in dict1.keys() if test_word in s]
        clm_matching = [s for s in dict1.keys() if test_word2 in s]
        
        print(mbr_matching)
        CRF_Columns_list = list(dict3.keys())        
        for table in mbr_matching :
            df = genericRuleEngine(dict3,df)
            
        df2 = df9.withColumn("clientname", lit(dict1.get(key))) \
                                     .withColumn("tablename", lit(key)) \
                                     .withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in CRF_Columns_list])).withColumn("Timestamp", current_timestamp())
 
        df2.write.format("delta").mode("append").saveAsTable("Logevents")   
        
        for table in clm_matching :
            df = genericRuleEngine(dict2,df)                                                                       
            
    CRF_Columns_list = list(dict2.keys())                                   
    df2 = df9.withColumn("clientname", lit(dict1.get(key))) \
                                     .withColumn("tablename", lit(key)) \
                                     .withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in CRF_Columns_list])).withColumn("Timestamp", current_timestamp())                     
            
    df2.write.format("delta").mode("append").saveAsTable("Logevents")           
                      
    
    return df

# COMMAND ----------

columns = ["clientname","tablename"]
schema = StructType([
 StructField('clientname', StringType(), True),
 StructField('tablename', StringType(), True)])

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
emptyRDD = spark.sparkContext.emptyRDD()
#Create empty DataFrame from empty RDD
df1 = spark.createDataFrame(emptyRDD,schema)
df1 = emptyRDD.toDF(schema)
df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql import Window
data=[("asd","xyz")]
df6=spark.createDataFrame(data=data,schema=schema)
thislist = []
for value in dict.values():
    print(value)
    thislist.append(value)
    
df7=spark.createDataFrame(thislist, StringType())
df7.show()
# df6 = df6.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
# df7 = df7.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))

# final_df = df6.join(df7, df6.row_idx == df7.row_idx).\
#              drop("row_idx")
# final_df.show()

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StructType,StructField, StringType, ArrayType, IntegerType
from array import array

columns = ["clientname","tablename","CRF_Columns_list"]
schema = StructType([
        StructField('clientname', StringType(), True),
        StructField('tablename', StringType(), True),
        StructField("CRF_Columns_list", ArrayType(IntegerType(), True), True)])
        #StructField('CRF_Columns_list', StringType(), True)])
    
sampledata=[("asd","xyz",[1,2])]
df10=spark.createDataFrame(data=sampledata,schema=schema)
list = [1,2,3,4]
final_df = df10.withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in list])).show()

final_df.show()

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

#     from pyspark.sql import SparkSession
#     from pyspark.sql.functions import col,lit
#     spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#     emptyRDD = spark.sparkContext.emptyRDD()
#     #Create empty DataFrame from empty RDD
#     df9 = spark.createDataFrame(sampledata,schema)
#     #df9 = emptyRDD.toDF(schema)
#     df9.printSchema()
#    display(df9)
    for key in dict1.keys(): 
        
            
        if key == 'mbr_History':
            #CRF_Columns_list=[]
            CRF_Columns_list = list(dict3.keys())
            print(list(dict3.keys()))
            
            for key1 in dict3.keys():
                if dict3[key1] == 'udf_Url_Deidentification':
#                     df2 = df9.withColumn("clientname", lit(dict1.get(key))) \
#                              .withColumn("tablename", lit(key)) \
#                              .withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in CRF_Columns_list]))
                             
                                                        
                    #df2.write.format("delta").mode("append").saveAsTable("Logevents")
                    df= df.withColumn("Hashed_url"+key1,
                      udf_IpAddr_Deidentification(col(key1)))
                           
                        
                elif dict3[key1] == 'udf_EmailAddress_Deidentification_InBinary':
                    df = df.withColumn("Hashed_Email"+key1,udf_EmailAddress_Deidentification_InBinary(col(key1)))
            
            #print(list(dict2.keys()))  
            #print(list(dict3.keys()))
            
            #print(CRF_Columns_list) 
            
            df2 = df9.withColumn("clientname", lit(dict1.get(key))) \
                                     .withColumn("tablename", lit(key)) \
                                     .withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in CRF_Columns_list])).withColumn("Timestamp", current_timestamp())
 
            df2.write.format("delta").mode("append").saveAsTable("Logevents")
  
        elif key == 'clm_History': 
            CRF_Columns_list = list(dict2.keys())
            print(list(dict2.keys()))
            
            for key1 in dict2.keys():
                if dict2[key1] == 'udf_dept_nullification_desc':
#                     df2 = df9.withColumn("clientname", lit(dict1.get(key))) \
#                              .withColumn("tablename", lit(key)) \
#                              .withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in CRF_Columns_list]))
                    
                    df= df.withColumn("Hashed_url"+key1,
                      udf_dept_nullification_desc(col(key1)))
                    
                            
                        
                elif dict2[key1] == 'udf_SIN_Deidentification_InHmac':
#                     df2 = df9.withColumn("clientname", lit(dict1.get(key))) \
#                              .withColumn("tablename", lit(key)) \
#                              .withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in CRF_Columns_list]))
                    
                    df = df.withColumn("Hashed_Email"+key1, udf_dept_nullification_desc(col(key1))) 
                    
                            
                df2 = df9.withColumn("clientname", lit(dict1.get(key))) \
                                     .withColumn("tablename", lit(key)) \
                                     .withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in CRF_Columns_list])).withColumn("Timestamp", current_timestamp())                     
            #df2 = df9.withColumn("CRF_Columns_list",  f.array([f.lit(x) for x in CRF_Columns_list]))                        
    df2.write.format("delta").mode("append").saveAsTable("Logevents")           
                      
    
    return df
