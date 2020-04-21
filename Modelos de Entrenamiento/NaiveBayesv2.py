
import findspark
findspark.init()

import sys, os, re
import json
import codecs


import pyspark
import sys
import decimal
import time
import datetime
import calendar
import json
import re
import base64
from array import array

if sys.version >= "3":
    long = int
    basestring = unicode = str

from py4j.protocol import register_input_converter
from py4j.java_gateway import JavaClass

from pyspark.serializers import CloudPickleSerializer
from pyspark.ml.regression import LinearRegression

import pyspark.sql.types
#from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import six
from types import *
import pandas as pd
import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.functions import col,sum


from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

import findspark
findspark.init()
sc = SparkContext('local')
spark = SparkSession(sc)


#Se define el esquema que va a tener el data frame
schema = StructType([
    StructField( 'srcip', StringType(), True),
    StructField('sport', IntegerType(), True),
    StructField('dstip', StringType(), True),
    StructField('dsport',IntegerType(), True),
    StructField('proto',StringType() , True),
    StructField('state', StringType(), True),
    StructField('dur', IntegerType(), True),
    StructField('sbytes', IntegerType(), True),
    StructField('dbytes', IntegerType(), True),
    StructField('sttl', IntegerType(), True),
    StructField('dttl', IntegerType(), True),
    StructField('sloss', IntegerType(), True),
    StructField('dloss', IntegerType(), True),
    StructField('service', StringType(), True),
    StructField('Sload', IntegerType(), True),
    StructField('Dload', IntegerType(), True),
    StructField('Spkts',IntegerType(), True),
    StructField('Dpkts',IntegerType(), True),
    StructField('swin', IntegerType(), True),
    StructField('dwin', IntegerType(), True),
    StructField('stcpb', IntegerType(), True),
    StructField('dtcpb', IntegerType(), True),
    StructField('smeansz', IntegerType(), True),
    StructField('dmeansz', IntegerType(), True),
    StructField('trans_depth',IntegerType(), True),
    StructField('res_bdy_len',IntegerType(), True),
    StructField('Sjit',IntegerType(), True),
    StructField('Djit',IntegerType(), True),
    StructField('Stime',IntegerType(), True),
    StructField('Ltime', IntegerType(), True),
    StructField('Sintpkt',IntegerType(), True),
    StructField('Dintpkt',IntegerType(), True),
    StructField('tcprtt',IntegerType(), True),
    StructField('synack',IntegerType(), True),
    StructField('ackdat',IntegerType(), True),
    StructField('is_sm_ips_ports',IntegerType(), True),
    StructField('ct_state_ttl',IntegerType(), True),
    StructField('ct_flw_http_mthd',IntegerType(), True),
    StructField('is_ftp_login',IntegerType(), True),
    StructField('ct_ftp_cm',IntegerType(), True),
    StructField('ct_srv_src',IntegerType(), True),
    StructField('ct_srv_dst',IntegerType(), True),
    StructField('ct_dst_ltm',IntegerType(), True),
    StructField('ct_src_ ltm',IntegerType(), True),
    StructField('ct_src_dport_ltm',IntegerType(), True),
    StructField('ct_dst_sport_ltm',IntegerType(), True),
    StructField('ct_dst_src_ltm',IntegerType(), True),
    StructField('attack_cat',StringType(), True),
    StructField('label',StringType(), True)])



dataSet = spark.read.format("csv").option("header", "true").schema(schema).option("mode", "DROPMALFORMED").load("UNSW.csv")

print("features iniciales-------------------------------")
print(len(dataSet.columns))
print((dataSet.show(6)))



#Elegir el valor de sample
dataSet = dataSet.sample(False, 0.75, seed=0)

dataSet.count()

dataSet = dataSet.na.replace([" Fuzzers "," Shellcode ","Backdoor"," Reconnaissance "],
                             [" Fuzzers","Shellcode","Backdoors","Reconnaissance"],"attack_cat")
dataSet.select("attack_cat").distinct().show()




from pyspark.ml.feature import StringIndexer

for column in ["srcip","dstip","proto","state","service", "label", "attack_cat"]:
      string_indexer = StringIndexer(
        inputCol=column,
        outputCol=column + "_index_",
        handleInvalid="keep"

      )
      String_Indexer_Model= string_indexer.fit(dataSet)
      dataSet = String_Indexer_Model.transform(dataSet)


      base_path = "."
      string_indexer_output_path = "{}/data/string_indexer_NB/string_indexer_model_NB_{}.bin".format(
      base_path,
      column
      )
      String_Indexer_Model.write().overwrite().save(string_indexer_output_path)

index_columns_to_assembler = ["srcip_index_", "sport","dstip_index_","dsport","proto_index_", "state_index_","dur","sbytes","dbytes","sttl","dttl","sloss","dloss",
               "service_index_","Sload","Dload","Spkts","Dpkts","swin","dwin","stcpb","dtcpb","smeansz","dmeansz","trans_depth",
               "res_bdy_len","Sjit","Djit", "Stime","Ltime","Sintpkt","Dintpkt","tcprtt","synack","ackdat","is_sm_ips_ports",
               "ct_state_ttl","ct_flw_http_mthd","is_ftp_login","ct_ftp_cm","ct_srv_src","ct_srv_dst","ct_dst_ltm",
               "ct_src_ ltm", "ct_src_dport_ltm","ct_dst_sport_ltm", "ct_dst_src_ltm" ]

index_columns = ["srcip", "sport","dstip","dsport","proto", "state","dur","sbytes","dbytes","sttl","dttl","sloss","dloss","service","Sload","Dload","Spkts","Dpkts","swin","dwin","stcpb","dtcpb","smeansz","dmeansz","trans_depth","res_bdy_len","Sjit","Djit", "Stime","Ltime","Sintpkt","Dintpkt","tcprtt","synack","ackdat","is_sm_ips_ports","ct_state_ttl","ct_flw_http_mthd","is_ftp_login","ct_ftp_cm","ct_srv_src","ct_srv_dst","ct_dst_ltm","ct_src_ ltm", "ct_src_dport_ltm","ct_dst_sport_ltm", "ct_dst_src_ltm", "label", "attack_cat","srcip_index_","dstip_index_","proto_index_","state_index_","service_index_", "label_index_"]

vector_assembler = VectorAssembler(
  inputCols= index_columns_to_assembler,
  outputCol="features"
)
finalDataSet = vector_assembler.transform(dataSet)
vector_assembler_path = "{}/data/numeric_vector_assembler_nb.bin".format(base_path)
vector_assembler.write().overwrite().save(vector_assembler_path)

for column in index_columns:
  finalDataSet = finalDataSet.drop(column)

finalDataSet = finalDataSet.withColumnRenamed("attack_cat_index_", "label")


from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


splits = finalDataSet.randomSplit([0.7, 0.3])
train = splits[0]
test = splits[1]

# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

# train the model
model = nb.fit(train)
model_output_path = "{}/data/NaiveBayer.bin".format( base_path)
model.write().overwrite().save(model_output_path)



from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Evaluate model using test data
predictions = model.transform(test)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(
  labelCol="label", metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)
print("Accuracy = {}".format(accuracy))

evaluator = MulticlassClassificationEvaluator(
  labelCol="label", metricName="weightedPrecision"
)
weightedPrecision = evaluator.evaluate(predictions)
print("weightedPrecision = {}".format(weightedPrecision))

evaluator = MulticlassClassificationEvaluator(
  labelCol="label", metricName="f1"
)
f1 = evaluator.evaluate(predictions)
print("f1 = {}".format(f1))
