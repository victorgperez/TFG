import sys, os, re
import json
import codecs
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

sc = SparkContext('local')
spark = SparkSession(sc)

schema = StructType([
    StructField( 'srcip', StringType(), True),
    StructField('sport', StringType(), True),
    StructField('dstip', StringType(), True),
    StructField('dsport',StringType(), True),
    StructField('proto',StringType() , True),
    StructField('state', StringType(), True),
    StructField('dur', StringType(), True),
    StructField('sbytes', StringType(), True),
    StructField('dbytes', StringType(), True),
    StructField('sttl', StringType(), True),
    StructField('dttl', StringType(), True),
    StructField('sloss', StringType(), True),
    StructField('dloss', StringType(), True),
    StructField('service', StringType(), True),
    StructField('Sload', StringType(), True),
    StructField('Dload', StringType(), True),
    StructField('Spkts',StringType(), True),
    StructField('Dpkts',StringType(), True),
    StructField('swin', StringType(), True),
    StructField('dwin', StringType(), True),
    StructField('stcpb', StringType(), True),
    StructField('dtcpb', StringType(), True),
    StructField('smeansz', StringType(), True),
    StructField('dmeansz', StringType(), True),
    StructField('trans_depth',StringType(), True),
    StructField('res_bdy_len',StringType(), True),
    StructField('Sjit',StringType(), True),
    StructField('Djit', StringType(), True),
    StructField('Stime',StringType(), True),
    StructField('Ltime', StringType(), True),
    StructField('Sintpkt',StringType(), True),
    StructField('Dintpkt',StringType(), True),
    StructField('tcprtt',StringType(), True),
    StructField('synack',StringType(), True),
    StructField('ackdat',StringType(), True),
    StructField('is_sm_ips_ports',StringType(), True),
    StructField('ct_state_ttl',StringType(), True),
    StructField('ct_flw_http_mthd',StringType(), True),
    StructField('is_ftp_login',StringType(), True),
    StructField('ct_ftp_cm',StringType(), True),
    StructField('ct_srv_src',StringType(), True),
    StructField('ct_srv_dst',StringType(), True),
    StructField('ct_dst_ltm',StringType(), True),
    StructField('ct_src_ ltm',StringType(), True),
    StructField('ct_src_dport_ltm',StringType(), True),
    StructField('ct_dst_sport_ltm',StringType(), True),
    StructField('ct_dst_src_ltm',StringType(), True),
    StructField('attack_cat',StringType(), True),
    StructField('label',StringType(), True)])

dataSet = spark.read.format("csv").option("header", "true").schema(schema).option("mode", "DROPMALFORMED").load("UNSW.csv")

dataSet = dataSet.sample(False, 0.65, seed=0)
#dataSet.count()
dataSet = dataSet.na.replace([" Fuzzers "," Shellcode ","Backdoor"," Reconnaissance "],[" Fuzzers","Shellcode","Backdoors","Reconnaissance"],"attack_cat")
#dataSet.select("attack_cat").distinct().show()

from pyspark.ml.feature import StringIndexer, VectorAssembler

for column in ["srcip", "sport","dstip","dsport","proto", "state","dur","sbytes","dbytes","sttl","dttl","sloss","dloss","service","Sload","Dload","Spkts","Dpkts","swin","dwin","stcpb","dtcpb","smeansz","dmeansz","trans_depth","res_bdy_len","Sjit","Djit", "Stime","Ltime","Sintpkt","Dintpkt","tcprtt","synack","ackdat","is_sm_ips_ports","ct_state_ttl","ct_flw_http_mthd","is_ftp_login","ct_ftp_cm","ct_srv_src","ct_srv_dst","ct_dst_ltm","ct_src_ ltm", "ct_src_dport_ltm","ct_dst_sport_ltm", "ct_dst_src_ltm", "label", "attack_cat"]:
    string_indexer = StringIndexer(
        inputCol=column,
        outputCol=column + "_index",
        handleInvalid="keep"

        )
    String_Indexer_Model= string_indexer.fit(dataSet)
    dataSet = String_Indexer_Model.transform(dataSet)
    base_path = "."
    string_indexer_output_path = "{}/data/string_indexer_rft/string_indexer_model_{}.bin".format(
    base_path,
    column
        )
    String_Indexer_Model.write().overwrite().save(string_indexer_output_path)

index_columns = ["srcip_index", "sport_index","dstip_index","dsport_index","proto_index", "state_index","dur_index","sbytes_index","dbytes_index","sttl_index","dttl_index","sloss_index","dloss_index",
                   "service_index","Sload_index","Dload_index","Spkts_index","Dpkts_index", "swin_index","dwin_index","stcpb_index","dtcpb_index","smeansz_index","dmeansz_index","trans_depth_index","res_bdy_len_index","Sjit_index","Djit_index", "Stime_index","Ltime_index","Sintpkt_index","Dintpkt_index","tcprtt_index","synack_index","ackdat_index","is_sm_ips_ports_index","ct_state_ttl_index","ct_flw_http_mthd_index","is_ftp_login_index","ct_ftp_cm_index","ct_srv_src_index","ct_srv_dst_index","ct_dst_ltm_index","ct_src_ ltm_index", "ct_src_dport_ltm_index","ct_dst_sport_ltm_index", "ct_dst_src_ltm_index"]

vector_assembler = VectorAssembler(
  inputCols= index_columns,
  outputCol="Features_vec"
)
finalDataSet = vector_assembler.transform(dataSet)
vector_assembler_path = "{}/data/numeric_vector_assembler_rft.bin".format(base_path)
vector_assembler.write().overwrite().save(vector_assembler_path)

for column in index_columns:
  finalDataSet = finalDataSet.drop(column)

training_data, test_data = finalDataSet.randomSplit([0.7, 0.3])

from pyspark.ml.classification import RandomForestClassifier
rfc = RandomForestClassifier(
featuresCol="Features_vec", labelCol="attack_cat_index",
  maxBins=277906
)
model = rfc.fit(training_data)
model_output_path = "{}/data/RandomForestTree.bin".format( base_path)
model.write().overwrite().save(model_output_path)

predictions = model.transform(test_data)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(
  labelCol="attack_cat_index", metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)
print("Accuracy = {}".format(accuracy))


evaluator = MulticlassClassificationEvaluator(
  labelCol="attack_cat_index", metricName="weightedPrecision"
)
weightedPrecision = evaluator.evaluate(predictions)
print("weightedPrecision = {}".format(weightedPrecision))

evaluator = MulticlassClassificationEvaluator(
  labelCol="attack_cat_index", metricName="f1"
)
f1 = evaluator.evaluate(predictions)
print("f1 = {}".format(f1))
