#!/usr/bin/env python

#
# Run with: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 StructuredStreamingAndElastic.py
#

import datetime, iso8601
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pymongo
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.types import StructType, StructField
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoderEstimator, StringIndexerModel
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

APP_NAME = "SparkStreaming.py"
PREDICTION_TOPIC = "IDSPRUEBA"
PERIOD = 10
BROKERS = 'localhost:9092'
base_path = "."

spark = SparkSession.builder.config("spark.default.parallelism", 1).appName(APP_NAME).getOrCreate()

lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKERS).option("subscribe", PREDICTION_TOPIC).load()

schema = T.StructType([
    T.StructField( 'srcip', StringType(), True),
    T.StructField('sport', StringType(), True),
    T.StructField('dstip', StringType(), True),
    T.StructField('dsport',StringType(), True),
    T.StructField('proto',StringType() , True),
    T.StructField('state', StringType(), True),
    T.StructField('dur', StringType(), True),
    T.StructField('sbytes', StringType(), True),
    T.StructField('dbytes', StringType(), True),
    T.StructField('sttl', StringType(), True),
    T.StructField('dttl', StringType(), True),
    T.StructField('sloss', StringType(), True),
    T.StructField('dloss', StringType(), True),
    T.StructField('service', StringType(), True),
    T.StructField('Sload', StringType(), True),
    T.StructField('Dload', StringType(), True),
    T.StructField('Spkts',StringType(), True),
    T.StructField('Dpkts',StringType(), True),
    T.StructField('swin', StringType(), True),
    T.StructField('dwin', StringType(), True),
    T.StructField('stcpb', StringType(), True),
    T.StructField('dtcpb', StringType(), True),
    T.StructField('smeansz', StringType(), True),
    T.StructField('dmeansz', StringType(), True),
    T.StructField('trans_depth',StringType(), True),
    T.StructField('res_bdy_len',StringType(), True),
    T.StructField('Sjit',StringType(), True),
    T.StructField('Djit', StringType(), True),
    T.StructField('Stime',StringType(), True),
    T.StructField('Ltime', StringType(), True),
    T.StructField('Sintpkt',StringType(), True),
    T.StructField('Dintpkt',StringType(), True),
    T.StructField('tcprtt',StringType(), True),
    T.StructField('synack',StringType(), True),
    T.StructField('ackdat',StringType(), True),
    T.StructField('is_sm_ips_ports',StringType(), True),
    T.StructField('ct_state_ttl',StringType(), True),
    T.StructField('ct_flw_http_mthd',StringType(), True),
    T.StructField('is_ftp_login',StringType(), True),
    T.StructField('ct_ftp_cm',StringType(), True),
    T.StructField('ct_srv_src',StringType(), True),
    T.StructField('ct_srv_dst',StringType(), True),
    T.StructField('ct_dst_ltm',StringType(), True),
    T.StructField('ct_src_ ltm',StringType(), True),
    T.StructField('ct_src_dport_ltm',StringType(), True),
    T.StructField('ct_dst_sport_ltm',StringType(), True),
    T.StructField('ct_dst_src_ltm',StringType(), True),
    T.StructField('attack_cat',StringType(), True),
    T.StructField('label',StringType(), True),
    T.StructField("UUID", StringType(), True)
    ])

dataset =  lines.select(F.from_json(F.col("value").cast("string"),schema).alias("dataset")).select("dataset.*")

dataset.printSchema()
print(type(dataset))

string_indexer_models = {}
for column in ["srcip", "sport","dstip","dsport","proto", "state","dur","sbytes","dbytes","sttl","dttl","sloss","dloss","service","Sload","Dload","Spkts","Dpkts","swin","dwin","stcpb","dtcpb","smeansz","dmeansz","trans_depth","res_bdy_len","Sjit","Djit", "Stime","Ltime","Sintpkt","Dintpkt","tcprtt","synack","ackdat","is_sm_ips_ports","ct_state_ttl","ct_flw_http_mthd","is_ftp_login","ct_ftp_cm","ct_srv_src","ct_srv_dst","ct_dst_ltm","ct_src_ ltm", "ct_src_dport_ltm","ct_dst_sport_ltm", "ct_dst_src_ltm", "label", "attack_cat"]:
        string_indexer_model_path = "{}/data/string_indexer/string_indexer_model_{}.bin".format(
                  base_path,
                  column
                )
        string_indexer = StringIndexerModel.load(string_indexer_model_path)
        string_indexer_models[column] = string_indexer


for column in ["srcip", "sport","dstip","dsport","proto", "state","dur","sbytes","dbytes","sttl","dttl","sloss","dloss","service","Sload","Dload","Spkts","Dpkts","swin","dwin","stcpb","dtcpb","smeansz","dmeansz","trans_depth","res_bdy_len","Sjit","Djit", "Stime","Ltime","Sintpkt","Dintpkt","tcprtt","synack","ackdat","is_sm_ips_ports","ct_state_ttl","ct_flw_http_mthd","is_ftp_login","ct_ftp_cm","ct_srv_src","ct_srv_dst","ct_dst_ltm","ct_src_ ltm", "ct_src_dport_ltm","ct_dst_sport_ltm", "ct_dst_src_ltm", "label", "attack_cat"]:
    string_indexer_model = string_indexer_models[column]
    dataset = string_indexer_model.transform(dataset)

vector_assembler_path = "{}/data/numeric_vector_assembler.bin".format(base_path)
vector_assembler = VectorAssembler.load(vector_assembler_path)

finalDataSet= vector_assembler.transform(dataset)

model_path = "{}/data/RedNeuronal.bin".format(
      base_path
  )

model = MultilayerPerceptronClassificationModel.load(model_path)

predictions = model.transform(finalDataSet)

predictions = predictions.withColumn("prediction", predictions.prediction.cast("string"))

predictions = predictions.na.replace(["0.0", "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0"],["Generic", "Explots","Fuzzers","DoS", "Recconnaisance","Analysis", "Backdoors","Shellcode","Worms","No ataque"],"prediction")

only_predictions = predictions.select( "prediction", "attack_cat","srcip", "sport","dstip","dsport","proto","ct_flw_http_mthd")
only_predictions = only_predictions.na.fill("No")

query = only_predictions.writeStream.outputMode("append").format("org.elasticsearch.spark.sql").option("checkpointLocation", "path-to-checkpointing").start("index-name-2/doc-type")

query.awaitTermination()
