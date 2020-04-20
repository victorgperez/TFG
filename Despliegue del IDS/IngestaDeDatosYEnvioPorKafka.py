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

from kafka import KafkaProducer, TopicPartition
import uuid

#En el sample elegir el porcentaje de datos del dataset que se quiere enviar.
envio = dataSet.sample(False, 0.0001, seed=0)
print(envio.count())
prediction_features = {}
prediction_features = envio
print(type(prediction_features))

kafka_topic = 'IDSPRUEBA'
if type(kafka_topic) == bytes:
    kafka_topic = kafka_topic.decode('utf-8')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10))
PREDICTION_TOPIC = kafka_topic

for row in prediction_features.toJSON().collect():
    print((row))
    producer.send(PREDICTION_TOPIC , row.encode())
    producer.flush()
