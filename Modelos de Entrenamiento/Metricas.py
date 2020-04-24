
# Evaluacion del modelo.
predictions = model.transform(test_data)

#Primeros las métricas básicas

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#Accuracy es equivalente a wieghtedRecall
evaluator = MulticlassClassificationEvaluator(
  labelCol="attack_cat_index_", metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)
print("Accuracy = {}".format(accuracy))

evaluator = MulticlassClassificationEvaluator(
  labelCol="attack_cat_index_", metricName="weightedPrecision"
)
weightedPrecision = evaluator.evaluate(predictions)
print("weightedPrecision = {}".format(weightedPrecision))

evaluator = MulticlassClassificationEvaluator(
  labelCol="attack_cat_index_", metricName="f1"
)
f1 = evaluator.evaluate(predictions)
print("f1 = {}".format(f1))

predictionsAndLabels = predictions.select("prediction", "attack_cat_index_").rdd

from pyspark.mllib.evaluation import MulticlassMetrics

metrics = MulticlassMetrics(predictionsAndLabels)

weightedFalsePositiveRate = metrics.
print("weightedFalsePositiveRate = {}".format(weightedFalsePositiveRate))


#Matriz de Confusion
matrizDeConfusion = metrics.confusionMatrix().toArray()
print(matrizDeConfusion)

#Metricas por etiqueta
falsePositiveRate = metrics.falsePositiveRate(0.0)
print("falsePositiveRate Generic")
print(falsePositiveRate)

precision = metrics.precision(0.0)
print("precision Generic")
print(precision)

recall = metrics.recall(0.0)
print("recall Generic ")
print(recall)

falsePositiveRate = metrics.falsePositiveRate(1.0)
print("falsePositiveRate Exploits")
print(falsePositiveRate)

precision = metrics.precision(1.0)
print("precision Exploits")
print(precision)

recall = metrics.recall(1.0)
print("recall Exploits")
print(recall)

falsePositiveRate = metrics.falsePositiveRate(2.0)
print("falsePositiveRate Fuzzers")
print(falsePositiveRate)

precision = metrics.precision(2.0)
print("precision Fuzzers")
print(precision)

recall = metrics.recall(2.0)
print("recall Fuzzers")
print(recall)

falsePositiveRate = metrics.falsePositiveRate(3.0)
print("falsePositiveRate DoS")
print(falsePositiveRate)

precision = metrics.precision(3.0)
print("precision DoS")
print(precision)

recall = metrics.recall(3.0)
print("recall DoS")
print(recall)

falsePositiveRate = metrics.falsePositiveRate(4.0)
print("falsePositiveRate Reconnaisance")
print(falsePositiveRate)

precision = metrics.precision(4.0)
print("precision Reconnaisance")
print(precision)

recall = metrics.recall(4.0)
print("recall Reconnaisance")
print(recall)

falsePositiveRate = metrics.falsePositiveRate(5.0)
print("falsePositiveRate Analysis")
print(falsePositiveRate)

precision = metrics.precision(5.0)
print("precision Analysis")
print(precision)

recall = metrics.recall(5.0)
print("recall Analysis")
print(recall)

falsePositiveRate = metrics.falsePositiveRate(6.0)
print("falsePositiveRate Backdoors")
print(falsePositiveRate)

precision = metrics.precision(6.0)
print("precision Backdoors")
print(precision)

recall = metrics.recall(6.0)
print("recall Backdoors")
print(recall)

falsePositiveRate = metrics.falsePositiveRate(7.0)
print("falsePositiveRate Shellcode")
print(falsePositiveRate)

precision = metrics.precision(7.0)
print("precision Shellcode")
print(precision)

recall = metrics.recall(7.0)
print("recall Shellcode")
print(recall)

falsePositiveRate = metrics.falsePositiveRate(8.0)
print("falsePositiveRate Worms")
print(falsePositiveRate)

precision = metrics.precision(8.0)
print("precision Worms")
print(precision)

recall = metrics.recall(8.0)
print("recall Worms")
print(recall)

falsePositiveRate = metrics.falsePositiveRate(9.0)
print("falsePositiveRate No attack")
print(falsePositiveRate)

precision = metrics.precision(9.0)
print("precision No attack")
print(precision)

recall = metrics.recall(9.0)
print("recall No attack")
print(recall)
