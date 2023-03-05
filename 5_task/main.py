from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


spark = SparkSession.builder\
    .appName('Task-5')\
    .master('local[*]')\
    .getOrCreate()

employee_file = 'example.csv'

df = spark.read.csv(employee_file, sep=';', header=True)

print(f"Количество записей: {df.count()}")


print("Статистические характеристики: ")
print(df.describe().show())


print('Распределение целевой переменной')
print(df.groupBy('1width').count().show())

# Корреляция признаков
assembler = VectorAssembler(inputCols=df.columns[:-1], outputCol='my_Age')
assembled_data = assembler.transform(df).select('kg')

pearson_corr = Correlation.corr(assembled_data, 'kg', 'pearson').collect()[0][0]
print(f"Корреляция Пирсона:\n{pearson_corr.toArray()}")

spearman_corr = Correlation.corr(assembled_data, 'kg', 'spearman').collect()[0][0]
print(f"Корреляция Спирмена:\n{spearman_corr.toArray()}")


# assembler = VectorAssembler(inputCols=['Name', 'Team', 'Age'], outputCol='features')
# assembled_data = assembler.transform(df).select('features', 'target')

