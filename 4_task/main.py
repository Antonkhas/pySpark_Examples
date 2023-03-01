from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master('local[*]')\
    .appName('task_4')\
    .getOrCreate()

employee_file = 'import_empl_csv.csv'

df = spark.read.csv(employee_file, sep=';', header=True)

train_df, test_df = df.randomSplit([0.8, 0.2])


print(train_df.show())
print(test_df.show())