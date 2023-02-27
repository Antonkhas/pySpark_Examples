from pyspark.sql import SparkSession


spark = SparkSession.builder\
    .master('local[*]')\
    .appName('task_3')\
    .getOrCreate()

employee_file = 'import_empl_csv.csv'

df = spark.read.csv(employee_file, sep=';', header=True)

print(df.select('dept_id', 'salary').describe().show())

