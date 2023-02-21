from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder\
    .master('local[*]')\
    .appName('PySpark_task_2')\
    .getOrCreate()


employee_file = 'import_empl_csv.csv'
dept_file = 'import_empl_id.csv'

df = spark.read.csv(employee_file, sep=';', header=True)
df1 = spark.read.csv(dept_file, sep=';', header=True)

df_joined = df.join(df1, on="dept_id")

print(df_joined.show())



