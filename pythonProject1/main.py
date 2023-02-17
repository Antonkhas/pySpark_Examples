from pyspark.sql import SparkSession


spark = SparkSession.builder\
    .master('local[*]')\
    .appName('PySpark_task_1')\
    .getOrCreate()

csv_file = 'import_empl_csv.csv'
df = spark.read.csv(csv_file,
                    sep=';',
                    header=True
                    )


print(df.show())
