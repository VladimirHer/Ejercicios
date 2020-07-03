import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim
import pandas as pd
sc = pyspark.SparkContext('local[*]')
spark = SparkSession.builder.appName('Ejercicio2').getOrCreate()

from pyspark.sql.functions import trim
df = spark.read.load('/data/demo.csv', format='csv', inferSchema=True, header=True)
df = df.na.fill('NA')
df = df.na.replace('\\N', 'NA')
df = df.withColumn('country', trim(df.country))
df.createOrReplaceTempView('airports')

df.describe()

countries = spark.sql('SELECT DISTINCT country FROM airports ORDER BY 1')
countries = countries.toPandas().to_dict()['country']

df_countries = pd.DataFrame.from_dict(countries, orient='index', columns=['country']).reset_index()
df_countries = spark.createDataFrame(df_countries)
df_countries.repartition(1).write.option('header', 'true').format('csv').mode('overwrite').save('/data/countries_csv')
os.system('cat /data/countries_csv/part-00000*.csv > /data/countries.csv')

countries = spark.read.load('/data/countries.csv', format='csv', inferSchema=True, header=True)
inner_join = df.join(countries, df.country==countries.country)
print(inner_join.count())
print(df.count())

countries.createOrReplaceTempView('countries')
inner_join = spark.sql("SELECT a.*, c.* FROM airports a INNER JOIN countries c on a.country = c.country")
print(inner_join.count())
print(df.count())
