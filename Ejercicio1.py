import pyspark
from pyspark.sql import SparkSession
sc = pyspark.SparkContext('local[*]')
spark = SparkSession.builder.appName('Ejercicio1').getOrCreate()

df = spark.read.load('/data/sample.csv', format='csv', inferSchema=True, header=True)
df.createOrReplaceTempView('sample')

df.describe()

query = spark.sql('SELECT COUNT(*) as Count FROM sample')
print(query.select('Count').first()[0])
print(df.count())

query = spark.sql('SELECT COUNT(DISTINCT categoria) as Count FROM sample')
print(query.select('Count').first()[0])
fquery = df.select('categoria').distinct().count()
print(fquery)

query = spark.sql('SELECT COUNT(DISTINCT cadenaComercial) as Count FROM sample')
print(query.select('Count').first()[0])
fquery = df.select('cadenaComercial').distinct().count()
print(fquery)

query = spark.sql("""SELECT estado, producto, conteo
                     FROM(SELECT estado, producto, conteo, dense_rank() 
                         OVER (PARTITION BY estado ORDER BY conteo DESC) as rank
                         FROM (SELECT estado, producto, COUNT(producto) as conteo
                             FROM sample GROUP BY estado, producto) t1) t2
                     WHERE rank <= 3""")
query.show()

query = spark.sql("""SELECT cadenaComercial, COUNT(DISTINCT producto) as conteo
                  FROM sample GROUP BY cadenaComercial ORDER BY conteo DESC LIMIT 1""")
query.show()
