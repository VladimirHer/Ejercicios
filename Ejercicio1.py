import pyspark
from pyspark.sql import SparkSession # Importación de la librería para crear una sesión en Spark

sc = pyspark.SparkContext('local[*]') # Conexión al Spark local
spark = SparkSession.builder.appName('Ejercicio1').getOrCreate() # Creación de la sesión Spark con nombre Ejercicio1

##### Carga de datos
df = spark.read.load('all_data.csv', format='csv', inferSchema=True, header=True) # Lectura del CSV a un DataFrame de Spark
df.createOrReplaceTempView('quien') # Creación de un Temporal View en Spark, basado en el dataframe creado anteriormente

df.describe() # Descripción del dataframe

##### Conteo de registros
query = spark.sql('SELECT COUNT(*) as Count FROM quien') # Conteo de registros usando SQL
print(query.select('Count').first()[0]) # Impresión del conteo de registros
print(df.count()) # Conteo de registros mediante la función count del dataframe

##### Conteo de categorias
query = spark.sql('SELECT COUNT(DISTINCT categoria) as Count FROM quien') # Mediante SQL se hace un conteo de los distintos valores de la columna "categoria"
print(query.select('Count').first()[0]) # Impresión del conteo
fquery = df.select('categoria').distinct().count() # Obtención del mismo resultado mediante funciones del dataframe
print(fquery)

##### Conteo de Cadenas Comerciales
query = spark.sql('SELECT COUNT(DISTINCT cadenaComercial) as Count FROM quien') # Mediante SQL se hace un conteo de los distintos valores de la columna "cadenaComercial"
print(query.select('Count').first()[0]) # Impresión del conteo
fquery = df.select('cadenaComercial').distinct().count() # Obtención del mismo resultado mediante funciones del dataframe
print(fquery)

##### Productos más monitoreados en cada estado de la república
query = spark.sql("""SELECT estado, producto, conteo
                     FROM(SELECT estado, producto, conteo, dense_rank() 
                         OVER (PARTITION BY estado ORDER BY conteo DESC) as rank
                         FROM (SELECT estado, producto, COUNT(producto) as conteo
                             FROM quien GROUP BY estado, producto) t1) t2
                     WHERE rank <= 3""")
"""
	Se crea una consulta la cual general el conteo veces en las que aparece un producto en cada 'estado'.
	Posterior a ello se crea una nueva consulta indicando su "Rango de Densidad" (su posición con respecto al grupo) con respecto a cada 'estado' en orden descendente.
	Al final se seleccionan todos los resultados que tengan un rango menor o igual a 3, indicando que sólo el top 3 de productos más monitoreados sea impreso.
"""
query.show() # Se muestran los datos

###### Cadena comercial con mayor variedad de productos monitoreados
query = spark.sql("""SELECT cadenaComercial, COUNT(DISTINCT producto) as conteo
                  FROM quien GROUP BY cadenaComercial ORDER BY conteo DESC LIMIT 1""")
"""
	La consulta selecciona para cada grupo 'cadenaComercial' el conteo de los distintos productos disponibles, los ordena de forma descendente y
	limita los resultados a solo el primer registro.
"""
query.show() # Se muestran los datos
