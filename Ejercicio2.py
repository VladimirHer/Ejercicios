import os # Usado para ejecutar tareas del Sistema Operativo
import pyspark
from pyspark.sql import SparkSession # Importación de la librería para crear una sesión en Spark
from pyspark.sql.functions import trim # Función que elimina los espacios al inicio y al final de una cadena de texto
import pandas as pd

sc = pyspark.SparkContext('local[*]') # Conexión al Spark local
spark = SparkSession.builder.appName('Ejercicio2').getOrCreate() # Creación de la sesión Spark con nombre Ejercicio1

##### Carga de datos
df = spark.read.load('demo.csv', format='csv', inferSchema=True, header=True) # Lectura del CSV a un DataFrame de Spark
df = df.na.fill('NA') # Reemplaza los datos faltantes (null) por la palabra NA en todo el dataframe
df = df.na.replace('\\N', 'NA') # Reemplaza el valor '\\N' por la palabra NA en todo el dataframe
df = df.withColumn('country', trim(df.country)) # Elimina los espacios al inicio y al final de los valores en la columna 'country'
df.createOrReplaceTempView('airports') # Creación de un Temporal View en Spark, basado en el dataframe creado anteriormente

df.describe() # Descripción del dataframe

##### Creación de diccionario a partir de la columna 'country'
countries = spark.sql('SELECT DISTINCT country FROM airports ORDER BY 1') # Se seleccionan los distintos valores de 'country' y se orden por la misma
countries = countries.toPandas().to_dict()['country']
# Se convierte el dataframe de formato Spark a formato Pandas, posteriormente se convierte en un diccionario y se seleccionan los valores en el resultado

##### Generar un CSV a partir del diccionario
df_countries = pd.DataFrame.from_dict(countries, orient='index', columns=['country']).reset_index() # Se genera un nuevo dataframe de Pandas a partir del diccionario, incluyendo el índice
df_countries = spark.createDataFrame(df_countries) # Se convierte el dataframe de Pandas a Spark
df_countries.repartition(1).write.option('header', 'true').format('csv').mode('overwrite').save('countries_csv')
# Se escribe en un único archivo incluyendo los nombres de variables a la carpeta 'countries_csv', usando la opción de sobreescritura
os.system('cat countries_csv/part-00000*.csv > countries.csv') # Se copia el contenido del CSV generado a un nuevo CSV en la raíz, renombrandolo

##### Inner Join de ambos CSVs
countries = spark.read.load('countries.csv', format='csv', inferSchema=True, header=True) # Lectura del CSV a un DataFrame de Spark
inner_join = df.join(countries, df.country==countries.country) # Aplicación del Inner Join usando la función join con coincidencia en las columnas 'country' de cada tabla
# Impresión del tamaño de ambos dataframes
print(inner_join.count())
print(df.count())

countries.createOrReplaceTempView('countries') # Creación de un Temporal View en Spark, basado en el dataframe 'countries'
inner_join = spark.sql("SELECT a.*, c.* FROM airports a INNER JOIN countries c on a.country = c.country") # Aplicación del Inner Join mediante SQL usando como coincidencia la columna 'country' de cada tabla
# Impresión del tamaño de ambos dataframes
print(inner_join.count())
print(df.count())
