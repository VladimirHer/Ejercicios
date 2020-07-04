import json
from math import sin, cos, atan2, pi, sqrt # Fórmulas matemáticas para Coseno, valor de PI y cálculo de Raíz Cuadrada
import pandas as pd
from flask import Flask, jsonify # Librerías necesarias para convertir el proceso en una API

app = Flask(__name__) # Definición de la app web

def isInside(pLat, pLng, cLat, cLng): # Definición de la función para el cálculo de la distancia
	rad = lambda x: x*pi/180 # Función anónima para convertir de grados a radianes
	r = 6378.137 # Diametro de la tierra
	dLat = rad(pLat-cLat) # Cálculo de la distancia entre las latitudes, convertido a radianes
	dLng = rad(pLng-cLng) # Cálculo de la distancia entre las longitudes, convertido a radianes

	# Aplicación de la fórmula del semiverseno para el cálculo de la distancia
	a = sin(dLat/2) * sin(dLat/2) + cos(rad(cLat)) * cos(rad(pLat)) * sin(dLng/2) * sin(dLng/2)
	c = 2 * atan2(sqrt(a), sqrt(1-a))
	d = r * c
	return d <= 1 # Comprobación de si la distancia es menor a 1km

def calc(cLat, cLng): # Definición de la función de conteo
	coordinates = [] # Lista donde se almacenarán las coordenadas
	with open('/home/vlad/OPI_data/tweets.json', errors='ignore') as json_file: # Apertura del archivo json, ignorando errores por caracteres especiales
		for line in json_file: # Lee cada línea, ya que cada línea es un registro/tweet
			try:
				coordinates.append(json.loads(line)['geo']['coordinates']) # Selecciona únicamente las coordenadas para evitar el uso excesivo de recursos
			except: # Se hace un proceso try-except para evitar los errores generados con los tweets que no tienen coordenadas
				continue

	coordinates = pd.DataFrame(coordinates, columns=['lat', 'lng']) # Convierte la lista en un dataframe
	# Aplica la función de distancia y lo aplica como un filtro sobre el dataframe
	coordinates = coordinates[coordinates.apply(lambda row: isInside(row['lat'], row['lng'], cLat, cLng), axis=1)]

	return len(coordinates) # Cuenta el número de registros

@app.route('/tweets/api/v1.0/countnear/<cLat>/<cLng>', methods=['GET']) # Inicia la apertura a peticiones, incluyendo dos strings como parámetro
def get_countnear(cLat, cLng): # Función que se ejecuta al llamar a la API
	cLat, cLng = float(cLat), float(cLng) # Convertir el string a float
	return jsonify({'countnear': calc(cLat, cLng)}) # Calcular el conteo y generar un JSON como respuesta

if __name__ == '__main__':
	app.run(debug=True)