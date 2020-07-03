import json
from math import cos, pi, sqrt
import pandas as pd

def isInside(pLat, pLng, cLat, cLng):
    ky = 40000 / 360
    kx = cos(pi*cLat / 180.0) * ky
    dx = abs(cLng - pLng) * kx
    dy = abs(cLat - pLat) * ky
    return sqrt(dx**2 + dy**2) <= 1

centerPoint = [33.8121, -117.9190]

coordinates = []
with open('/data/tweets.json', errors='ignore') as json_file:
    for line in json_file:
        try:
            coordinates.append(json.loads(line)['geo']['coordinates'])
        except:
            continue

coordinates = pd.DataFrame(coordinates, columns=['lat', 'lng'])
coordinates = coordinates[coordinates.apply(lambda row: isInside(row['lat'], row['lng'], centerPoint[0], centerPoint[1]), axis=1)]

count = len(coordinates)
