"""
This module contains the implementation of the endpoint.

"""
#third party imports
from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import jsonschema
from pulp import *
from geopy.distance import geodesic as GD
import re

#local application imports
try:
    import db_connection
except ModuleNotFoundError:
    from app.src import db_connection

try:
    import errorHandling
except ModuleNotFoundError:
    from app.src import errorHandling

app = Flask(__name__) 
CORS(app)

@app.errorhandler(errorHandling.InvalidAPIUsage)
def invalid_api_usage(e):
    return jsonify(e.to_dict()), e.status_code

@app.route('/', methods=['POST'])
def isValid():

    try:
        db = db_connection.DBConnection()
    except IOError:
        return "Database connection not possible", 504, {
            'ContentType': 'text/plain'
        }

    try: 
        input = json.loads(request.data)
    except Exception as e:
        raise errorHandling.InvalidAPIUsage("The input data is not a valid JSON.", exception=e)

    try:
        f = open("schemas/Feature.json", "r")
        schema = json.load(f)
        validator = jsonschema.Draft7Validator(schema, resolver=None, format_checker=None)
        validator.validate(input)
    except jsonschema.exceptions.ValidationError:
        raise errorHandling.InvalidAPIUsage("The input data is not a valid geoJSON.")
   
    if(input["geometry"]["type"] != "MultiPoint"):
        raise errorHandling.InvalidAPIUsage("The geometry is not a 'MultiPoint'")

    if not "p" in input["properties"]:
        raise errorHandling.InvalidAPIUsage("The number of facilites should be informed")

    result = db.isValid(json.dumps(input["geometry"]))

    # Quantidade de pontos passados
    pontos = len(input["geometry"]["coordinates"])
    p = input["properties"]["p"]

    # Criando a matriz de distâncias
    D = []

    for i in input["geometry"]["coordinates"]:
        row = []
        for j in input["geometry"]["coordinates"]:
            row.append(GD(i,j).km)
        D.append(row)

    location = list(range(0, pontos))
    X = LpVariable.dicts('X_%s_%s', (location,location), cat = 'Binary', lowBound = 0, upBound = 1)

    prob = LpProblem('P_Median', LpMinimize)
    prob += sum(sum(D[i][j] * X[i][j] for j in location) for i in location)
    
    # Configurando restrições
    prob += sum(X[i][i] for i in location) == p

    for i in location: 
        prob += sum(X[i][j] for j in location) == 1

    for i in location:
        for j in location: 
            prob += X[i][j] <= X[j][j]

    prob.solve()

    points = []
    lineStrings = []

    for v in prob.variables():
        sentence = v.name
        s = [int(s) for s in re.findall(r'-?\d+\.?\d*', sentence)]
        if s[0] == s[1] and v.varValue == 1.0:
            points.append(input["geometry"]["coordinates"][s[0]])
        elif s[0] != s[1] and v.varValue == 1.0:
            s = [int(s) for s in re.findall(r'-?\d+\.?\d*', sentence)]
            lineStrings.append([input["geometry"]["coordinates"][s[0]], input["geometry"]["coordinates"][s[1]]])

    featureCollection = {
        "type": "FeatureCollection",
        "features": []
    }

    geojson = {
        "type": "Feature",
        "properties":{},
        "geometry":{}
    }

    geojson["geometry"]["type"] = "MultiPoint"
    geojson["geometry"]["coordinates"] = points

    geojson2 = {
        "type": "Feature",
        "properties":{},
        "geometry":{}
    }

    geojson2["geometry"]["type"] = "MultiLineString"
    geojson2["geometry"]["coordinates"] = lineStrings

    featureCollection["features"].append(geojson)
    featureCollection["features"].append(geojson2)
    featureCollection["features"].append(input)
    return jsonify(featureCollection), 200
 

@app.route('/info', methods=['GET'])
def connection_stats():
    """
        Returns the connection stats for database connection (used for 
        debugging).

        Args: None
           
        Returns: 
            Response 200 and JSON String of connection stats - see 
            DBConnection class for more info. 
            
            Response 504 if database connection not possible

    """
    try:
        db = db_connection.DBConnection()
    except IOError:
        return "Database connection not possible", 504, {
            'ContentType': 'text/plain'
        }
    return db.get_connection_stats(), 200, {'ContentType': 'application/json'}


def run():
    """
    Start up Flask endpoint on port 80
    """
    app.run(debug=True, host='0.0.0.0', port=80)
