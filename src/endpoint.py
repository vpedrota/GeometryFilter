"""
This module contains the implementation of the endpoint.

The endpoint provides a clean REST API for other sections of the project to 
query and modify the database.

"""
#third party imports
from flask import Flask, request
from flask_cors import CORS

#local application imports

try:
    import db_connection
except ModuleNotFoundError:
    from app.src import db_connection

app = Flask(__name__)  #pylint: disable=C0103
CORS(app)


@app.route('/', methods=['GET'])
def database_info():
    try:
        db = db_connection.DBConnection()
    except IOError:
        return "Database connection not possible", 504, {
            'ContentType': 'text/plain'
        }

    return db.isValid(), 200


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
