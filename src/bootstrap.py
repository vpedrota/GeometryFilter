"""
This module contains the code to run the bootstrap procedure for the endpoint
 and database.
"""

#standard imports
import glob

#local application imports -``
try:
    import db_connection
    import endpoint
except ModuleNotFoundError:
    from app.src import db_connection
    from app.src import endpoint


def bootstrap():
    """
        Sets up the database with a clean consistent state.
        Args: None
           
        Returns: None
    """
    print("Beginning bootstrap procedure...")  #for debugging
    db = db_connection.DBConnection()  #pylint: disable=C0103
    db.create_extension()
    
if __name__ == '__main__':  #set up database and endpoint
    bootstrap()
    endpoint.run()