"""
This module contains all the database information.
There is a Database class to define the schema of the database.
The DBConnection class provides a clean interface to perform operations
on the database.

"""
#standard imports
import json
import os
import uuid
import io
import time
import random

#third party imports
import psycopg2
from psycopg2 import sql
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd


class Database:

    """
    This class is responsible for defining the PostgreSQL database schema.
    """


class DBConnection:
    """
    This class is responsible for initiating the connection with the 
    PostgreSQL database.
    It provides a clean interface to perform operations on the database.
    
    Attributes:
        _engine: API object used to interact with database.
        _conn: handles connection (encapsulates DB session)
        _cur:  cursor object to execute PostgreSQl commands
    """

    def __init__(self,
                 db_user=os.environ['POSTGRES_USER'],
                 db_password=os.environ['POSTGRES_PASSWORD'],
                 host_addr="database:5432",
                 max_num_tries=20):
        """
        Initiates a connection with the PostgreSQL database as the given user 
        on the given port.

        This tries to connect to the database, if not it will retry with 
        increasing waits in between.
      

        Args:
            db_user: the name of the user connecting to the database.
            db_password: the password of said user.
            host_addr: (of the form <host>:<port>) the address where the 
            database is hosted 
            For the Postgres docker container, the default port is 5432 and 
            the host is "database".
            Docker resolves "database" to the internal subnet URL of the 
            database container.
            max_num_tries: the maximum number of tries the __init__ method 
            should try to connect to the database for.
        Returns: None (since __init__)
        Raises:
            IOError: An error occurred accessing the database.
            Raised if after the max number of tries the connection still hasn't
            been established.
        """
        db_name = os.environ['POSTGRES_DB']

        engine_params = (f'postgresql+psycopg2://{db_user}:{db_password}@'
                         f'{host_addr}/{db_name}')
        num_tries = 1

        while True:
            try:
                self._engine = create_engine(engine_params)
                self._conn = self._engine.raw_connection()
                self._cur = self._conn.cursor()
                break
            except (sqlalchemy.exc.OperationalError,
                    psycopg2.OperationalError):
                # Use binary exponential backoff
                #- i.e. sample a wait between [0..2^n]
                #when n = number of tries.
                time.sleep(random.randint(0, 2**num_tries))
                if num_tries > max_num_tries:
                    raise IOError("Database unavailable")
                num_tries += 1

    def create_extension(self):
        """
        Creates the extension postgis.

        Args: None
           
        Returns: None (since commits execution result to database)
        """
        
        self._cur.execute(
            sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis"))
        self._conn.commit()
        


    def isValid(self, geometry):
        """
        Verifica se a geometria inserida é valida utilizando a função IsValid() do Postgis.

        Args: None
           
        Returns: None (since commits execution result to database)
        """

        self._cur.execute(
            sql.SQL("SELECT ST_IsValid(ST_GeomFromGeoJSON('{}')) As good_line".format(geometry)))
        self._conn.commit()
        return json.dumps(self._cur.fetchall()[0][0])
    


    def get_connection_stats(self):
        """
       Returns the statistics for the database connection (useful for 
       debugging). 

        Args: None
           
        Returns: A JSON string consisting of connection statistics. 
        e.g. 
        {"user": "tester", "dbname": "testdatabase",
         "host": "database", "port": "5432", 
         "tty": "", "options": "", "sslmode": "prefer", 
         "sslcompression": "0", "krbsrvname": "postgres",
         "target_session_attrs": "any"}
         """
        return json.dumps(self._conn.get_dsn_parameters())
