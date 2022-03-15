from flask import Flask, render_template, request, jsonify
import pandas as pd
import pymysql
import numpy as np
from sqlalchemy import create_engine
import pymongo
import dns
import json
import os
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors

app = Flask(__name__)
"""gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)"""


def get_file_creds(file_info):
    cwd = os.getcwd()
    script = os.path.realpath(__file__)
    myvars = {}
    with open(file_info) as myfile:
        for line in myfile:
            name, var = line.partition("=")[::2]
            myvars[name.strip()] = var.rstrip()

    return myvars

def get_mysql_creds():
    myvars = {}
    with open("mysql_info.txt") as myfile:
        for line in myfile:
            name, var = line.partition("=")[::2]
            myvars[name.strip()] = var.rstrip()

    return myvars

def get_mongo_creds():
    myvars = {}
    with open("mongo_info.txt") as myfile:
        for line in myfile:
            name, var = line.partition("=")[::2]
            myvars[name.strip()] = var.rstrip()

    return myvars


def create_mongo_connection(mongo_info):
    # Creating the low level functional client
    # Creating the high level object oriented interface
    app.logger.info('Conectando a Atlas')


    mongo_user = mongo_info["mongo_user"]
    mongo_password = mongo_info["mongo_password"]
    mongo_host = mongo_info["mongo_host"]
    mongo_database = mongo_info["mongo_database"]
    mongo_collection = mongo_info["mongo_collection"]

    atlas_conn_string = "mongodb+srv://{usuario}:{password}@{host}/{database}?retryWrites=true&w=majority".format(usuario=mongo_user, password=mongo_password, host=mongo_host, database=mongo_database)

    client = pymongo.MongoClient(atlas_conn_string) 
    database = client[mongo_database]
    coleccion = database[mongo_collection]

    return database



def create_mysql_connection(mysql_info):
    # Creating the low level functional client
    # Creating the high level object oriented interface
    app.logger.info('Conectando a Mysql')

    mysql_user = mysql_info["mysql_user"]
    mysql_password = mysql_info["mysql_password"]
    mysql_host = mysql_info["mysql_host"]
    mysql_port = mysql_info["mysql_port"]
    mysql_db = mysql_info["mysql_db"]

    mysql_conn_string = "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=mysql_host, db=mysql_db, user=mysql_user, pw=mysql_password, port=mysql_port)
    engine = create_engine(mysql_conn_string)
    connection = engine.raw_connection()
    return connection


app.logger.info('App root path: ' + app.root_path)
database_mongo = create_mongo_connection(get_file_creds("mongo_info.txt"))
conexion_mysql = create_mysql_connection(get_file_creds("mysql_info.txt"))

@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')

@app.route('/api/recommend/users/<int:id_usuario>', methods=['GET'])
def recommend(id_usuario):
    coleccion_users = database_mongo["usuarios"]
    coleccion_info_users = database_mongo["infotestafinidads"]
    query_user = { "id_usuario":  id_usuario}
    info_user_doc = coleccion_info_users.find(query_user)

    user_doc = coleccion_users.find_one( query_user, {"_id" : 0, "password" : 0} )
    info_doc = coleccion_info_users.find_one(query_user, {"_id": 0})

    response = user_doc.copy()
    response.update(info_doc)
    # insertar usuario en mysql-analitico
    # generar dataframe

    # recomendacion
    return jsonify(response)



if __name__ == "__main__":
    app.run()

