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


def get_datos_mysql(conexion_mysql):
    df =pd.read_sql("select * from users", conexion_mysql);
    #print(df.to_json(orient='index'))
    return df


def obtener_usuarios_similares(matrix_df, knn_model, user, n=5):
    # input a esta función es el ID del usuario y el número de top usuarios que quieres que el modelo considere
    # número de usuarios se traduce en el valor que asignamos a n aquí
    knn_input = np.asarray([matrix_df.values[user - 1]])
    distances, indices = knn_model.kneighbors(knn_input, n_neighbors=n + 1)
    result = {}
    ids_usuario = []

    for i in range(1, len(distances[0])):
        ids_usuario.append(indices[0][i] + 1)

    for i, id in enumerate(ids_usuario):
        result[str(i+1)] = str(id)

    return result

def ejecutar_recomendacion(conexion_mysql, id_usuario, num_recomendaciones ):
    matrix_df = get_datos_mysql(conexion_mysql)
    columns_to_drop = [ 'users_name',
                        'users_last_name',
                        'users_last_name_2',
                        'users_initial',
                        'users_years',
                        'users_gender',
                        'users_sector',
                        'users_ccaa',
                        'users_prov',
                        'users_prox',
                        'users_phone',
                        'users_mail',
                        'users_lang',
                        'users_lgtb',
                        'users_family',
                        'users_relig',
                        'users_politic',
                        'users_vegan',
                        'users_pets',
                        'users_smoke',
                        'users_env',
                        'users_range',
                        'users_budget',
                        'users_sqr_m',
                        'users_bath',
                        'users_outs',
                        'users_facil',
                        'users_drive',
                        'users_max_p']
    matrix_df.drop(columns_to_drop, axis=1, inplace=True)
    df = pd.melt(matrix_df,
                 id_vars='users_id',
                 value_vars=list(matrix_df.columns[1:]),
                 var_name='Aficiones',
                 value_name='Valoracion')

    # usamos pivot de pandas y creamos matriz aficiones - usuario
    matrix_df = df.pivot(
        index='users_id',
        columns='Aficiones',
        values='Valoracion').fillna(0)

    # transformar matriz en scipy sparse
    # se pueden usar en operaciones aritméticasm admiten sumas, restas, multiplicaciones, divisiones y potencia de matrices
    # csr_matrix viene de Compressed Sparse Row matrix
    matrix_sparse_df = csr_matrix(matrix_df.values)

    knn_model = NearestNeighbors(metric='cosine', algorithm='brute')
    knn_model.fit(matrix_sparse_df)

    return obtener_usuarios_similares(matrix_df, knn_model, id_usuario, num_recomendaciones)


app.logger.info('App root path: ' + app.root_path)
database_mongo = create_mongo_connection(get_file_creds("mongo_info.txt"))
conexion_mysql = create_mysql_connection(get_file_creds("mysql_info.txt"))

@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')

@app.route('/api/recommend/users/<int:id_usuario>', methods=['GET'])
def recommend(id_usuario):
    response = ejecutar_recomendacion(conexion_mysql, id_usuario, 10)
    return jsonify(response)



if __name__ == "__main__":
    app.run()

