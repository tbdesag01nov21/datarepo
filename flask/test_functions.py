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


def get_file_creds(file_info):
    cwd = os.getcwd()
    script = os.path.realpath(__file__)
    myvars = {}
    with open(file_info) as myfile:
        for line in myfile:
            name, var = line.partition("=")[::2]
            myvars[name.strip()] = var.rstrip()

    return myvars


def create_mongo_connection(mongo_info):
    # Creating the low level functional client
    # Creating the high level object oriented interface



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


    mysql_user = mysql_info["mysql_user"]
    mysql_password = mysql_info["mysql_password"]
    mysql_host = mysql_info["mysql_host"]
    mysql_port = mysql_info["mysql_port"]
    mysql_db = mysql_info["mysql_db"]

    mysql_conn_string = "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=mysql_host, db=mysql_db, user=mysql_user, pw=mysql_password, port=mysql_port)
    engine = create_engine(mysql_conn_string)
    connection = engine.raw_connection()
    return connection

def recommend(id_usuario):
    coleccion_users = database_mongo["usuarios"]
    coleccion_info_users = database_mongo["infotestafinidads"]
    query_user = { "id_usuario":  id_usuario}

    user_doc = coleccion_users.find_one( query_user, {"_id" : 0, "password" : 0} )
    info_doc = coleccion_info_users.find_one(query_user, {"_id": 0})

    response = user_doc.copy()
    response.update(info_doc)
    # insertar usuario en mysql-analitico
    # generar dataframe

    # recomendacion
    return response

def get_datos_mysql(conexion_mysql):
    df =pd.read_sql("select * from users", conexion_mysql);
    #print(df.to_json(orient='index'))
    return df


def obtener_usuarios_similares(matrix_df, knn_model, user, n=5):
    # input a esta función es el ID del usuario y el número de top usuarios que quieres que el modelo considere
    # número de usuarios se traduce en el valor que asignamos a n aquí
    knn_input = np.asarray([matrix_df.values[user - 1]])
    distances, indices = knn_model.kneighbors(knn_input, n_neighbors=n + 1)

    print("Los", n, "usuarios más similares al usuario", user, "son: ")
    print(" ")
    for i in range(1, len(distances[0])):
        print(i, ". Usuario:", indices[0][i] + 1, "separado por una distancia de", distances[0][i])
    return indices.flatten()[1:] + 1, distances.flatten()[1:]

def ejecutar_recomendacion(conexion_mysql):
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

    obtener_usuarios_similares(matrix_df, knn_model, 500, 10)

def export_table_info(conexion_mysql):
    matrix_df = get_datos_mysql(conexion_mysql)
    print(list(matrix_df.columns.values))

def insert_mysql_mongo_user(id_usuario):
    #Probar conexion a mongo
    personales_usuario = ['nombre', 'apellidos', 'email', 'direccion', 'ciudad', 'cp', 'telefono']
    listas_simples = ['oficio', 'idiomas', 'ubicacion', 'tipoVivienda', 'region', 'instalaciones', 'rangoEdad', 'exteriores', 'caracter']
    listas_valoracion = ['aficiones']
    propiedades_bool = ["orientacionSexual", "religion", "politica", "mascotas", "fumador", "carnet"]

    propiedades_int = ["dinero","metros","gente"]
    propiedades_value = ["lavabo"]

    id_usuario = 1002

    #INSERT INTO users(id_usuario, nombre, apellidos, carnet, ciudad, cp, direccion, edad, email, telefono, genero)
    #VALUES(100, 'Cien', 'Ciento', '100100P', 'Madrid', '28100', 'Calle Cien', 60, 'ciento@gmail.com', '34100100','hombre');

    #json_usuario = recommend(id_usuario)
    f = open('ejemplo_full05_from_mongo.txt')
    json_usuario = json.load(f)
    cursor = conexion_mysql.cursor()

    columns_string_insert = ''
    values_string_insert = ''
    for prop_personal in personales_usuario:
        value_prop_personal = json_usuario[prop_personal]
        columns_string_insert = columns_string_insert +prop_personal + ", "
        value_string = "'" + value_prop_personal + "', "
        values_string_insert = values_string_insert + value_string

    columns_string_insert = columns_string_insert.rsplit(', ', 1)[0]
    values_string_insert = values_string_insert.rsplit(', ', 1)[0]

    string_sql_insert  = "INSERT INTO users(id_usuario," + columns_string_insert + ") VALUES("+ str(id_usuario) + ", " +  values_string_insert + ");"
    fsqlinsert = open("demosqlinsert.txt", "w")
    fsqlinsert.write(string_sql_insert)
    fsqlinsert.close()

    values_string_update =''
    for prop_user in json_usuario.keys():
        if prop_user in listas_valoracion:
            valoraciones_user = json_usuario[prop_user]
            for valoracion_user in valoraciones_user:
                elementos_valoracion = valoracion_user.split(': ')
                value_string = elementos_valoracion[0] + '=' + elementos_valoracion[1] + ', '
                values_string_update = values_string_update + value_string
            print(values_string_update)

        if prop_user in listas_simples:
            selecciones_user = json_usuario[prop_user]
            for seleccion_user in selecciones_user:
                valor = '1'
                value_string = seleccion_user + '=' + valor + ', '
                values_string_update = values_string_update + value_string
            print(values_string_update)

        if prop_user in propiedades_bool:
            seleccion_user = json_usuario[prop_user]
            valor = '1' if seleccion_user == True else '0'
            value_string = prop_user + '=' + valor + ', '
            values_string_update = values_string_update + value_string
            print(values_string_update)

    values_string_update = values_string_update.rsplit(', ', 1)[0]
    string_sql_update = "update users set " + values_string_update + " where id_usuario=" + str(id_usuario) + ";"

    fsqlupdate = open("demosqlupdate.txt", "w")
    fsqlupdate.write(string_sql_update)
    fsqlupdate.close()

    f.close()

def alter_mysql_table(conexion_mysql, id_usuario):
    #Probar conexion a mongo
    personales_usuario = ['nombre', 'apellidos', 'email', 'direccion', 'ciudad', 'cp', 'telefono']
    listas_simples = ['oficio', 'idiomas', 'ubicacion', 'tipoVivienda', 'region', 'instalaciones', 'rangoEdad', 'exteriores', 'caracter']
    listas_valoracion = ['aficiones']
    propiedades_bool = ["orientacionSexual", "religion", "politica", "mascotas", "fumador", "carnet"]
    propiedades_int = ["dinero","metros","gente"]
    propiedades_value = ["lavabo"]



    id_usuario = 1002
    #json_usuario = recommend(id_usuario)

    #INSERT INTO users(id_usuario, nombre, apellidos, carnet, ciudad, cp, direccion, edad, email, telefono, genero)
    #VALUES(100, 'Cien', 'Ciento', '100100P', 'Madrid', '28100', 'Calle Cien', 60, 'ciento@gmail.com', '34100100','hombre');

    f = open('ejemplo_full05_from_mongo.txt')
    json_usuario = json.load(f)
    cursor = conexion_mysql.cursor()

    for prop_user in json_usuario.keys():
        if prop_user in listas_valoracion:
            valoraciones_user = json_usuario[prop_user]
            for valoracion_user in valoraciones_user:
                elementos_valoracion = valoracion_user.split(': ')
                print(elementos_valoracion[0])
                command = "ALTER TABLE users ADD COLUMN {} TINYINT DEFAULT 0;".format(elementos_valoracion[0])
                cursor.execute(command)

        if prop_user in listas_simples:
            selecciones_user = json_usuario[prop_user]
            for seleccion_user in selecciones_user:
                command = "ALTER TABLE users ADD COLUMN {} TINYINT DEFAULT 0;".format(seleccion_user)
                cursor.execute(command)

        if prop_user in propiedades_bool:
            command = "ALTER TABLE users ADD COLUMN {} TINYINT DEFAULT 0;".format(prop_user)
            cursor.execute(command)

        if prop_user in propiedades_int:
            command = "ALTER TABLE users ADD COLUMN {} TINYINT DEFAULT 0;".format(prop_user)
            cursor.execute(command)

        if prop_user in propiedades_value:
            command = "ALTER TABLE users ADD COLUMN {} TINYINT DEFAULT 0;".format(prop_user)
            cursor.execute(command)

    conexion_mysql.commit()
    cursor.close()
    f.close()

if __name__ == "__main__":
    database_mongo = create_mongo_connection(get_file_creds("mongo_info.txt"))
    conexion_mysql = create_mysql_connection(get_file_creds("mysql_info.txt"))
    #ejecutar_recomendacion(conexion_mysql)  #ok
    #export_table_info(conexion_mysql)
    print(recommend(1002))
    #insert_mysql_mongo_user(1002)
    #alter_mysql_table(conexion_mysql, 1002)
    conexion_mysql.close()


    exit()

