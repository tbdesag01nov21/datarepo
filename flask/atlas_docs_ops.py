import pymongo
import dns
import os


def create_mongo_connection(mongo_info):
    # Creating the low level functional client
    # Creating the high level object oriented interface

    mongo_user = mongo_info["mongo_user"]
    mongo_password = mongo_info["mongo_password"]
    mongo_host = mongo_info["mongo_host"]
    mongo_database = mongo_info["mongo_database"]

    atlas_conn_string = "mongodb+srv://{usuario}:{password}@{host}/{database}?retryWrites=true&w=majority"\
                .format(usuario=mongo_user, password=mongo_password, host=mongo_host, database=mongo_database)
    client = pymongo.MongoClient(atlas_conn_string)
    database = client[mongo_database]
    return database

def check_coleccion_mongo(mongo_database, mongo_collection):
    if mongo_collection in mongo_database.list_collection_names():
        return True
    else:
        return False

def crear_coleccion_mongo(mongo_database, nombre_coleccion):
    mycol = mongo_database[nombre_coleccion]

def regenerar_coleccion_mongo(mongo_database, nombre_coleccion):
    if check_coleccion_mongo(mongo_database, nombre_coleccion):
        mycol = mongo_database[nombre_coleccion]
        mycol.drop()
    crear_coleccion_mongo(mongo_database, nombre_coleccion)

def put_docs(col_usuarios, col_afinidades, user_docs):
    col_usuarios.insert_one(user_docs[0])
    col_afinidades.insert_one(user_docs[1])


def get_user_docs_from_mongo(col_usuarios, col_afinidades, id_usuario):
    result = []
    query_user = { "id_usuario":  id_usuario}

    user_doc = col_usuarios.find_one( query_user, {"_id" : 0, "password" : 0} )
    info_doc = col_afinidades.find_one(query_user, {"_id": 0})

    if user_doc is not None and info_doc is not None:
        result.append(user_doc.copy())
        result.append(info_doc.copy())


    return result

if __name__ == "__main__":
    print(os.getcwd())