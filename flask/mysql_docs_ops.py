from sqlalchemy import create_engine
import os
import json

columnas_crear = {'aficiones': ['accion', 'ajedrez', 'alternativa',
             'animales', 'antiguedades', 'arte',
             'artesanias', 'astrologia', 'astronomia', 'aventura',
             'aventurero', 'aves', 'badminton', 'bailarin', 'baile',
             'baloncesto', 'beatboxing', 'beisbol', 'belico', 'bingo',
             'biologia', 'blogging', 'blues', 'bolero', 'bolillo',
             'bolos', 'bordado', 'boxeo', 'bricolage', 'buceo',
             'cantante', 'canto', 'carpinteria', 'cartas',
             'casino', 'caza', 'ceramica', 'cerveza', 'ciclismo',
             'cienciaficcion', 'cine', 'cinefilo', 'clasica', 'cocina',
             'comedia', 'comics', 'comprador', 'conciertos',
             'copla', 'corazon', 'correr', 'cosplay', 'costura',
             'crimen', 'croche', 'crossfit', 'crucigramas',
             'cultura', 'dardos', 'dibujo',
             'disfraces', 'documental', 'drama', 'electronica',
             'emprendedor', 'entustiasta', 'escritura', 'esqui',
             'excursionismo', 'excursionista', 'familiar', 'fantasia', 'fiestero',
             'filatelia', 'filologia', 'fisica', 'flamenco',
             'folk', 'foodie', 'fotografia', 'fotografo', 'fronton',
             'fumadores', 'futbol', 'gamer', 'ganchillo', 'gastronomia',
             'gimnasio', 'golf', 'gospel',
             'hipica', 'historia', 'historicas', 'hogar',
             'iglesia', 'india', 'instrumental', 'instrumentos', 'jardineria',
             'jazz', 'latina', 'libros', 'literatura', 'lounge',
             'macrame', 'madrugador', 'magia', 'manualidades', 'matematicas',
             'meditacion', 'mediterranea', 'mesa', 'metal', 'mexicana',
             'misterio', 'moda', 'modelismo', 'montanismo', 'motociclismo',
             'museos', 'musical', 'natacion', 'negro',
             'nocturno', 'numismatica', 'opera',
             'oriental', 'pachangueo', 'padel', 'pesca', 'petanca',
             'pintura', 'piraguismo', 'playa', 'poesia', 'poker',
             'politicas', 'punk', 'puntocruz',
             'quimica', 'rb', 'reciclaje', 'reggae',
             'remo', 'reposteria', 'robotica', 'rock', 'rol',
             'romantica', 'rompecabezas', 'saludable', 'sociable',
             'socializar', 'soul', 'suspense', 'tarot', 'teatro',
             'tecnologia', 'tejido', 'telenovelas', 'tenis', 'tenismesa',
             'terror', 'trabajo', 'turismo', 'vegano',
             'vegetariano', 'viajar', 'viajero', 'videojuegos', 'vino',
             'vivo', 'voluntariado', 'western', 'yeye', 'yoga',
             'zarzuela', 'zoologia'],

 'oficio': ['sanidad', 'ingenieria', 'forestal', 'administrativo', 'artes', 'comunicacion', 'derecho',
          'empresariales', 'enseñanza', 'investigacioncientifica', 'investigaciontecnica', 'musica',
          'seguridad', 'rh', 'cineasta', 'amacasa', 'mantenimiento', 'construccion', 'otro'],

 'idiomas': ["castellano", "catalan", "euskera", "gallego", "valenciano", "coreano", "chino", "japones", "ingles",
           "italiano", "aleman", "frances", "ruso", "neerlandes"],

 'ubicacion': ['interior', 'ciudad', 'costa', 'montaña'],

 'tipoVivienda': ['piso', 'unifamiliar', 'atico', 'duplex', 'chalet', 'otros'],

 'region': ["andalucia", "aragon", "asturias", "baleares", "canarias", "cantabria", "castillaleon", "castillalamancha",
          "catalunya", "valencia", "extremadura", "galicia", "madrid", "murcia", "navarra", "paisvasco", "rioja",
          "ceuta",
          "melilla"],

 'instalaciones': ['salacine', 'recreativos', 'salamanualidades', 'salagimnasio', 'rehabilitacion',
                 'pingpong', 'spa', 'billar', 'futbolin', 'trastero',
                 'garage', 'salacocina', 'buffet', 'lavavajillas', 'lavanderia'],

 'rangoEdad': ["50_60", "60_70", "70_80", "_80"],

 'exteriores': ['jardin', 'terraza', 'canchabaloncesto', 'huerto', 'barbacoa', 'canchapadel',
              'piscina', 'cubierta', 'campofutbol', 'campogolf', 'campohipica', 'campoesqui'],

 'caracter': ["activo", "deportista", "calmado", "atento", "alegre",
            "colaborador", "creativo", "decidido", "tratofacil",
            "empatico", "entusiasta", "flexible", "amable", "divertido",
            "honesto", "gracioso", "optimista", "ordenado", "apasionado",
            "practico", "proactivo", "sensato", "relajado", "generoso"],

 'propiedades_bool': ["orientacionSexual", "religion", "politica", "mascotas", "fumador", "carnet"],
 'propiedades_int': ["dinero", "metros", "gente"],
 'propiedades_value': ["lavabo",'no']}




def create_mysql_connection(mysql_info):
    # Creating the low level functional client
    # Creating the high level object oriented interface

    mysql_user = mysql_info["mysql_user"]
    mysql_password = mysql_info["mysql_password"]
    mysql_host = mysql_info["mysql_host"]
    mysql_port = mysql_info["mysql_port"]
    mysql_db = mysql_info["mysql_db"]

    mysql_conn_string = "mysql+pymysql://{user}:{pw}@{host}/{db}"\
        .format(host=mysql_host, db=mysql_db, user=mysql_user, pw=mysql_password, port=mysql_port)
    engine = create_engine(mysql_conn_string)
    connection = engine.raw_connection()
    return connection

def eliminar_tabla(conexion_mysql, table):
    cursor = conexion_mysql.cursor()
    command = "DROP  TABLE IF EXISTS {table_drop}".format(table_drop=table)
    cursor.execute(command)
    conexion_mysql.commit()
    cursor.close()


def crear_tabla_mysql(conexion_mysql, tabla):
    cursor = conexion_mysql.cursor()

    command = "CREATE TABLE IF NOT EXISTS {} (id_usuario INT);".format(tabla)
    cursor.execute(command)

    for lista in columnas_crear.keys():
        columnas = columnas_crear[lista]
        for columna in columnas:
            command = "ALTER TABLE users ADD COLUMN {} TINYINT DEFAULT 0;".format(columna)
            cursor.execute(command)

    conexion_mysql.commit()
    cursor.close()

def insert_mysql_mongo_user(conexion_mysql, tabla, json_usuario):
    id_usuario = json_usuario['id_usuario']
    cursor = conexion_mysql.cursor()
    command = "INSERT INTO users(id_usuario) VALUES({});".format(id_usuario)
    cursor.execute(command)
    conexion_mysql.commit()

def limpiar_datos_usuario(conexion_mysql, tabla, json_usuario):
    cursor = conexion_mysql.cursor()
    valor = '0'
    values_string_update = ''
    id_usuario = json_usuario['id_usuario']

    for lista in columnas_crear.keys():
        columnas = columnas_crear[lista]
        for columna in columnas:
            value_string = columna + '=' + valor + ', '
            values_string_update = values_string_update + value_string


    values_string_update = values_string_update.rsplit(', ', 1)[0]
    string_sql_update = "update users set " + values_string_update + " where id_usuario=" + str(id_usuario) + ";"

    command = string_sql_update
    try:
        cursor.execute(command)
        conexion_mysql.commit()
    except:
        f = open("errorSQLinsert.txt", "w")
        f.write(command)
        f.write('-------------------------------------------------------')
        f.close()

    cursor.close()

def limpiar_campo(limpiar, valor):
    if limpiar:
        return '0'
    else:
        return valor

def update_mysql_mongo_user(conexion_mysql, tabla, json_usuario, limpiar):
    acumulador_columnas = []
    for lista in columnas_crear.keys():
        valores = columnas_crear[lista]
        for valor in valores:
            acumulador_columnas.append(valor)
    #Probar conexion a mongo
    personales_usuario = ['nombre', 'apellidos', 'email', 'direccion', 'ciudad', 'cp', 'telefono']
    listas_simples = ['oficio', 'idiomas', 'ubicacion', 'tipoVivienda', 'region', 'instalaciones', 'rangoEdad', 'exteriores', 'caracter']
    listas_valoracion = ['aficiones']
    propiedades_bool = ["orientacionSexual", "religion", "politica", "mascotas", "fumador", "carnet"]

    propiedades_int = ["dinero","metros","gente"]
    propiedades_value = ["lavabo"]

    id_usuario = json_usuario['id_usuario']

    cursor = conexion_mysql.cursor()


    values_string_update = ''
    for prop_user in json_usuario.keys():
        if prop_user in listas_valoracion:
            valoraciones_user = json_usuario[prop_user]
            for valoracion_user in valoraciones_user:
                elementos_valoracion = valoracion_user.split(': ')
                if elementos_valoracion[0] in acumulador_columnas:
                    value_string = elementos_valoracion[0] + '=' + limpiar_campo(limpiar,elementos_valoracion[1]) + ', '
                    values_string_update = values_string_update + value_string


        if prop_user in listas_simples:
            selecciones_user = json_usuario[prop_user]
            if type(selecciones_user) == list:
                for seleccion_user in selecciones_user:
                    if seleccion_user in acumulador_columnas:
                        valor = '1'
                        value_string = seleccion_user + '=' + limpiar_campo(limpiar,valor) + ', '
                        values_string_update = values_string_update + value_string
            elif type(selecciones_user) == str:
                if selecciones_user in acumulador_columnas:
                    valor = '1'
                    value_string = selecciones_user + '=' + limpiar_campo(limpiar,valor) + ', '
                    values_string_update = values_string_update + value_string


        if prop_user in propiedades_bool:
            seleccion_user = json_usuario[prop_user]
            valor = '1' if seleccion_user == True else '0'
            if prop_user in acumulador_columnas:
                value_string = prop_user + '=' + limpiar_campo(limpiar,valor) + ', '
                values_string_update = values_string_update + value_string


    values_string_update = values_string_update.rsplit(', ', 1)[0]
    string_sql_update = "update users set " + values_string_update + " where id_usuario=" + str(id_usuario) + ";"

    command = string_sql_update
    try:
        cursor.execute(command)
        conexion_mysql.commit()
    except:
        f = open("errorSQLinsert.txt", "w")
        f.write(command)
        f.write('-------------------------------------------------------')
        f.close()

    cursor.close()

def check_id_usuario(conexion_mysql, tabla, json_usuario):
    id_usuario = json_usuario['id_usuario']
    cursor = conexion_mysql.cursor()
    command = "select id_usuario from users where id_usuario={};".format(id_usuario)
    try:
        cursor.execute(command)
        records = cursor.fetchone()
        if type(records) == tuple:
            return True
        else:
            return False
    except:
        return False

if __name__ == "__main__":
    print(os.getcwd())