import os

personales_usuario = ['nombre', 'apellidos', 'email', 'direccion', 'ciudad', 'cp', 'telefono','id_usuario']
lista_afinidades = ['edad', 'genero', 'oficio','idiomas','orientacionSexual','religion','politica',\
'mascotas','fumador','carnet','ubicacion','tipoVivienda','region','instalaciones','rangoEdad',\
'dinero', 'metros','lavabo','exteriores','gente','caracter','aficiones', 'id_usuario']


def return_mongo_docs(json_usuario):
    result = []
    doc_usuario = {}
    doc_propiedades = {}

    for propiedad in personales_usuario:
        value = json_usuario[propiedad]
        doc_usuario[propiedad] = value
    for afinidad in lista_afinidades:
        value = json_usuario[afinidad]
        doc_propiedades[afinidad] = value

    #Añadir password custom
    doc_usuario["password"] = '$2b$10$.bk.VGlyghK.ylB0y77psejaHmWrcrw8axrWJqrLNj4tNjjOd9dam'
    result.append(doc_usuario)
    result.append(doc_propiedades)
    return result


if __name__ == "__main__":
    print(os.getcwd())