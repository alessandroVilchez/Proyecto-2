
"""
EJERCICIOS A REALIZAR: 
    - CONSUMIR API DE RICK AND MORTY.
    - SELECCIONAR UNICAMENTE LA DATA EN LA QUE SE VA A TRABAJAR Y TRANSFORMARLA EN UN DATAFRAME.
    - GUARDAR EL DF EN LA BASE DE DATOS.
    - EN LA API SE ESPECIFICAN 3 PAGINAS DE 51 EPISODIOS CADA UNA. Y SE VA A CREAR UN DASHBOARD EN DONDE SE OBSERVE
      LA CANTIDAD DE VECES QUE APARECE CADA PERSONAJE EN ESTA PAGINA O CAPITULO 1 Y EL PORCENTAJE DE VARONES Y MUJERES QUE HAY
      EN LA MISMA PAGINA.
"""
import mysql.connector
import requests 
import pandas as pd 


############################################### INICIO DE CONEXION A LA BASE DE DATOS ################################
conn= mysql.connector.connect(
    host="localhost",
    user="root",
    password=""
)
cursor=conn.cursor(buffered=True)
############################################### FIN DE CONEXION A LA BASE DE DATOS ################################



############################################### INICIO DE CONSUMO DE API ################################
peticion= requests.get(" https://rickandmortyapi.com/api/character")
respuesta=peticion.json()
lista=respuesta["results"]
############################################### FIN DE CONSUMO DE API ###################################



######################################## INICIO FILTRO DE DATA CON LAS COLUMNAS EN LAS QUE SE DESEA TRABAJAR #################

episodios=[]
for x in lista:
#..........................................INICIO MANIPULACION Y TRANSFORMACION DE LA COLUMNA EPISODIOS................ 
    for a in [x["episode"]]:
        episodios.append(len(a))
#..........................................FIN MANIPULACION Y TRANSFORMACION DE LA COLUMNA EPISODIOS...................

    for c in list(x.keys()):
        if c!="id" and c!="name" and c!="status" and c!="species" and c!="gender" and c!="image": 
            del x[c]

######################################## FIN FILTRO DE DATA CON LAS COLUMNAS EN LAS QUE SE DESEA TRABAJAR #####################



###################################  INICIO DE CREACION DE DATAFRAME ##################################
df=pd.DataFrame(lista)
###################################  FIN DE CREACION DE DATAFRAME #####################################



###################################  INCIO DE INSERCION DE LA CANTIDAD DE EPISODIOS AL DF ###################################
df.insert(5, "episodios", episodios)
###################################  FIN DE INSERCION DE LA CANTIDAD DE EPISODIOS AL DF #####################################



###################################  INCIO DE LIMPIEZA DE DATOS CON VALOR "UNKNOWN" #####################################
buscado=df[df.isin(["unknown"]).any(axis=1)]
df["status"]=df["status"].replace("unknown", "Alive")
df["gender"]=df["gender"].replace("unknown", "Male")
###################################  FIN DE LIMPIEZA DE DATOS CON VALOR "UNKNOWN" #####################################



cursor.execute("USE db")
cursor.execute("""CREATE TABLE rick&morty (
               id INT AUTO_INCREMENT PRIMARY KEY, 
               name VARCHAR (100),
               status VARCHAR (30),
               species VARCHAR (30),
               gender VARCHAR (30),
               episodios INT,
               image VARCHAR (150)
)""")

cursor.execute("SELECT * FROM rick&morty")

for i,v in df.iterrows():
    sql="INSERT INTO pokemon (name, status, species, gender, episodios, image) VALUES (%s, %s, %s, %s, %s, %s)"
    values= (v["name"], v["status"], v["species"], v["gender"], v["episodios"], v["image"])
    cursor.execute(sql, values)

conn.commit()

print("esta bien")
