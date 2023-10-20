#Entregable 2

import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

#Primero hacemos el diccionario con las ciudades
cities = [
    {'station': '87453', 'name': 'Rio Cuarto'},
    {'station': '87576', 'name': 'Ezeiza'},
    {'station': '87345', 'name': 'Cordoba'},
    {'station': '87571', 'name': 'Moron'},
    {'station': '87480', 'name': 'Rosario'},
    {'station': '87497', 'name': 'Gualeguaychu'},
    {'station': '87047', 'name': 'Salta'},
    {'station': '87046', 'name': 'San Salvador de Jujuy'},
    {'station': '87121', 'name': 'San Miguel de Tucuman'},
    {'station': '87217', 'name': 'La Rioja'}
]
# Lista para almacenar los datos de todas las ciudades
all_filtered_data = []

#Declaracion de credenciales + conexion
credenciales = {
    'username': 'juanaescobarpinzon_coderhouse',
    'password': 'nG6Z4M9Z80',
    'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    'port': '5439',
    'database': 'data-engineer-database'
}
db_conn_str = f"postgresql://{credenciales['username']}:{credenciales['password']}@{credenciales['host']}:{credenciales['port']}/{credenciales['database']}"
conn = create_engine(db_conn_str)

#Request de la API meteostat
for city_info in cities:
    city = city_info['station']
    name = city_info['name']

    url = "https://meteostat.p.rapidapi.com/stations/daily"

    querystring = {"station":city,"start":"2023-08-01","end":"2023-08-01"}
    headers = {
        "X-RapidAPI-Key": "a5b08030f7mshb2c9409a627ea15p10ce88jsn90cea41c9648",
        "X-RapidAPI-Host": "meteostat.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    data = response.json()

    #Aca se filtran los campos que quiero tener 
    filtered_data = []
    for entry in data['data']:
        filtered_entry = {
            'fecha': entry['date'],
            'ciudad':name, #agregado ultimo
            'temp_promedio': entry['tavg'],
            'temp_minima': entry['tmin'],
            'temp_maxima': entry['tmax'],
            'precipitaciones': entry['prcp'],
            'nieve': entry['snow'],
            'dir_viento': entry['wdir'],
            'vel_viento': entry['wspd'],
            'presion': entry['pres'],
            'minutos_atardecer': entry['tsun']
            
        }
        filtered_data.append(filtered_entry)

    all_filtered_data.extend(filtered_data)

#Crear el dataframe
df = pd.DataFrame(all_filtered_data)


table_name = 'prueba_clima'

#Ver si existe la tabla
inspector = inspect(conn)
table_exists = any(table_name.lower() == table.lower() for table in inspector.get_table_names())

#Aca declaro las pk
if not table_exists:
    create_table_query = """
    CREATE TABLE {} (
        fecha DATE,
        ciudad VARCHAR(255),
        temp_promedio FLOAT,
        temp_minima FLOAT,
        temp_maxima FLOAT,
        precipitaciones FLOAT,
        nieve FLOAT,
        dir_viento FLOAT,
        vel_viento FLOAT,
        presion FLOAT,
        minutos_atardecer FLOAT,
        PRIMARY KEY (fecha, ciudad)
    )
    """.format(table_name)
    conn.execute(create_table_query)


# Ingestar los datos en la tabla
df.to_sql(table_name, conn, index=False, if_exists='append')

# Cerrar la conexión
conn.dispose()
