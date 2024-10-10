import urllib.request, json, os, dotenv, pprint
import pandas as pd
import psycopg2

from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text

load_dotenv()


class Extraer:
    def __init__(self, url, api_key):
        self.url = url
        self.api_key = api_key
        self.raw_data = None
        self.df_info_posiciones = {'VehicleID': [], 
                                'TripID': [],
                                'RouteID': [], 
                                'BlockNumber': [],
                                'Lat': [],
                                'Lon': [],
                                'DirectionText': [],
                                'TripHeadsign': [],
                                'DateTime': [],
                                'TripStartTime': [],
                                'TripEndTime': [],
                                'id': []}


    def generar_json(self):
        try:
            headers = {'Cache-Control': 'no-cache',
                            'api_key': self.api_key}

            req = urllib.request.Request(self.url, headers=headers)

            req.get_method = lambda: 'GET'      #Se sobreescribe el metodo y se le asigna lambda
            # Abrir la conexión y obtener la respuesta
            with urllib.request.urlopen(req) as response:
                # Leer el contenido de la respuesta (que está en bytes) y decodificarlo a UTF-8
                # Esta expresion es un string, por lo que hay que convertirlo a diccionario 
                # posteriormente
                response_data = response.read().decode('utf-8')

            # cargar los datos JSON en un diccionario de python
            self.raw_data = json.loads(response_data)
        except Exception as e:
            print(f'Hubo un error al generar el archivo Json: {e}')


    def df_buses(self):
        try:
            for diccionario in self.raw_data['BusPositions']:
                self.df_info_posiciones['VehicleID'].append(int(diccionario['VehicleID']))
                self.df_info_posiciones['TripID'].append(int(diccionario['TripID']))               
                self.df_info_posiciones['RouteID'].append(diccionario['RouteID'])
                self.df_info_posiciones['BlockNumber'].append(diccionario['BlockNumber'])
                self.df_info_posiciones['Lat'].append(diccionario['Lat'])
                self.df_info_posiciones['Lon'].append(diccionario['Lon'])
                self.df_info_posiciones['DirectionText'].append(diccionario['DirectionText'])
                self.df_info_posiciones['TripHeadsign'].append(diccionario['TripHeadsign'])
                self.df_info_posiciones['DateTime'].append(diccionario['DateTime'])
                self.df_info_posiciones['TripStartTime'].append(diccionario['TripStartTime'])
                self.df_info_posiciones['TripEndTime'].append(diccionario['TripEndTime'])
                self.df_info_posiciones['id'].append(f'{diccionario['VehicleID']}_{diccionario['TripID']}')
            self.df_info_posiciones = pd.DataFrame(self.df_info_posiciones)

            #cambiamos formato de datos a tipo "datetime"
            self.df_info_posiciones['DateTime'] = pd.to_datetime(self.df_info_posiciones['DateTime'], format="%Y-%m-%dT%H:%M:%S")
            self.df_info_posiciones['TripStartTime'] = pd.to_datetime(self.df_info_posiciones['TripStartTime'], format="%Y-%m-%dT%H:%M:%S")           
            self.df_info_posiciones['TripEndTime'] = pd.to_datetime(self.df_info_posiciones['TripEndTime'], format="%Y-%m-%dT%H:%M:%S")
            print('Se ha generado la tabla exitosamente\n')
            print(self.df_info_posiciones)
            return self.df_info_posiciones
        except Exception as e:
            print(f"Error al generar dataframe: {e}")



class Cargar:
    def __init__(self, credenciales: dict, schema: str, nombre_tabla: str, dataframe):
        self.credenciales = credenciales
        self.schema = schema
        self.nombre_tabla = nombre_tabla
        self.dataframe = dataframe
        self.conexion = None
        

    def crear_motor_conexion(self):
        user = self.credenciales.get('redshift_user')
        password = self.credenciales.get('redshift_pass')
        host = self.credenciales.get('redshift_host')
        port = self.credenciales.get('redshift_port')
        database = self.credenciales.get('redshift_database')

        if self.conexion is None:
            try:
                engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
                try:
                    self.conexion = engine.connect()
                    #ejecutamos un query aleatorio para ver si la conexión está estable
                    prueba = self.conexion.execute('SELECT 1;')
                    if prueba:
                        print('Conectado a AWS Redshift con éxito')
                        return self.conexion
                except Exception as e:
                    print(f"Error al conectar a AWS Redshift: {e}")
            except Exception as e:
                print(f"Error al generar el motor de conexion: {e}")
    

    def crear_tabla(self):
        if self.conexion is not None:
            try:
                query = f'''CREATE TABLE IF NOT EXISTS {self.schema}.{self.nombre_tabla} (VehicleID INTEGER, TripID INTEGER, RouteID VARCHAR(50), BlockNumber VARCHAR(50), Lat FLOAT, Lon FLOAT, DirectionText VARCHAR(50), TripHeadsign VARCHAR(50), DateTime TIMESTAMP, TripStartTime TIMESTAMP, TripEndTime TIMESTAMP, fecha_carga DATE DEFAULT CURRENT_DATE NOT NULL, hora_carga VARCHAR(8) DEFAULT TO_CHAR(CURRENT_TIMESTAMP, 'HH24:MI:SS') NOT NULL, id VARCHAR(255) NOT NULL, PRIMARY KEY (id));'''
                self.conexion.execute(text(query))
                print('Tabla creada con éxito en AWS Redshift')
            except Exception as e:
                print(f"Error al crear la tabla en AWS: {e}")


    def insertar_datos(self):
        if self.conexion is not None:
            try:
                insert_query = f"INSERT INTO {self.schema}.{self.nombre_tabla} (VehicleID, TripID, RouteID, BlockNumber, Lat, Lon, DirectionText, TripHeadsign, DateTime, TripStartTime, TripEndTime, fecha_carga, hora_carga, id) VALUES"
                valores = []
                for index, row in self.dataframe.iterrows():
                    vehicleid = row['VehicleID']
                    tripid = row['TripID']
                    routeid = row['RouteID']
                    blocknumber = row['BlockNumber']
                    lat = row['Lat']
                    lon = row['Lon']
                    directiontext = row['DirectionText']
                    tripheadsign = row['TripHeadsign']
                    date_time = row['DateTime']
                    tripstarttime = row['TripStartTime']
                    tripendtime = row['TripEndTime']
                    id = row['id']

                    valores.append(f"({vehicleid}, {tripid}, '{routeid.replace('\'', '\'\'')}', '{blocknumber.replace('\'', '\'\'')}', {lat}, {lon}, '{directiontext.replace('\'', '\'\'')}', '{tripheadsign.replace('\'', '\'\'')}', '{date_time}', '{tripstarttime}', '{tripendtime}', CURRENT_DATE, TO_CHAR(CURRENT_TIMESTAMP, 'HH24:MI:SS'), '{id.replace('\'', '\'\'')}')")   # para que no existan errores de 'comillas' usamos replace

                    # unimos los valores de la lista creada para que se incluyan en una sola consulta
                insert_query += ", ".join(valores)
                self.conexion.execute(insert_query)
                print('Datos cargados con éxito')
            except Exception as e:
                print(f"Error al cargar los datos a AWS: {e}")


    def actualizar_datos(self):
        if self.conexion is not None:
            try:
                vehiculo_viaje_ids = self.dataframe[['VehicleID', 'TripID']].values.tolist()   #uso values para convertir ambas columnas a una serie y poder usar tolist(). Esto genera una lista de listas que contendrán valores de vehicleid y tripid posiblemente duplicados
                for vehiculoid, tripid in vehiculo_viaje_ids:
                    # esta query elimina registros posiblemente duplicados de vehiculoid y tripid
                    query_eliminar = f'''DELETE FROM {self.schema}.{self.nombre_tabla} WHERE vehicleid = {vehiculoid} AND tripid = {tripid};'''
                    self.conexion.execute(text(query_eliminar))
                print('Se actualizó correctamente la información')
            except Exception as e:
                print(f'Ocurrió un error al actualizar los vehículoid y tripid: {e}')


    def cerrar_conexion(self):
        if self.conexion:
            try:
                self.conexion.close()
                print('Conexion cerrada')
                return self.conexion
            except Exception as e:
                print(f'Problemas al cerrar la conexion: {e}')
        else:
            print('No hay conexión abierta')




