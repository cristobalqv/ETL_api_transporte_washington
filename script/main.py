import os 

from dotenv import load_dotenv
from modulos.utils import Extraer, Cargar

load_dotenv()

url = "https://api.wmata.com/Bus.svc/json/jBusPositions"
api_key = os.getenv('api_key')

credenciales_redshift = {'redshift_user': os.getenv('redshift_user'),
                         'redshift_pass': os.getenv('redshift_pass'),
                         'redshift_host': os.getenv('redshift_host'),
                         'redshift_port': os.getenv('redshift_port'),
                         'redshift_database': os.getenv('redshift_database')}
schema = 'cjquirozv_coderhouse'
nombre_tabla = 'transporte_washington'

if __name__=="__main__":
    extraccion = Extraer(url, api_key)
    extraccion.generar_json()
    
    df = extraccion.df_buses()

    print(extraccion.df_info_posiciones.dtypes)

    carga_df = Cargar(credenciales_redshift, schema, nombre_tabla, df)
    carga_df.crear_motor_conexion()
    carga_df.crear_tabla()
    carga_df.actualizar_datos()
    carga_df.insertar_datos()
    carga_df.cerrar_conexion()