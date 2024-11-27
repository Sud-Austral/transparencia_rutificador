import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import traceback  # Para el manejo y formateo de excepciones

class ConnectionClass:
    """
    Clase para manejar conexiones y consultas a una base de datos PostgreSQL usando SQLAlchemy.

    Métodos:
        __init__(conn_params): Inicializa la conexión con los parámetros proporcionados.
        get_engine(): Crea y devuelve una instancia de SQLAlchemy Engine.
        execute_query(query): Ejecuta una consulta SQL en la base de datos.
        fetch_table(query): Recupera los resultados de una consulta SQL como un DataFrame de pandas.
        close_engine(engine): Cierra la conexión de Engine de SQLAlchemy.
        save_dataframe(df, table_name): Guarda un DataFrame en una tabla de la base de datos.
    """

    def __init__(self, conn_params: dict):
        """
        Inicializa la clase con los parámetros de conexión.

        Args:
            conn_params (dict): Diccionario con los detalles de la conexión.
                                Debe contener las claves 'user', 'password', 'host', y 'dbname'.
        """
        print("Crear objeto")
        self.conn_params = conn_params

    def get_engine(self) -> Engine:
        """
        Crea y devuelve un motor de conexión SQLAlchemy Engine.

        Returns:
            Engine: Una instancia de SQLAlchemy Engine para conectar con PostgreSQL.
        """
        connection_string = (
            f"postgresql+psycopg2://{self.conn_params['user']}:{self.conn_params['password']}"
            f"@{self.conn_params['host']}/{self.conn_params['dbname']}"
        )
        return create_engine(connection_string)

    def execute_query(self, query: str) -> bool:
        """
        Ejecuta una consulta SQL sin devolver resultados, ideal para INSERT, UPDATE, DELETE.

        Args:
            query (str): La consulta SQL a ejecutar.

        Returns:
            bool: True si la consulta se ejecutó exitosamente, False si ocurrió un error.
        """
        engine = self.get_engine()
        success = False
        try:
            with engine.connect() as connection:
                connection.execute(query)
                success = True
                print("Consulta ejecutada exitosamente.")
        except Exception as e:
            print(f"Error al ejecutar la consulta: {e}")
            print(traceback.format_exc())
        finally:
            self.close_engine(engine)
        return success

    def fetch_table(self, query: str) -> pd.DataFrame:
        """
        Recupera los resultados de una consulta SQL en un DataFrame de pandas.

        Args:
            query (str): La consulta SQL SELECT para obtener datos.

        Returns:
            pd.DataFrame: DataFrame con los datos obtenidos de la consulta, o vacío si ocurre un error.
        """
        engine = self.get_engine()
        df = pd.DataFrame()
        try:
            df = pd.read_sql(query, engine)
            #print("Datos recuperados exitosamente.")
        except Exception as e:
            print(f"Error al recuperar datos: {e}")
            print(traceback.format_exc())
        finally:
            self.close_engine(engine)
        return df

    def close_engine(self, engine: Engine):
        """
        Cierra el motor de conexión SQLAlchemy para liberar recursos.

        Args:
            engine (Engine): Instancia de SQLAlchemy Engine a cerrar.
        """
        engine.dispose()
        #print("Conexión cerrada.")

    def save_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Guarda un DataFrame en una tabla de la base de datos.

        Args:
            df (pd.DataFrame): DataFrame a guardar.
            table_name (str): Nombre de la tabla destino.

        Returns:
            None
        """
        engine = self.get_engine()
        try:
            df.to_sql(table_name, engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Ocurrió un error al guardar los datos: {e}")
            print("Traceback detallado:")
            print(traceback.format_exc())
        finally:
            self.close_engine(engine)



    

