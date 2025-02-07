import os
from dotenv import load_dotenv
import sqlalchemy


class MySQLConnection():

    def __init__(self, user_name:str=None, password:str=None, host_name:str=None, db_name:str=None) -> None:

        connection_configs = {
            'user_name': user_name,
            'password': password,
            'host_name': host_name,
            'db_name': db_name,
        }
        connection_configs = {key:value for key, value in connection_configs.items() if value is not None}
        self.set_credentials(connection_configs)
        pass


    def set_credentials(self, configs:dict[str]) -> None:
        """
        Try to get the values set by the user. If not set, try to get the values from the .env file.
        If not set either, use the default values (root, '', localhost, dw_coincap).
        """
        load_dotenv()
        self.user = configs.get("user_name", os.getenv('MYSQL_USER_NAME','root'))
        self.password = configs.get("password", os.getenv('MYSQL_PASSWORD',''))
        self.host_address = configs.get("host_name", os.getenv('MYSQL_HOST','localhost'))
        self.db_name = configs.get("db_name", os.getenv('MYSQL_DB','dw_coincap'))


    def get_engine(self) -> sqlalchemy.Engine:
        """
        Creates an Engine, without connection (ideal for dataframes)        
        """
        try:
            db_connection_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host_address}/{self.db_name}"
            sql_engine = sqlalchemy.create_engine(db_connection_str)
            return sql_engine
        except Exception as e:
            raise ConnectionError(e)
        

    def forge_connection(self) -> sqlalchemy.Connection:
        """
        Creates a Connection (ideal for explicits sql commands).
        """
        try:
            db_connection_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host_address}/{self.db_name}"
            sql_engine = sqlalchemy.create_engine(db_connection_str)
            db_connection = sql_engine.connect()
            return db_connection
        except Exception as e:
            raise ConnectionError(e)
        
