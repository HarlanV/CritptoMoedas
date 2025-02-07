from Connections.RDMS.MySQL import MySQLConnection
from Connections.DW.BigQuery import ExportTableBQ
from Connections.API.Coincap import CoincapAPI
import pandas as pd
from datetime import datetime
import logging
import time

engine = MySQLConnection().get_engine()
gcp_key_path = 'chave-bq.json'
logging.basicConfig(filename='tables.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', filemode='a')

logger = logging.getLogger(__name__)

def persist_table_sql(df:pd.DataFrame, table_name:str, if_exists):
    if df.shape[0] == 0:
        print(f"Tabela {table_name} vazia!")
        pass
    
    df['processing_date'] = datetime.now()
    try:
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        logger.info(f"Tabela {table_name} no banco sql foi atualizada - Sucesso")
        
    except Exception as e:
        logger.info(f"Tabela {table_name} no banco sql teve falha ao ser atualizada - Falha")
        message= f""" Falha ao salvar a tabela {table_name}!!
        Erro:
        {e}
        """
        raise Exception(message)
    

def persist_table_gcp(df:pd.DataFrame, table_name:str, if_exists):

    if df.shape[0] == 0:
        print(f"Tabela {table_name} vazia!")
        pass
    
    df['processing_date'] = datetime.now()

    try:
        ExportTableBQ(gcp_key_path).export_table(df,table_name)
        logger.info(f"Tabela {table_name} no BigQuery foi atualizada - Sucesso")
    except Exception as e:
        logger.info(f"Tabela {table_name} no BigQuery teve falha ao ser atualizada - Falha")

        message= f""" Falha ao salvar a tabela {table_name}!!
        Erro:
        {e}
        """
        raise Exception(message)


def persist_table(df:pd.DataFrame, table_name:str, if_exists='append'):
    return persist_table_sql(df, table_name, if_exists)
    return persist_table_gcp(df, table_name, if_exists)


def import_market():
    api = CoincapAPI()
    
    response = api.get_market()
    df = response['data']
    persist_table(df, "raw_market",'replace')
    print(f"Tabela raw_market armazenada!")


def import_rates():
    api = CoincapAPI()
   
    response = api.get_rates()
    df = response['data']
    persist_table(df, "raw_rates",'replace')
    print(f"Tabela raw_rates armazenada!")


def import_asset_tables():
    api = CoincapAPI()

    # get assets
    response =  api.get_assets()
    df = response['data']
    name_table = "raw_assets"

    if df.shape[0] == 0:
        return False

    persist_table(df, name_table,'replace')
    
    # Gett asset history
    resource = 'history'
    assets_lists = df['id'].unique()
    if_existis = 'replace'
    for asset in assets_lists:
        response = api.get_assets(asset, resource)
        df = response['data']
        name_table = "raw_history"
        if df.shape[0] != 0:
            persist_table(df, name_table, if_existis)
            print(f"Tabela {resource} da {asset} armazenada!")
        else:
            print(f"Tabela {resource} da {asset} vazia!")
        time.sleep(0.5)
        if_existis = 'append'
    
import_asset_tables()
import_rates()
import_market()