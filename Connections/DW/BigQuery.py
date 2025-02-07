import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import os
from dotenv import load_dotenv


class ExportTableBQ():
    def __init__(self, key_path:str) -> None:
        self.key_path = key_path
        pass

    def export_table(self, df:pd.DataFrame, table):

        load_dotenv()
        project_id = os.getenv('PROJECT_ID')
        dataset_id = os.getenv('DATASET_ID')
        table_id = table

        table_full_name = f"{project_id}.{dataset_id}.{table_id}"
        credentials = self.get_credentials()
        pandas_gbq.to_gbq(df, table_full_name, project_id=project_id, if_exists='replace', credentials=credentials)
        return True


    def get_credentials(self):
        # Cria credenciais usando a chave de serviço
        credentials = service_account.Credentials.from_service_account_file(
            self.key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return credentials
