import requests
import pandas as pd
import logging


# Configuração global do logger
logging.basicConfig(filename='api_requests.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', filemode='a')

class CoincapAPI():
    
    def __init__(self, key=None) -> None:
        self.key = key
        self.url="https://api.coincap.io/v2"
        self.logger = logging.getLogger(__name__)


    def get_models_df(self, model_name:str=None) -> dict:
        
        models = {
            'df_assets': {
                'id': str,
                'rank': int,
                'symbol':str,
                'name': str,
                'supply':float,
                'maxSupply' :float,
                'marketCapUsd':float,
                'volumeUsd24Hr' :float,
                'priceUsd':float,
                'changePercent24Hr':float ,
                'vwap24Hr':float,
                'explorer': str
            },
            'df_assets_history': {
                'priceUsd':float,
                'time' :int
            },
            'df_rates':{
                "id": str,
                "symbol": str,
                "currencySymbol": str,
                "type": str,
                "rateUsd":float
            },
            'df_market':{
                "exchangeId": str,
                "rank": int,
                "baseSymbol": str,
                "baseId": str,
                "quoteSymbol": str,
                "quoteId": str,
                "priceQuote": float,
                "priceUsd": float,
                "volumeUsd24Hr": float,
                "percentExchangeVolume":float,
                "tradesCount24Hr": str,
                "updated": int
            },
            'df_candles':
                {
                    "open": float,
                    "high": float,
                    "low": float,
                    "close": float,
                    "volume": float,
                    "period": int
                },

        }

        if model_name:
            return models[model_name]
        return models

    
    def get_loging(self) -> logging:
        """
        Auxiliar to create logs
        """
        return logging.basicConfig(filename='api_requests.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', filemode='a')


    def get_asset_unique(self, asset_name:str) -> tuple[int, str, pd.DataFrame]:
        """
        This endpoint returns an unique asset 
        """

        get_url = f"{self.url}/assets/{asset_name}"
        headers = {
            'Accept-Encoding': 'gzip',  # or 'deflate' (decide later)
        }

        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"
        response = requests.get(get_url, headers=headers)
        logging = self.logger()

        if response.status_code == 200:
            data = response.json()
            assets = data['data']
            df = pd.DataFrame(assets)
            
            logging.info(f"Status Code: {response.status_code} - Sucesso")
            return 200, "Sucess", df
        
        message = f"""
            Erro na requisição: {response.status_code}
            "Resposta:", {response.text}
        """
        logging.error(f"Status Code: {response.status_code} - Request: {get_url} - Resposta: {response.text}")
        return int(response.status_code), message, pd.DataFrame()
        

    def get_list_assets(self) -> tuple[int, str, pd.DataFrame]:
        get_url = f"{self.url}/assets"
        headers = {
            'Accept-Encoding': 'gzip',  # or 'deflate' (decide later)
        }
        
        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"
        
        response = requests.get(get_url, headers=headers)
        logging = self.logger

        if response.status_code == 200:

            data = response.json()
            assets = data['data']
            df = pd.DataFrame(assets)
            df = df.astype(self.get_models_df('df_assets'))
            logging.info(f"Status Code: {response.status_code} - Sucesso")
            return 200, "Sucess", df
        
        message = f"""
            Erro na requisição: {response.status_code}
            "Resposta:", {response.text}
        """
        logging.error(f"Status Code: {response.status_code} - Request: {get_url} - Resposta: {response.text}")
        return int(response.status_code), message, pd.DataFrame()


    def get_asset_history(self, asset_name:str, **kwargs) -> tuple[int, str, pd.DataFrame]:
        """
        This endpoint returns the history of an unique asset.
        kwargs: interval
        """

        args = {
            'interval':kwargs.get('interval', 'h1'),
        }

        get_url = f"{self.url}/assets/{asset_name}/history"

        concat_str = "?"
        for key, value in args.items():
            if value is not None:
                get_url = f"{get_url}{concat_str}{key}={value}"
                concat_str="&"
        
        headers = {
            'Accept-Encoding': 'gzip',  # or 'deflate' (decide later)
        }

        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"
        response = requests.get(get_url, headers=headers)
        logging = self.logger

        if response.status_code == 200:

            data = response.json()
            assets = data['data']
            df = pd.DataFrame(assets)
            
            df = df.astype(self.get_models_df('df_assets_history'))
            
            df["datetime"] = pd.to_datetime(df["time"], unit="ms")
            df["date"] = df["datetime"].dt.date
            df["time"] =  df["datetime"].dt.time
            
            df['asset'] = asset_name
            logging.info(f"Status Code: {response.status_code} - Sucesso")
            return 200, "Sucess", df
        
        message = f"""
            Erro na requisição: {response.status_code}
            "Resposta:", {response.text}
        """
        logging.error(f"Status Code: {response.status_code} - Request: {get_url} - Resposta: {response.text}")
        return int(response.status_code), message, pd.DataFrame()


    def get_asset_markets(self, asset_name:str, **kwargs) -> tuple[int, str, pd.DataFrame]:
        """
        This endpoint returns the history of an unique asset.
        kwargs: interval
        """
        args = {
            'limit':kwargs.get('limit'),
            'offset':kwargs.get('offset')
        }

        get_url = f"{self.url}/assets/{asset_name}/markets"

        concat_str = "?"
        for key, value in args.items():
            if value is not None:
                get_url = f"{get_url}{concat_str}{key}={value}"
                concat_str="&"
        
        headers = {
            'Accept-Encoding': 'gzip',  # or 'deflate' (decide later)
        }

        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"

        response = requests.get(get_url, headers=headers)
        logging = self.logger

        if response.status_code == 200:

            data = response.json()
            assets = data['data']
            df = pd.DataFrame(assets)
            
            logging.info(f"Status Code: {response.status_code} - Sucesso")
            return 200, "Sucess", df
        
        message = f"""
            Erro na requisição: {response.status_code}
            "Resposta:", {response.text}
        """
        self.logger.error(f"Status Code: {response.status_code} - Request: {get_url} - Resposta: {response.text}")
        return int(response.status_code), message, pd.DataFrame()


    def get_assets(self, asset_name:str=None, resource:str=None) -> dict:
        resources = {
            'history': self.get_asset_history,
            'markets': self.get_asset_markets
        }

        if not asset_name:
            status, message, df = self.get_list_assets()
        elif asset_name and not resource:
            status, message, df = self.get_asset_unique(asset_name)
        elif resource in resources:
            status, message, df = resources[resource](asset_name)
        else:
            print(f"{asset_name} e {resource} não processados")
            return {
                "successful": 0,
                'data':pd.DataFrame()
            }

        return {
            "successful": status == 200,
            'data':df
        }


    def get_rates(self, asset_name:str=None, **kwargs) -> tuple[int, str, pd.DataFrame]:
        """
        This endpoint returns the exchange rates (from usd-dollar to another coin).
        kwargs: interval
        """

        get_url = f"{self.url}/rates"

        if asset_name:
            get_url = f"{self.url}/rates/{asset_name}"

        else:
            args = {
                'id':kwargs.get('id', None),
                'symbol':kwargs.get('symbol', None),
                'currencySymbol':kwargs.get('currencySymbol', None),
                'rateUsd':kwargs.get('rateUsd', None),
                'type':kwargs.get('type', None),
            }

            concat_str = "?"
            for key, value in args.items():
                if value is not None:
                    get_url = f"{get_url}{concat_str}{key}={value}"
                    concat_str="&"

        headers = {
            'Accept-Encoding': 'gzip',  # or 'deflate' (decide later)
        }

        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"

        response = requests.get(get_url, headers=headers)
        logging = self.logger

        if response.status_code == 200:

            data = response.json()
            rates = data['data']
            df = pd.DataFrame(rates)
            df = df.astype(self.get_models_df('df_rates'))
            logging.info(f"Status Code: {response.status_code} - Sucesso")
            
            return {
                "successful": True,
                'data':df
            }
            
        logging.error(f"Status Code: {response.status_code} - Request: {get_url} - Resposta: {response.text}")

        return {
            "successful": False,
            'data':pd.DataFrame()
        }


    def get_market(self) -> tuple[int, str, pd.DataFrame]:
        """
        This endpoint get the market values.
        kwargs: interval
        """

        get_url = f"{self.url}/markets"

        headers = {
            'Accept-Encoding': 'gzip',  # or 'deflate' (decide later)
        }

        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"

        response = requests.get(get_url, headers=headers)
        logging = self.logger

        if response.status_code == 200:

            data = response.json()
            rates = data['data']
            df = pd.DataFrame(rates)
            df = df.astype(self.get_models_df('df_market'))
            df["updated"] = pd.to_datetime(df["updated"], unit="ms")
            logging.info(f"Status Code: {response.status_code} - Sucesso")
            
            return {
                "successful": True,
                'data':df
            }
            
        logging.error(f"Status Code: {response.status_code} - Request: {get_url} - Resposta: {response.text}")

        return {
            "successful": False,
            'data':pd.DataFrame()
        }

    # com problemas no retorno da API
    def get_candles(self, **kwargs) -> tuple[int, str, pd.DataFrame]:
        """
        kwargs: exchange, interval,baseId,quoteId, start(optional),end(optional)
        """

        get_url = f"{self.url}/candles"

        args = {
            'exchange':kwargs.get('exchange', 'poloniex'),
            'interval':kwargs.get('interval', 'd1'),
            'baseId':kwargs.get('baseId', 'ethereum'),
            'quoteId':kwargs.get('quoteId', 'bitcoin'),
            'start':kwargs.get('start', None),
            'end':kwargs.get('end', None),
        }

        coin = args['baseId']
        exchanger = args['exchange']
        concat_str = "?"
        for key, value in args.items():
            if value is not None:
                get_url = f"{get_url}{concat_str}{key}={value}"
                concat_str="&"

        headers = {
            'Accept-Encoding': 'gzip',  # or 'deflate' (decide later)
        }

        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"

        response = requests.get(get_url, headers=headers)
        logging = self.logger

        if response.status_code == 200:

            data = response.json()
            rates = data['data']
            df = pd.DataFrame(rates)
            if df.shape[0] == 0:
                return {
                    "successful": True,
                    'data':pd.DataFrame()
                }
            df = df.astype(self.get_models_df('df_candles'))
            df["period"] = pd.to_datetime(df["period"], unit="ms")
            df["base_id"] = coin
            df['exchanger'] = exchanger

            logging.info(f"Status Code: {response.status_code} - Sucesso")
            
            return {
                "successful": True,
                'data':df
            }
            
        logging.error(f"Status Code: {response.status_code} - Request: {get_url} - Resposta: {response.text}")

        return {
            "successful": False,
            'data':pd.DataFrame()
        }


