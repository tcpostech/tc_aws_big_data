import base64
import io
from datetime import datetime

import boto3
import pandas as pd
import requests

s3 = boto3.client('s3', region_name='sa-east-1')
URL = 'https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetDownloadPortfolioDay/eyJpbmRleCI6IklCT1YiLCJsYW5ndWFnZSI6InB0LWJyIn0='
BUCKET_NAME = "s3-bucket"
DATA = datetime.now().strftime('%Y-%m-%d')
FILE_NAME = f'raw/{DATA}/Dados_B3.parquet'


def lambda_handler(event, context):
    # Uncomment below to visualize all fields from dataframe
    pd.set_option('display.max_rows', None)  # Show all rows
    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.width', None)  # Adjust width to avoid truncation
    pd.set_option('display.colheader_justify', 'center')  # Optional: Center

    response = requests.get(URL)
    decoded_content = base64.b64decode(response.content)
    content = decoded_content.decode('latin-1')
    df = pd.read_csv(io.StringIO(content), delimiter=';', engine='python', encoding='latin-1', index_col=False,
                     skiprows=1)
    df.columns = [col.upper().strip().replace(" ", "_").replace(".", "").replace("(%)", "PP") for col in df.columns]
    df['QTDE_TEÓRICA'] = df['QTDE_TEÓRICA'].apply(lambda x: str(x).replace('.', '').replace(',', '')).astype(int)
    df['DATA_PREGAO'] = DATA
    df['PART_PP'] = df['PART_PP'].str.replace(',', '.').astype(float)
    df = df[~df["CÓDIGO"].isin(["Quantidade Teórica Total", "Redutor"])]

    parquet_file = df.to_parquet(FILE_NAME, engine='pyarrow')
    new_df = pd.read_parquet(FILE_NAME)
    print(new_df.to_json())


lambda_handler(None, None)
