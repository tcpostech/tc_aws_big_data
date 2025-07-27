import base64
import io
import json
from datetime import datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

s3 = boto3.client('s3', region_name='sa-east-1')
URL = 'https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetDownloadPortfolioDay/eyJpbmRleCI6IklCT1YiLCJsYW5ndWFnZSI6InB0LWJyIn0='
BUCKET_NAME = "s3-scrapper-bucket"
DATA = datetime.now().strftime('%Y-%m-%d')
FILE_NAME = f'raw/{DATA}/Dados_B3.parquet'


def lambda_handler(event, context):
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
    try:
        parquet_file = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(parquet_file, buffer)
        buffer.seek(0)

        s3.put_object(Bucket=BUCKET_NAME, Key=FILE_NAME, Body=buffer.getvalue())
        return {
            'statusCode': 200,
            'body': json.dumps('File saved successfully on S3 Bucket!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
