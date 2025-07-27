import io
import json
import time
from datetime import datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from unidecode import unidecode
from webdriver_manager.chrome import ChromeDriverManager

s3 = boto3.client('s3', region_name='sa-east-1')
URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
BUCKET_NAME = "b3-bucket"
DATA = datetime.now().strftime('%Y-%m-%d')
FILE_NAME = f'raw/{DATA}/Dados_B3.parquet'


def lambda_handler(event, context):
    options = Options()
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(URL)
    time.sleep(5)

    select = Select(driver.find_element(By.ID, "selectPage"))
    select.select_by_visible_text("120")
    time.sleep(5)

    btn_download = driver.find_element(By.LINK_TEXT, 'Download')
    btn_download.click()
    time.sleep(5)

    content = driver.page_source
    driver.quit()

    df = pd.read_html(io.StringIO(content))[0]
    df.columns = [unidecode(col.upper()).strip()
                  .replace(" ", "_")
                  .replace(".", "")
                  .replace("(%)", "PP") for col in df.columns]
    df['QTDE_TEORICA'] = df['QTDE_TEORICA'].apply(lambda x: str(x).replace('.', '').replace(',', '')).astype(int)
    df['PART_PP'] = df['PART_PP'].str.replace(',', '.').astype(float)
    df['DATA_PREGAO'] = DATA
    df = df[~df["CODIGO"].isin(["Quantidade Teórica Total", "Redutor"])]

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