import gdelt
from datetime import date, timedelta
import polars as pl
from azure.cosmos.aio import CosmosClient
import asyncio
import os
import requests
import extraction
import pandas as pd
import requests
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
from urllib.request import urlopen
from bs4 import BeautifulSoup
import duckdb

gd2 = gdelt.gdelt(version=2)

def get_data():

    URL="http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    r = requests.get(URL)

    zip_file = r.text.split('\n')[2].split(' ')[2]

    with urlopen(zip_file) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall('data')

    header = ['GKGRECORDID',
     'DATE',
     'SourceCollectionIdentifier',
     'SourceCommonName',
     'DocumentIdentifier',
     'Counts',
     'V2Counts',
     'Themes',
     'V2Themes',
     'Locations',
     'V2Locations',
     'Persons',
     'V2Persons',
     'Organizations',
     'V2Organizations',
     'V2Tone',
     'Dates',
     'GCAM',
     'SharingImage',
     'RelatedImages',
     'SocialImageEmbeds',
     'SocialVideoEmbeds',
     'Quotations',
     'AllNames',
     'Amounts',
     'TranslationInfo',
     'Extras']

    file_name = zip_file.split('/')[-1][:-4]
    file_path = f'./data/{file_name}'

    try:
      df = pd.read_csv(file_path,delimiter='\t',names=header)
      
      df = df[[
          "DATE", "V2Counts", "SourceCommonName", "DocumentIdentifier",
          "SharingImage", "V2Themes", "V2Locations", "V2Organizations",
          "V2Persons"
      ]].copy().dropna(axis=0)
      return(df)
    except Exception as e:
      print(f"An unexpected error occurred: {e}")
      return(None)
    
def get_title(url):
  soup = BeautifulSoup(urlopen(url))
  title = soup.title.get_text()
  return(title)

def transform_data(df):
    df_polars = pl.from_pandas(df)
    df_data = df_polars.select(
        pl.col('DATE').cast(pl.String).str.slice(0, length=8).str.to_date(
            "%Y%m%d").dt.strftime("%Y-%m-%d").alias('date'),
        pl.col('SourceCommonName').alias('sourceName'),
        pl.col('DocumentIdentifier').alias('eventDocument'),
        pl.col('SharingImage').alias('image'),
        pl.col('V2Counts').str.split_exact('#', 1).struct.rename_fields(
            ['event', 'eventImportance']).alias('events'),
        pl.col('V2Locations').str.split_exact('#',
                                              6).struct[5].alias('latitude'),
        pl.col('V2Locations').str.split_exact('#',
                                              6).struct[6].alias('longitude'),
        pl.col('V2Locations').str.split_exact('#',
                                              6).struct[2].alias('country'),
        pl.col('V2Locations').str.split_exact('#',
                                              6).struct[1].alias('region'),
        pl.col('V2Organizations').str.split(';').alias('organizations'),
        pl.col('V2Persons').str.split(';').alias('persons'),
        pl.struct("DATE", "DocumentIdentifier",
                  'SourceCommonName').hash().cast(
                      pl.String).alias('uuid')).unnest('events')
    df_data = df_data.with_columns([pl.col('eventDocument').map_elements(get_title,return_dtype=pl.String).alias('title')])
    return df_data


async def insert_documents(df, ENDPOINT, credential, DATABASE_NAME,
                           CONTAINER_NAME):
    async with CosmosClient(ENDPOINT, credential) as client:
        database = client.get_database_client(DATABASE_NAME)
        container = database.get_container_client(CONTAINER_NAME)
        rows = 0
        for row in df.iter_rows(named=True):
            rows += 1
            await container.upsert_item({
                'id':
                row['uuid'],
                'date':
                row['date'],
                'sourceName':
                row['sourceName'],
                'eventDocument':
                row['eventDocument'],
                'image':
                row['image'],
                'event':
                row['event'],
                'eventImportance':
                row['eventImportance'],
                'latitude':
                row['latitude'],
                'longitude':
                row['longitude'],
                'country':
                row['country'],
                'region':
                row['region'],
                'organizations':
                row['organizations'],
                'persons':
                row['persons'],
                'title':
                row['title']
            })

        print('Upserted Item\'s {0}'.format(rows))


async def azure_dump(df):
    # connect to Azure
    ENDPOINT = os.environ["ENDPOINT"]

    credential = os.environ["CREDENTIAL"]

    DATABASE_NAME = "gdelt"
    CONTAINER_NAME = "gdelt"

    await asyncio.gather(
        insert_documents(df, ENDPOINT, credential, DATABASE_NAME,
                         CONTAINER_NAME))
def mother_duck_dump(df):
    con = duckdb.connect(f'md:gdelt_db') 
    for row in df.rows(named=True):
        try:
          title = row['title'].replace("'", "''") if row['title'] else None
          region = row['region'].replace("'", "''") if row['region'] else None
          query = f"""INSERT OR IGNORE INTO gdelt
                    VALUES ({row['uuid']}, 
                            '{row['date']}',
                            '{row['sourceName']}',
                            '{row['eventDocument']}',
                            '{row['image']}',
                            '{row['event']}',
                            {row['eventImportance']},
                            '{row['latitude']}',
                            '{row['longitude']}',
                            '{row['country']}',
                            '{region}',
                            {row['organizations']},
                            {row['persons']},
                          '{title}');"""
          con.sql(query)
        except Exception as e:
          print(f"An unexpected error occurred: {e}")
          print(query)
          break

async def flows():
    df = get_data()
    transform_df = transform_data(df)
    await azure_dump(transform_df)
    mother_duck_dump(transform_df)


async def runs():
    await flows()


if __name__ == "__main__":
    asyncio.run(runs())
