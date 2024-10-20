from prefect import flow,task
import gdelt
from datetime import date,timedelta
import polars as pl
from azure.cosmos.aio import CosmosClient
import asyncio
import os

gd2 = gdelt.gdelt(version=2)


@task()
def get_data():
    dateStr = date.today()
    #  - timedelta(1)
    frmt="%Y %m %d"
    dateStr = dateStr.strftime(frmt)

    results = gd2.Search([dateStr],table='gkg',output='pandas dataframe')
   
    main_df = results[["DATE", "V2Counts", "SourceCommonName", "DocumentIdentifier", 
                            "SharingImage","V2Themes","V2Locations","V2Organizations",
                            "V2Persons"]].copy().dropna(axis=0)
    return main_df

@task(log_prints=True)
def transform_data(df):
    df_polars = pl.from_pandas(df)
    df_data = df_polars.select(pl.col('DATE').cast(pl.String).str.slice(0,length=8).str.to_date("%Y%m%d").dt.strftime("%Y-%m-%d").alias('date'),
                           pl.col('SourceCommonName').alias('sourceName'),
                           pl.col('DocumentIdentifier').alias('eventDocument'),
                           pl.col('SharingImage').alias('image'),
                           pl.col('V2Counts').str.split_exact('#',1).struct.rename_fields(['event','eventImportance']).alias('events'),
                           pl.col('V2Locations').str.split_exact('#',6).struct[5].alias('latitude'),
                           pl.col('V2Locations').str.split_exact('#',6).struct[6].alias('longitude'),
                           pl.col('V2Locations').str.split_exact('#',6).struct[2].alias('country'),
                           pl.col('V2Locations').str.split_exact('#',6).struct[1].alias('region'),
                           pl.col('V2Organizations').str.split(';').alias('organizations'),
                           pl.col('V2Persons').str.split(';').alias('persons'),
                           pl.struct("DATE", "DocumentIdentifier",'SourceCommonName').hash().cast(pl.String).alias('uuid')
    ).unnest('events')
    return df_data

@task(log_prints=True)
async def insert_documents(df,ENDPOINT,credential,DATABASE_NAME,CONTAINER_NAME):
    async with CosmosClient(ENDPOINT, credential) as client:
        database = client.get_database_client(DATABASE_NAME)
        container = database.get_container_client(CONTAINER_NAME)
        rows = 0
        for row in df.iter_rows(named=True):
            rows+=1
            await container.upsert_item({
                'id': row['uuid'],
                'date': row['date'],
                'sourceName': row['sourceName'],
                'eventDocument': row['eventDocument'],
                'image': row['image'],
                'event': row['event'],
                'eventImportance': row['eventImportance'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'country': row['country'],
                'region': row['region'],
                'organizations': row['organizations'],
                'persons': row['persons']
            })
            

        print('Upserted Item\'s {0}'.format(rows))

@task(log_prints=True)
async def azure_dump(df):
    # connect to Azure
    ENDPOINT = os.environ["ENDPOINT"]

    credential = os.environ["CREDENTIAL"]

    DATABASE_NAME = "gdelt"
    CONTAINER_NAME = "gdelt"

    await asyncio.gather(insert_documents(df,ENDPOINT,credential,DATABASE_NAME,CONTAINER_NAME))


@flow(log_prints=True)
async def flows():
   df = get_data()
   transform_df = transform_data(df)
   await azure_dump(transform_df)

if __name__ == "__main__":
    asyncio.run(flows())