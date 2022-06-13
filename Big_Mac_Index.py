#!/usr/bin/env python
# coding: utf-8

#koniecznym może być instalacja poniższych modułów

#get_ipython().system('pip install nasdaq-data-link')
#get_ipython().system('pip install boto3')
#get_ipython().system('pip install quandl')
#get_ipython().system('pip install wget')

import nasdaqdatalink
import logging
import boto3
import pandas as pd
import quandl
import os
import wget
import requests

path = os.getcwd()

NASDAQ_DATA_LINK_API_KEY = input('wpisz klucz API do pobrania danych z data.nasdaq : ')
AWS_ACCESS_KEY = input('wpisz klucz ID (access key ID) do nawiązania połączenia z usługą Amazon s3 : ')
AWS_SECRET_ACCESS = input('wpisz ukryty klucz (secret access key) do nawiązania połączenia z usługą Amazon s3 : ')
BUCKET_NAME = input("wpisz nazwę bucket'a (folderu do przechowywania danych w s3) : ")


quandl.ApiConfig.api_key = (NASDAQ_DATA_LINK_API_KEY)

url = 'https://static.quandl.com/ECONOMIST_Descriptions/economist_country_codes.csv'
filename = wget.download(url)
df = pd.read_csv(f'{path}\\economist_country_codes.csv')
df = df['COUNTRY|CODE'].str.split('|', expand=True)
countries = df[1]


#usunięcie krajów z wartościmi Nonetype
countries = countries[0:44]
countries.drop(index=20, axis=0, inplace=True)
countries.drop(index=40, axis=0, inplace=True)
countries.reset_index()
countries.drop(columns='index', inplace=True)


df1 = pd.DataFrame()
quandl.ApiConfig.api_key = NASDAQ_DATA_LINK_API_KEY
for x in countries:
    print(x)
    df2 = quandl.get(f'ECONOMIST/BIGMAC_{x}', start_date='2021-07-01', end_date='2021-07-31')
    print(df2)
    df1 = pd.concat([df1,df2])

df1['country'] = str(countries.values)

df1.to_csv(f'{path}\\output.csv', index=False)

OBJECT_NAME_TO_UPLOAD = 'output.csv'

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS
)

#presigned URL
response = s3_client.generate_presigned_post(
    Bucket = BUCKET_NAME,
    Key = OBJECT_NAME_TO_UPLOAD,
    ExpiresIn = 10 
)

print(response)

#udostępnianie pliku do s3 za pomocą presigned URL
files = { 'file': open(OBJECT_NAME_TO_UPLOAD, 'rb')}
r = requests.post(response['url'], data=response['fields'], files=files)
print(r.status_code)

#SCRIPT FOR POWERBI
print()
print("UKOŃCZONO TRANSFER DANYCH. Użyj poniższego kodu do wczytania pliku csv do narzędzia PowerBI:")
print(f"""
import boto3, os, io
import pandas as pd 

my_key = {AWS_ACCESS_KEY}
my_secret= {AWS_SECRET_ACCESS}

my_bucket_name = {BUCKET_NAME}
my_file_path = 'output.csv' 

session = boto3.Session(aws_access_key_id=my_key,aws_secret_access_key=my_secret) 
s3Client = session.client('s3') 
f = s3Client.get_object(Bucket=my_bucket_name, Key=my_file_path) 
bigmac_index = pd.read_csv(io.BytesIO(f['Body'].read()), header=0) """)
print()
input("Naciśnij enter by zakończyć...")

