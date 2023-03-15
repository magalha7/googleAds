import argparse
import sys
from urllib import response
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import httplib2
import pandas as pd
from datetime import datetime, date
import blob_uploader
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import google.ads.googleads

def get_data_history(client):

    ga_service = client.get_service("GoogleAdsService")

    df = pd.DataFrame()
    new_row = dict()

    query = """
        SELECT
          ad_group.id,
          ad_group.name,
          ad_group_criterion.keyword.text,
          segments.date,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          metrics.all_conversions
        FROM keyword_view 
        WHERE segments.date >= '2021-01-01'
        AND segments.date <= '2022-04-28'
        AND ad_group.status = 'ENABLED'
        AND ad_group_criterion.status IN ('ENABLED', 'PAUSED')
        ORDER BY ad_group.name, ad_group_criterion.keyword.text, segments.date DESC
        """

    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = '1428809378'
    search_request.query = query
    stream = ga_service.search_stream(search_request)
    for batch in stream:
        for row in batch.results:
            new_row['ad_group'] = row.ad_group.name
            new_row['keyword'] = row.ad_group_criterion.keyword.text
            new_row['investment'] = row.metrics.cost_micros
            new_row['click'] = row.metrics.clicks
            new_row['conversion'] = row.metrics.all_conversions
            new_row['impressions'] = row.metrics.impressions
            new_row['date'] = row.segments.date
            df = df.append(new_row, ignore_index=True)

    return df

def get_data(client):


    ga_service = client.get_service("GoogleAdsService")

    df = pd.DataFrame()
    new_row = dict()

    query = """
        SELECT
          ad_group.id,
          ad_group.name,
          ad_group_criterion.keyword.text,
          segments.date,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          metrics.all_conversions
        FROM keyword_view 
        WHERE segments.date DURING YESTERDAY
        AND ad_group.status = 'ENABLED'
        AND ad_group_criterion.status IN ('ENABLED', 'PAUSED')
        ORDER BY ad_group.name, ad_group_criterion.keyword.text, segments.date DESC
        """

    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = '1428809378'
    search_request.query = query
    stream = ga_service.search_stream(search_request)

    for batch in stream:
        for row in batch.results:
            new_row['ad_group'] = row.ad_group.name
            new_row['keyword'] = row.ad_group_criterion.keyword.text
            new_row['investment'] = row.metrics.cost_micros
            new_row['click'] = row.metrics.clicks
            new_row['conversion'] = row.metrics.all_conversions
            new_row['impressions'] = row.metrics.impressions
            new_row['date'] = row.segments.date
            df = df.append(new_row, ignore_index=True)

    return df

def get_offerings(row):

    offering = 'Institucional'

    match row.ad_group:
        case 'Growth e M&A':
            offering = 'Growth Strategy M&A'
        case 'Value Creation':
            offering = 'Value Creation Practices'
        case 'Finance':
            offering = 'Finance'
        case 'Supply Chain':
            offering = 'Supply Chain'
        case 'Digital':
            offering = 'Digital'
        case 'Educação pública':
            offering = 'Educação Pública'
        case 'Insurance':
            offering = 'Insurance'
        case 'Health':
            offering = 'Health'
        case 'Banking':
            offering = 'Banking'

    return offering

def main():

    print(google.ads.googleads.VERSION)
    #client = GoogleAdsClient.load_from_storage("./google_ads.yaml")
    client = GoogleAdsClient.load_from_storage("02.Dados/02.PROD/google_ads.yaml")

    keyword_data = get_data(client)

    keyword_data['offering'] = keyword_data.apply(lambda x: get_offerings(x), axis = 1)

    file_name = str(date.today()) + '_' + 'google_ads.csv'
    blob_uploader.upload_file(keyword_data, file_name, 'googleads')

    file_name = 'google_ads.csv'
    blob_uploader.upload_file(keyword_data, file_name, 'today')

  


if __name__ == '__main__':
    main()
    
