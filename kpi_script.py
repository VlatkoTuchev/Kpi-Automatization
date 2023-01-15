#### Libraries
import pandas as pd
from datetime import datetime,  timedelta
from datetime import date
import warnings
warnings.filterwarnings("ignore")
from tqdm.notebook import tqdm_notebook
import time

import logging
import requests as rq
import re

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import facebook_business.adobjects
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.campaign import Campaign
from google.ads.googleads.client import GoogleAdsClient

from apiclient.discovery import build 
from oauth2client.service_account import ServiceAccountCredentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import MetricType
from google.analytics.data_v1beta.types import RunReportRequest

from forex_python.converter import CurrencyRates

import requests
import time
from urllib.request import urlopen
import json
from datetime import datetime, timedelta
from operator import itemgetter
from collections import Counter
import pandas as pd

import json
import csv
from google.oauth2 import service_account
import pygsheets

import numpy as np

import os

from credentials import *

# Google Analytics Functions
def ga_response_dataframe(response):
    row_list = []
    # Get each collected report
    for report in response.get('reports', []):
        # Set column headers
        column_header = report.get('columnHeader', {})
        dimension_headers = column_header.get('dimensions', [])
        metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])

        # Get each row in the report
        for row in report.get('data', {}).get('rows', []):
            # create dict for each row
            row_dict = {}
            dimensions = row.get('dimensions', [])
            date_range_values = row.get('metrics', [])

            # Fill dict with dimension header (key) and dimension value (value)
            for header, dimension in zip(dimension_headers, dimensions):
                row_dict[header] = dimension

            # Fill dict with metric header (key) and metric value (value)
            for i, values in enumerate(date_range_values):
                for metric, value in zip(metric_headers, values.get('values')):
                # Set int as int, float a float
                    if ',' in value or '.' in value:
                        row_dict[metric.get('name')] = float(value)
                    else:
                        row_dict[metric.get('name')] = int(value)

            row_list.append(row_dict)
    return pd.DataFrame(row_list)

def print_run_report_response(response):
    list_landing_page = []
    list_pageview = []
    for row in response.rows:
        for dimension_value in row.dimension_values:
            list_landing_page.append(dimension_value.value)

        for metric_value in row.metric_values:
            list_pageview.append(int(metric_value.value))

    return sum(list_pageview)

def run_report(property_id="YOUR-GA4-PROPERTY-ID"):
    """Runs a report of active users grouped by country."""
    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="landingPage")],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date="7daysAgo", end_date="yesterday")],
    )
    response = client.run_report(request)
    return print_run_report_response(response)


###################### Facebook ADS ######################
def FacebookAds():
    my_app_id = my_app_id_facebook
    my_app_secret = my_app_secret_facebook
    my_access_token = my_access_token_facebook

    ## Creating instance of Facebook Ads Profile and getting the campaings
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    my_account = AdAccount(my_account_facebook)
    campaigns = my_account.get_campaigns(fields=[Campaign.Field.name])
    account_number = account_number_facebook

    ## Creating lists 
    adset_land, front_end_list, full_stack_list, adset_land_blogs = [], [], [], []
    academies = academies

    ##### Fields & params
    fields_facebook_ads = ['adset_name','actions','spend']
    params_facebook_ads = {'date_preset':{'last_7d'}}

    for i in tqdm_notebook(range(len(campaigns))):
        # Campaign Name
        try:
            campaign_name = campaigns[i]['name'].split('|')[0].strip()
        except:
            campaign_name = None

        if campaign_name in academies:
            # List of all adset in the campaign
            adsets = Campaign(campaigns[i]['id']).get_ad_sets(fields=['name', 'amount_spent'])
            time.sleep(10)
            for adset in adsets:
                # Checking the limit rate of API Calls so we can pause for certain time for it to refresh    
                check=rq.get('https://graph.facebook.com/v13.0/act_'+account_number+'/insights?access_token='+my_access_token)
                if float(check.headers['x-fb-ads-insights-throttle'].split(':')[-1].split('}')[0].strip()) > 85:
                    time.sleep(100)
                if 'Front' in adset['name']:
                    front_end_list.append(AdSet(adset['id']).get_insights(fields=fields_facebook_ads, params = params_facebook_ads))
                    time.sleep(5)
                elif 'Stack' in adset['name']:
                    full_stack_list.append(AdSet(adset['id']).get_insights(fields=fields_facebook_ads, params = params_facebook_ads))
                    time.sleep(5)
                else:
                    if 'Blogs' in adset['name'] and 'Front' not in adset['name'] and 'Stack' not in adset['name']:
                        adset_land_blogs.append(AdSet(adset['id']).get_insights(fields=fields_facebook_ads, params = params_facebook_ads))
                        time.sleep(5)
                    elif 'Blogs' not in adset['name'] and 'Front' not in adset['name'] and 'Stack' not in adset['name']:
                        adset_land.append(AdSet(adset['id']).get_insights(fields=fields_facebook_ads, params = params_facebook_ads)) 
                        time.sleep(5)

    adset_land = [x for x in adset_land if x]
    adset_land_blogs = [x for x in adset_land_blogs if x]
    full_stack_list = [x for x in full_stack_list if x]
    front_end_list = [x for x in front_end_list if x]

    landing_page_view_list_adset, landing_page_view_list_adset_blogs, list_of_blogs_spend, list_of_spend = [], [], [], []

    for i in range(len(adset_land)):
        list_of_spend.append((adset_land[i][0]['adset_name'].split()[0], adset_land[i][0]['spend']))
        for y in range(len(adset_land[i][0]['actions'])):
            if 'landing_page_view' in adset_land[i][0]['actions'][y].values():
                adset_land[i][0]['adset_name'] = adset_land[i][0]['adset_name'].split()[0]
                landing_page_view_list_adset.append((adset_land[i][0]['adset_name'],list(adset_land[i][0]['actions'][y].values())[1]))

    for i in range(len(adset_land_blogs)):
        list_of_blogs_spend.append((adset_land_blogs[i][0]['adset_name'].split()[0], adset_land_blogs[i][0]['spend']))
        for y in range(len(adset_land_blogs[i][0]['actions'])):   
            if 'landing_page_view' in adset_land_blogs[i][0]['actions'][y].values():
                adset_land_blogs[i][0]['adset_name'] = adset_land_blogs[i][0]['adset_name'].split()[0]
                landing_page_view_list_adset_blogs.append((adset_land_blogs[i][0]['adset_name'],list(adset_land_blogs[i][0]['actions'][y].values())[1]))


    # Full-stack
    landing_page_view_list_fullstack, full_stack_spend, full_stack_spend_blogs, full_stack_blogs  = [], [], [], []

    for i in range(len(full_stack_list)):
        if 'Blogs' in full_stack_list[i][0]['adset_name']:
            full_stack_spend_blogs.append((full_stack_list[i][0]['adset_name'].split('-')[0], full_stack_list[i][0]['spend']))
        else:
            full_stack_spend.append((full_stack_list[i][0]['adset_name'].split()[0], full_stack_list[i][0]['spend']))
        for y in range(len(full_stack_list[i][0]['actions'])):
            if 'landing_page_view' in full_stack_list[i][0]['actions'][y].values():
                if 'Blogs' in full_stack_list[i][0]['adset_name']:
                    full_stack_list[i][0]['adset_name'] = full_stack_list[i][0]['adset_name'].split('-')[0]
                    full_stack_blogs.append((full_stack_list[i][0]['adset_name'],list(full_stack_list[i][0]['actions'][y].values())[1]))
                    pass
                else:
                    full_stack_list[i][0]['adset_name'] = full_stack_list[i][0]['adset_name'].split()[0]
                    landing_page_view_list_fullstack.append((full_stack_list[i][0]['adset_name'],list(full_stack_list[i][0]['actions'][y].values())[1]))

    # Front-end 
    landing_page_view_list_frontend, front_end_list_spend, front_end_blogs, front_end_list_spend_blogs  = [], [], [], []

    for i in range(len(front_end_list)):
        if 'Blogs' in front_end_list[i][0]['adset_name']:
            front_end_list_spend_blogs.append((front_end_list[i][0]['adset_name'].split()[0],front_end_list[i][0]['spend']))
        else:
            front_end_list_spend.append((front_end_list[i][0]['adset_name'].split()[0], front_end_list[i][0]['spend']))
        for y in range(len(front_end_list[i][0]['actions'])):
            if 'landing_page_view' in front_end_list[i][0]['actions'][y].values():
                if 'Blogs' in front_end_list[i][0]['adset_name']:
                    front_end_list[i][0]['adset_name'] = front_end_list[i][0]['adset_name'].split()[0]
                    front_end_blogs.append((front_end_list[i][0]['adset_name'],list(front_end_list[i][0]['actions'][y].values())[1]))
                else:
                    front_end_list[i][0]['adset_name'] = front_end_list[i][0]['adset_name'].split()[0]
                    landing_page_view_list_frontend.append((front_end_list[i][0]['adset_name'],list(front_end_list[i][0]['actions'][y].values())[1]))

    # If there is no data for Front-end (without blogs)
    if landing_page_view_list_frontend == []:
        landing_page_view_list_frontend.append(('Front', 0))

    # If there is no data for Front-end (blogs)
    if front_end_blogs == []:
        front_end_blogs.append(('Front', 0))

    # If there is no data for Full-Stack (without blogs)
    if landing_page_view_list_fullstack == []:
        landing_page_view_list_fullstack.append(('Full', 0))

    # If there is no data for Full-Stack (blogs)
    if full_stack_blogs == []:
        full_stack_blogs.append(('Full', 0))

    # Appending Front and Full in the same list (without blogs)
    for i in range(len(landing_page_view_list_frontend)):
        landing_page_view_list_fullstack.append(landing_page_view_list_frontend[i])

    # Appending Front and Full in the same list (blogs)
    for i in range(len(front_end_blogs)):
        full_stack_blogs.append(front_end_blogs[i])

    # Appending Front and Full in the same list (blogs)
    for i in range(len(front_end_list_spend_blogs)):
        full_stack_spend_blogs.append(front_end_list_spend_blogs[i])

    # Appending Front and Full in the same list (without blogs)
    for i in range(len(front_end_list_spend)):
        full_stack_spend.append(front_end_list_spend[i])

    # Creating dataframe of Front-end and Full-stack (without blogs)
    df_front_full = pd.DataFrame(landing_page_view_list_fullstack, columns=['name', 'Landing_page_views'])
    df_front_full['Landing_page_views'] = df_front_full['Landing_page_views'].astype(int)
    df_front_full = df_front_full.groupby('name').sum().reset_index()

    # Creating dataframe of Front-end and Full-stack (blogs)
    df_front_full_blogs = pd.DataFrame(full_stack_blogs, columns=['name', 'Landing_page_views (blogs)'])
    df_front_full_blogs['Landing_page_views (blogs)'] = df_front_full_blogs['Landing_page_views (blogs)'].astype(int)
    df_front_full_blogs = df_front_full_blogs.groupby('name').sum().reset_index()

    # Merging both dataframes for front and full-stack for pageviews
    df_page_views = pd.merge(df_front_full, df_front_full_blogs, on = 'name')

    # Creating dataframe of Front-end and Full-stack (without blogs) for spend
    df_front_full_spend = pd.DataFrame(full_stack_spend, columns=['name', 'spend'])
    df_front_full_spend['spend'] = df_front_full_spend['spend'].astype(float)
    df_front_full_spend = df_front_full_spend.groupby('name').sum().reset_index()

    # Creating dataframe of Front-end and Full-stack (blogs) for spend
    front_end_list_spend_blogs = pd.DataFrame(full_stack_spend_blogs, columns=['name', 'spend (blogs)'])
    front_end_list_spend_blogs['spend (blogs)'] = front_end_list_spend_blogs['spend (blogs)'].astype(float)
    front_end_list_spend_blogs = front_end_list_spend_blogs.groupby('name').sum().reset_index()

    # Merging both dataframes for front and full-stack for spned 
    df_spend = pd.merge(df_front_full_spend, front_end_list_spend_blogs, on = 'name')

    # Merging them all together
    df_front_full = pd.merge(df_page_views, df_spend, on = 'name').set_index('name')

    # Creating dataframe of all the other academies (landing page views are for the blogs)
    df = pd.DataFrame(landing_page_view_list_adset, columns=['name', 'landing_page_view'])
    df['Landing_page_views'] = df['landing_page_view'].astype(int)
    df_final = df.groupby('name').sum()
    df_final = df_final.sort_values(by=['name'])

    # Creating dataframe of all the other academies (spend are for the blogs)
    df_spend = pd.DataFrame(list_of_spend, columns=['name', 'spend'])
    df_spend['spend'] = df_spend['spend'].astype(float)
    df_final_spend = df_spend.groupby('name').sum()
    df_final_spend = df_final_spend.sort_values(by=['name'])

    df_final_spend = pd.merge(df_final, df_final_spend, on = 'name')

    # Creating dataframe of all the other academies (landing page views are for the blogs)
    df_blogs = pd.DataFrame(landing_page_view_list_adset_blogs, columns=['name', 'landing_page_view (blogs)'])
    df_blogs['Landing_page_views (blogs)'] = df_blogs['landing_page_view (blogs)'].astype(int)
    df_final_blogs = df_blogs.groupby('name').sum()
    df_final_blogs = df_final_blogs.sort_values(by=['name'])

    # Creating dataframe of all the other academies (spend are for the blogs)
    df_spend_blogs = pd.DataFrame(list_of_blogs_spend, columns=['name', 'spend (blogs)'])
    df_spend_blogs['spend (blogs)'] = df_spend_blogs['spend (blogs)'].astype(float)
    df_final_spend_blogs = df_spend_blogs.groupby('name').sum()
    df_final_spend_blogs = df_final_spend_blogs.sort_values(by=['name'])

    # Merging Data Frames
    df_final_spend_blogs = pd.merge(df_final_blogs, df_final_spend_blogs, on = 'name')

    df_final = pd.merge(df_final_spend, df_final_spend_blogs, on = 'name')

    df_final_facebook = df_final.append(df_front_full).reset_index()

    # Renaming the Column names and sorting the values by the Academy column
    df_final_facebook = df_final_facebook.rename({'name':'Academies', 'Landing_page_views': 'FB_Clicks', 'spend':'FB_Budget', 'Landing_page_views (blogs)': 'FB_Clicks (blogs)', 'spend (blogs)': 'FB_Budget (blogs)'}, axis = 1).sort_values(by='Academies')

    fb_budget_pages = list(df_final_facebook['FB_Budget']) 
    fb_budget_blogs = list(df_final_facebook['FB_Budget (blogs)'])

    fb_budget_total = []
    for i in range(len(fb_budget_pages)):
        fb_budget_total.append(fb_budget_pages[i] + fb_budget_blogs[i])

    df_final_facebook['FB_Budget'] = fb_budget_total
    return df_final_facebook

###################### Google ADS ######################
def GoogleADS(facebook_ads_dataframe):
    # Credentials
    os.environ["GOOGLE_ADS_CONFIGURATION_FILE_PATH"] = r"google-ads.yaml"

    client = GoogleAdsClient.load_from_storage()
    ga_service = client.get_service("GoogleAdsService", version="v10")

    # Query for all the information needed from the campaigns
    query = """
            SELECT
              campaign.id,
              segments.date,
              campaign.name,
              metrics.clicks,
              metrics.cost_micros
            FROM ad_group 
            WHERE segments.date DURING LAST_7_DAYS
            ORDER BY campaign.id"""

    # Sending request to Google ADS
    response = ga_service.search_stream(customer_id=customer_id_google, query=query)

    # Creaiting list of all the information from the response (from the campaigns)
    lista_res = []
    try:
        for batch in response:
            for row in batch.results:
                name = row.campaign.name
                clicks = row.metrics.clicks
                cost = row.metrics.cost_micros / 1000000
                lista_res.append((name,clicks,cost))
    except:
        pass

    # Real time currency rates for converting USD to EUR
    c = CurrencyRates()
    cur_value = c.get_rate('USD', 'EUR')

    # Dictionairy for future renaiming columns
    dict_names = dict_names

    # DataFrame of all the information about the campaigns
    df_final_google = pd.DataFrame({'name': [x[0] for x in lista_res],
                  'clicks': [x[1] for x in lista_res],
                  'cost': [round(x[2]*cur_value,2) for x in lista_res]})

    # Grouping by name so we can take the total of clicks and cost
    df_final_google = df_final_google.groupby(['name']).sum().reset_index()

    # Because there is no campaigns for HR, we are adding 0 for both clicks and budget
    df_final_google.loc[len(df_final_google)] = ['HR', 0, 0]

    # Eliminating the campaigns with name containing NEXT
    df_final_google = df_final_google[df_final_google['name'].str.contains('NEXT') == False]

    for key,value in dict_names.items():
        for name in df_final_google['name']:
            if key in name:
                df_final_google.loc[df_final_google['name'].str.contains(key), 'name'] = value

    # Renaiming the columns and sorting by Academies
    df_final_google = df_final_google.rename({'name': 'Academies','clicks': 'Google_clicks', 'cost':'Google_Budget'}, axis = 1).sort_values(['Academies'])

    # Final we are merging the dataframe from google ads with the one from facebook ads 
    final_result_fb_google = pd.merge(facebook_ads_dataframe, df_final_google, on = 'Academies')
    
    return final_result_fb_google

###################### Google Analytics ######################
def Google_Analytics(facebook_google_dataframe):
    # Credentials
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    KEY_FILE_LOCATION = 'google-analytics.json'

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='google-analytics.json'

    credentials = ServiceAccountCredentials.from_json_keyfile_name( 
                KEY_FILE_LOCATION, SCOPES)

    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    # Each academy with its unique view_id
    view_ids = view_ids_google_analytics

    # HR and PPM are with account IDs
    account_ids = account_ids_google_analytics

    blogs_id = blogs_id_google_analytics

    # List of all academies names
    list_academies = list_academies

    # Listing all academies with view_id (pageviews)
    list_pageviews = []

    for key,value in view_ids.items():
        response = analytics.reports().batchGet(body={
            'reportRequests': [{
                'viewId': value,
                'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'yesterday'}],
                'metrics': [
                    {"expression": "ga:pageviews"},
                    {"expression": "ga:avgSessionDuration"}
                ], "dimensions": [
                    {"name": "ga:pagePath"},
                ]
            }]}).execute()

        pd_exm = ga_response_dataframe(response)

        list_pageviews.append(sum(pd_exm['ga:pageviews']))

    # Adding HR and PPM to the list
    for key,value in account_ids.items():
        list_pageviews.append(run_report(value))

    # Creating final dataframe from Google Analytics with Page views
    result = pd.DataFrame({'Academies': list_academies, 'Page views': list_pageviews})

    # Searching with theese name in google analytics
    list_of_blogs = list_of_blogs

    response = analytics.reports().batchGet(body={
        'reportRequests': [{
            'viewId': blogs_id,
            'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'yesterday'}],
            'metrics': [
                {"expression": "ga:pageviews"},
                {"expression": "ga:avgSessionDuration"}
            ], "dimensions": [
                {"name": "ga:pagePath"},
            ],
            'pageSize': 100000
        }]}).execute()

    pd_exm = ga_response_dataframe(response)

    blogs_sum = []
    for blog_name in list_of_blogs:
        sum_of_blogs = []
        for i in range(len(pd_exm)):
            if blog_name == '/code-fe-' and '/codepreneurs' in pd_exm['ga:pagePath'][i]:
                sum_of_blogs.append(int(pd_exm['ga:pageviews'][i]))

            if blog_name == '/code-fs-' and '/code-' in pd_exm['ga:pagePath'][i] and '/code-fe' not in pd_exm['ga:pagePath'][i]:
                sum_of_blogs.append(int(pd_exm['ga:pageviews'][i]))

            if blog_name in pd_exm['ga:pagePath'][i]:
               sum_of_blogs.append(int(pd_exm['ga:pageviews'][i]))
        suma = sum(sum_of_blogs)

        blogs_sum.append(suma)

    data_blogs_organic = pd.DataFrame({
        'Academies': list_academies,
        'Page Views (blogs)': blogs_sum
    })

    g_data_merged = pd.merge(result, data_blogs_organic, on = 'Academies')

    # Concatenating the Data Frame from facebook ADS and Google ADS with Google Analytics
    final_data = pd.merge(facebook_google_dataframe, g_data_merged, on = 'Academies')

    # Getting the paid page views
    paid = []
    for i in range(len(final_data)):
        paid.append(final_data['FB_Clicks'][i] + final_data['Google_clicks'][i])

    # Adding the Paid column
    final_data['Paid'] = paid

    # Paid for blogs (renaming)
    final_data.rename({'FB_Clicks (blogs)': 'Paid (blogs)'}, axis = 1, inplace=True)

    # Getting the organic page views for landing pages
    organic = []
    for i in range(len(final_data)):
        organic.append(final_data['Page views'][i] - final_data['Paid'][i])

    # Adding the Organic column
    final_data['Organic'] = organic

    # Organic page views for blogs
    organic_blogs = []
    for i in range(len(final_data)):
        organic_blogs.append(final_data['Page Views (blogs)'][i] - final_data['Paid (blogs)'][i])

    final_data['Organic (blogs)'] = organic_blogs

    return final_data

###################### Sales Manago ######################
def Sales_Manago(data_facebook_google_gAnalytics):
    # Initiating time frames 
    current_time = datetime.fromtimestamp(int(time.time())).strftime('%d-%m-%Y')
    previous_week = datetime.strptime(current_time, '%d-%m-%Y') - timedelta(7)
    till_yesterday = datetime.strptime(current_time, '%d-%m-%Y')

    list_all_tags_pipeline = lista_tags_pipeline_sales_manago

    # Listing all the tags from the pipeline
    lista_all_tags_general = lista_tags_general_sales_manago

    # Listing enrolment periods for each academy
    list_enrolment_period_ = list_enrolment_period_sales_manago

    # Headers for calling the request from Sales Manago
    headers = {'Content-Type': 'application/json;charset=UTF-8',
               'Accept': 'application/json; charset=UTF-8'}

    # Funnels, owners, tags
    funnel_list_academies = funnel_list_academies_sales_manago

    # Creating list for all the information needed
    list_apps_num, list_interviews_num, list_cont_num, lista_names_apps, lista_all, lista_final, lista_names, lista_stages, list_all_values, list_avg_time_weekly_all, list_avg_time_enr_all = [], [], [], [], [], [], [], [], [], [], []

    # Names of the Academies
    list_academies = list_academies_sales_manago

    all_leads_in_all_funnels = []
    # Going through all the funnels
    for i in tqdm_notebook(range(len(funnel_list_academies))):
        funnel = funnel_list_academies[i]
        # Making the call structure
        json_call = {
              "clientId": client_id_sales_manago,
              "apiKey": api_key_sales_manago,
              "requestTime": int(time.time()),
              "sha": sha_sales_manago,
              "owner": funnel[1],
              "contacts" : [
                    {"addresseeType" : "funnel", "value" : funnel[0]},
                ],
              "data": [
                    {"dataType" : "CONTACT"},
                    {"dataType" : "TAG"},
              ]
            }

        # Making request to Sales Manago
        call = requests.post('https://app3.salesmanago.pl/api/contact/export/data', json=json_call, headers = headers)

        request_id = call.json()['requestId']

        # Getting the call information
        json_request_id = {
          "clientId": client_id_sales_manago,
          "apiKey": api_key_sales_manago,
          "requestTime": int(time.time()),
          "sha": sha_sales_manago,
          "owner": funnel[1],
          "requestId":request_id
        }

        Status = True

        # While Status = true, try to fetch the infromation 
        while Status:
            try:
                request_id_status = requests.post('https://app3.salesmanago.pl/api/job/status', json=json_request_id, headers = headers)
                json_url = request_id_status.json()['fileUrl']
                Status = False
            except:
                pass

        response = urlopen(json_url)

        # storing the JSON response 
        # from url in data
        data_json = json.loads(response.read())

        # Going through all leads in the funnel and storing into one list
        for i in range(len(data_json)):
            all_leads_in_all_funnels.append(data_json[i])

    lista_signed_tags = lista_signed_tags_sales_manago
    lista_applicant_tags = lista_applicant_tags_sales_manago
    list_academies_tags = lista_academies_tags_sales_manago 
 
    # Function for removing duplicates in a list (take a note that this is only for tuples)
    def removeDuplicates(lst):
        return [t for t in (set(tuple(i) for i in lst))]

    list_applications, list_interviews, list_signed, lista_avg_time_weekly, lista_avg_time_enr, list_signed_enr = [], [], [], [], [], []
    # Going through all leads in the funnel
    lista_all_leads = []
    lista_all_leads_test = []
    for i in range(len(all_leads_in_all_funnels)):
        # Sometimes there are None values and we want to filter them
        if all_leads_in_all_funnels[i][list(all_leads_in_all_funnels[i].items())[0][0]]['tagData'] != None and all_leads_in_all_funnels[i][list(all_leads_in_all_funnels[i].items())[0][0]]['contactData'] != None:
            if 'test' not in all_leads_in_all_funnels[i][list(all_leads_in_all_funnels[i].items())[0][0]]['contactData']['name'].lower():
                lista_temp = []
                lista_temp_tags = []
                # Going through all tags for the lead
                for y in range(len(all_leads_in_all_funnels[i][list(all_leads_in_all_funnels[i].items())[0][0]]['tagData'])):
                    try:
                        # Datetime of creation, Contact_name, Tag_name, contact_mail
                        createdOn = datetime.strptime(datetime.fromtimestamp(int(str(all_leads_in_all_funnels[i][list(all_leads_in_all_funnels[i].items())[0][0]]['tagData'][y]['createdOn'])[:10])).strftime('%d-%m-%Y %H:%M:%S'), '%d-%m-%Y %H:%M:%S')
                        contact_name = all_leads_in_all_funnels[i][list(all_leads_in_all_funnels[i].items())[0][0]]['contactData']['name']
                        tag_name = all_leads_in_all_funnels[i][list(all_leads_in_all_funnels[i].items())[0][0]]['tagData'][y]['tagName']
                        contact_email = all_leads_in_all_funnels[i][list(all_leads_in_all_funnels[i].items())[0][0]]['contactData']['email']

                        # Temporary list for the tags of the lead for the avg time in funnel
                        lista_temp_tags.append((createdOn, contact_name, tag_name, contact_email))
                        lista_all_leads_test.append((createdOn.strftime(format='%d-%m-%Y'), contact_name, tag_name))
                        # Listing all who are signed through Sales Manago automatically (Eliminating those from Data Baza Master)
                        if 'SIGNED' in tag_name and 'NOVA_BAZA_MASTER' not in tag_name and 'EXCEL_BASE' not in tag_name:
                            list_signed_enr.append((createdOn, contact_email))

                        # Temporary list of the tags for the lead only for the pipeline
                        if tag_name in list_all_tags_pipeline:
                            lista_temp.append((createdOn, contact_name, tag_name))

                        # Applications
                        if createdOn >= previous_week and createdOn < till_yesterday:
                            if 'APPLICANT' in tag_name and 'OPTIN' not in tag_name and 'NEXT' not in tag_name:
                                list_applications.append((createdOn.strftime('%d-%m-%Y'), contact_name + ' ', tag_name))
                            # Interviews
                            elif 'INTERVIEW_HELD' in tag_name or 'PHONE_INTERVIEW' in tag_name:
                                list_interviews.append((createdOn.strftime('%d-%m-%Y'), contact_name, tag_name))
                            # Contracts
                            elif 'SIGNED' in tag_name and 'NEXT' not in tag_name:
                                list_signed.append((createdOn.strftime('%d-%m-%Y'), contact_name, tag_name))
                                list_signed_weekly.append((createdOn, contact_email))
                    except:
                        pass
                # If the list is empty we want to pass 
                if lista_temp == []:
                    pass
                else:
                    for funnel in funnel_list_academies:
                        temp_funnel_list = []
                        for info in lista_temp:
                            if info[2] in funnel[5]:
                                temp_funnel_list.append(info)
                        try:
    #                         print(temp_funnel_list)
                            lista_all_leads.append((max(temp_funnel_list,key=itemgetter(0))[0].strftime(format='%d-%m-%Y'), max(temp_funnel_list,key=itemgetter(0))[1], max(temp_funnel_list,key=itemgetter(0))[2]))
                        except:
                            pass

        # Average time in funnel 
        for tag in lista_temp_tags:
            for tag_signed_num in range(len(lista_signed_tags)):
                if tag[2] == lista_signed_tags[tag_signed_num] and tag[0] >= previous_week and createdOn < till_yesterday:

                    created_signed = tag[0]
                    email_lead = tag[3]

                    for tag_ in lista_temp_tags:
                        if tag_[2] == lista_applicant_tags[tag_signed_num]:
                            created_tag_applicant = tag_[0]
                            if (created_signed - created_tag_applicant).days < 0:
                                pass
                            else:
                                lista_avg_time_weekly.append(((created_signed - created_tag_applicant).days, email_lead, list_academies_tags[tag_signed_num]))
                elif tag[2] == lista_signed_tags[tag_signed_num] and tag[0] >= datetime.strptime(list_enrolment_period_[tag_signed_num], '%m/%d/%Y'):

                    created_signed = tag[0]
                    email_lead = tag[3]

                    for tag_ in lista_temp_tags:
                        if tag_[2] == lista_applicant_tags[tag_signed_num]:
                            created_tag_applicant = tag_[0]
                            if (created_signed - created_tag_applicant).days < 0:
                                pass
                            else:
                                lista_avg_time_enr.append(((created_signed - created_tag_applicant).days, email_lead, list_academies_tags[tag_signed_num]))


    lista_all_leads = removeDuplicates(lista_all_leads)

    # Avg time in funnel weekly
    data_df_weekly = pd.DataFrame(lista_avg_time_weekly).groupby(2).mean().reset_index()
    data_df_weekly[0] = data_df_weekly[0].astype(int)

    try:
        data_df_weekly = data_df_weekly.set_index(2).loc[list_academies_tags].reset_index()  
    except:
        for academy in list_academies_tags:
            if academy not in list(data_df_weekly[2]):
                data_df_weekly.loc[len(data_df_weekly) + 1] = [academy, 0]

        data_df_weekly = data_df_weekly.set_index(2).loc[list_academies_tags].reset_index()

    avg_time_in_funnel_weekly = list(data_df_weekly[0])

    # Avg time in funnel enrolment
    data_df_enrolment = pd.DataFrame(lista_avg_time_enr).groupby(2).mean().reset_index()
    data_df_enrolment[0] = data_df_enrolment[0].astype(int)

    try:
        data_df_enrolment = data_df_enrolment.set_index(2).loc[list_academies_tags].reset_index()  
    except:
        for academy in list_academies_tags:
            if academy not in list(data_df_weekly[2]):
                data_df_enrolment.loc[len(data_df_enrolment) + 1] = [academy, 0]

        data_df_enrolment = data_df_enrolment.set_index(2).loc[list_academies_tags].reset_index()

    avg_time_in_funnel_enrolment = list(data_df_enrolment[0])

    list_last_7_days, DO_all_enr_period = [], []

    for i in range(len(funnel_list_academies)):
        funnel = funnel_list_academies[i]
        for elem in lista_all_leads:
            enrolment_period = list_enrolment_period_[i]
            enrolment_period_date = datetime.strptime(enrolment_period, '%m/%d/%Y')
            if elem[2] in funnel[5]:
                if datetime.strptime(elem[0], '%d-%m-%Y') >= enrolment_period_date:
                    if datetime.strptime(elem[0], '%d-%m-%Y') >= previous_week and datetime.strptime(elem[0], '%d-%m-%Y') < till_yesterday and 'DO' not in elem[2] and 'UNS' not in elem[2] and 'SIGNED' not in elem[2] and 'POSTPONED' not in elem[2] and 'SCHEDULED' not in elem[2] and 'ACCEPTED' not in elem[2] and 'THINKING_THEM' not in elem[2] and 'CALLED_FOR_DETAILS' not in elem[2] and 'DETAILS_WAITING' not in elem[2] and 'CONTRACT_SENT' not in elem[2] and 'FOR_NEXT_BOOTCAMP' not in elem[2] and 'SENT_TO_ANOTHER_BOOTCAMP' not in elem[2]:
                        list_last_7_days.append(elem)
                    elif datetime.strptime(elem[0], '%d-%m-%Y') < till_yesterday:
                        if 'DO' in elem[2]:
                            DO_all_enr_period.append(elem)
                        elif 'SCHEDULED' in elem[2] or 'UNS' in elem[2] or 'SIGNED' in elem[2] or 'ACCEPTED' in elem[2] or 'THINKING_THEM' in elem[2] or 'POSTPONED' in elem[2] or 'CALLED_FOR_DETAILS' in elem[2] or 'DETAILS_WAITING' in elem[2] or 'CONTRACT_SENT' in elem[2] or 'FOR_NEXT_BOOTCAMP' in elem[2] or 'SENT_TO_ANOTHER_BOOTCAMP' in elem[2]:
                            list_last_7_days.append(elem)

    for i in range(len(DO_all_enr_period)):
        list_last_7_days.append(DO_all_enr_period[i])

    list_all_leads_pipeline = list_last_7_days
    list_all_leads_pipeline = removeDuplicates(list_all_leads_pipeline)

    dict_items = dict(Counter(list(np.array(list_all_leads_pipeline)[:,2])))

    # For those tags that the lead hasn't been through add 0 value
    for academy_tag in list_all_tags_pipeline:
        if academy_tag not in list(dict_items.keys()):
            dict_items[academy_tag] = 0

    dict_items = {k: dict_items[k] for k in list_all_tags_pipeline}

    list_applications = removeDuplicates(list_applications)
    dict_items_apps = dict(Counter(list(np.array(list_applications)[:,2])))
    dict_items_apps = {k: dict_items_apps[k] for k in lista_applicant_tags}
    list_apps_num = list(dict_items_apps.values())

    list_int = []
    list_interviews = removeDuplicates(list_interviews)
    for academy in list_academies_tags:
        for info in list_interviews:
            if academy == 'DS':
                if academy in info[2] and 'DSG' not in info[2]:
                    list_int.append(academy)
            else:
                if academy in info[2]:
                    list_int.append(academy)
    dict_items_int = dict(Counter(list_int))
    list_interviews_num = list(dict_items_int.values())

    list_signed = removeDuplicates(list_signed)
    dict_items_signed = dict(Counter(list(np.array(list_signed)[:,2])))

    # For those tags that the lead hasn't been through add 0 value
    for academy_tag in lista_signed_tags:
        if academy_tag not in list(dict_items_signed.keys()):
            dict_items_signed[academy_tag] = 0

    dict_items_signed = {k: dict_items_signed[k] for k in lista_signed_tags}
    list_cont_num = list(dict_items_signed.values())

    names_apps = pd.DataFrame(list_applications).groupby(2).sum().loc[lista_applicant_tags].reset_index()[[2,1]]
    lista_names_apps = list(names_apps[1])

    list_names_tags = list(dict_items.keys())
    list_values_tags = list(dict_items.values())

    data = pd.DataFrame({
            'Academies': list_academies,
            'APPS': list_apps_num,
            'Names': lista_names_apps,
            'INTERVIEWS': list_interviews_num, 
            'CONTRACTS': list_cont_num,
            'Avg Time Weekly': avg_time_in_funnel_weekly,
            'Avg Time Enrolment': avg_time_in_funnel_enrolment
        })

    # Creating dataframe for the pipeline
    list_pipeline_values = []
    increment = 33
    list_pipeline_values.append(list_values_tags[:increment])
    for i in range(8):
        increment_ = (i+2) * increment
        list_pipeline_values.append(list_values_tags[33*(i+1):increment_])

    data_pipeline = pd.DataFrame(columns = lista_all_tags_general)

    for elem in list_pipeline_values:
        data_pipeline.loc[len(data_pipeline)+1] = elem

    # Taking the sum of the numbers for Total_pipeline and Total_dropOff
    lista_total_pipeline = []
    lista_total_dropoff = []
    for i in range(len(data_pipeline)):
        lista_total_pipeline.append(data_pipeline.loc[:,~data_pipeline.columns.str.contains('DO') & ~data_pipeline.columns.str.contains('CR10_MKD__SIGNED')].iloc[i].sum())
        lista_total_dropoff.append(data_pipeline.loc[:,data_pipeline.columns.str.contains('DO') | data_pipeline.columns.str.contains('CR10_MKD__SIGNED')].iloc[i].sum())

    # Adding the numbers in the pipeline dataframe
    data_pipeline['Total DropOFF'] = lista_total_dropoff
    data_pipeline['Total Pipeline'] = lista_total_pipeline

    # Adding the Academies names in the pipeline dataframe so we can merge the two dataframes
    data_pipeline['Academies'] = list_academies

    # Merging data with data_pipeline
    data_final_ = pd.merge(data, data_pipeline, on = 'Academies')

    # Merging the main dataframe with the dataframe from Sales Manago
    final_data = pd.merge(data_facebook_google_gAnalytics, data_final_, on = 'Academies')

    # Changing the names of the Academies
    final_data['Academies'] = final_data['Academies'].replace({'DIZ': 'Graphic Design', 'DS': 'Data Science', 'Front': 'Front-end', 'Full': 'Full-stack', 'UX': 'UX/UI Design', 'QA': 'Software testing'})

    return final_data

def Google_Sheets_insert(final_data):
    ##################################################################
    # Authorization
    client = pygsheets.authorize(service_file=r"google-analytics.json")

    # Url of the document on google drive
    spreadsheet_url = spreadsheet_url_google
    test = spreadsheet_url.split('/d/')
    id_ = test[1].split('/edit')[0]

    # Opening the document
    sheet_by_key = client.open_by_key(id_)
    sheet_by_url = client.open_by_url(spreadsheet_url)

    # List of sheets name
    list_of_sheets = list_of_sheets

    # Open the sheet with GD name
    wks = sheet_by_key.worksheet_by_title('GD')

    # Taking the columns name of the sheet
    columns_data = list(wks.get_as_df().columns)

    # Creating data frame with those columns name
    data = pd.DataFrame(columns = columns_data)

    # List of enrolment periods
    lista_total_con = []
    lista_total_ep = []

    list_dates, list_end_period, start_enr_period, list_cpl, list_interviews, list_contracts, list_cac, list_pvs_aps, list_aps_int, list_int_cont, list_total_budget, list_enr_period, list_avg_weekly, list_avg_enrolment  = [], [], [], [], [], [], [], [], [], [], [], [], [], []
    # Going through all the rows in final_data dataframe
    for i in range(len(final_data)):
        total_budget = round(float(final_data['FB_Budget'][i]) + float(final_data['Google_Budget'][i]), 2)

        list_dates.append(datetime.fromtimestamp(int(time.time())).strftime('%d/%m/%Y'))
        list_cpl.append(total_budget / final_data['APPS'][i])
        list_interviews.append(final_data['INTERVIEWS'][i])
        list_contracts.append(final_data['CONTRACTS'][i])
        list_pvs_aps.append(str(round(final_data['APPS'][i] / final_data['Page views'][i] * 100, 2)) + '%')
        list_aps_int.append(str(round(final_data['INTERVIEWS'][i] / final_data['APPS'][i] * 100, 2)) + '%')
        list_int_cont.append(str(round(final_data['CONTRACTS'][i] / final_data['INTERVIEWS'][i] * 100, 2)) + '%')
        list_total_budget.append(total_budget)

    # Creating new columns in final_data
    final_data['date'] = list_dates
    final_data['CPL'] = list_cpl
    final_data['Interviews'] = list_interviews
    final_data['Contracts'] = list_contracts
    final_data['PVS/APS'] = list_pvs_aps
    final_data['APS/INT'] = list_aps_int
    final_data['INT/CONT'] = list_int_cont
    final_data['Total Budget'] = list_total_budget
    final_data['Avg.time in funnel (Weekly)'] = final_data['Avg Time Weekly']
    final_data['Avg.time in funnel (Enrollment period)'] = final_data['Avg Time Enrolment']

    # Going through all the sheets to take the number of total contracts and total ep
    for i in range(len(list_of_sheets)):
        academy = list_of_sheets[i]
        wks = sheet_by_key.worksheet_by_title(academy)

        ### Getting total contr and total ep from the spreadsheet
        data_ = wks.get_as_df().set_index('DATE')
        list_end_period.append(data_['End Enr Period'][-1])
        start_enr_period.append(data_['Start Enr Period'][-1])

        try:
            total_ep = str(int(final_data['Total Budget'][i]) + int(data_.iloc[-1]['TOTAL EP'].replace(',','').split('€')[0])) + '€'
            total_con = int(str(list_contracts[i]).strip()) + int(data_.iloc[-1]['Tot.Contr.'])

            lista_total_ep.append(total_ep)
            lista_total_con.append(total_con)
        except:
            if data_['Start Enr Period'][-1] == final_data['date'][i]:  
                total_ep = str(int(final_data['Total Budget'][i])) + '€'
                lista_total_con.append(list_contracts[i])
                lista_total_ep.append(total_ep)
            else:
                total_ep = str(int(final_data['Total Budget'][i]) + int(data_.iloc[-1]['TOTAL EP'].replace(',','').split('€')[0])) + '€'
                lista_total_con.append(list_contracts[i])
                lista_total_ep.append(total_ep)

    # List of CAC
    for i in range(len(final_data)):
        try:
            list_cac.append(int(lista_total_ep[i].split('€')[0]) / lista_total_con[i])
        except:
            list_cac.append(0)

    # Adding new columns 
    final_data['Total EP'] = lista_total_ep
    final_data['Total Contracts'] = lista_total_con
    final_data['CAC'] = list_cac

    lista_dates_start_enr = []
    for date in start_enr_period:
        date_splitted = date.split('/')

        date = date_splitted[1] + '/' + date_splitted[0] + '/' + date_splitted[2]
        lista_dates_start_enr.append(date)

    lista_dates_end_enr = []
    for date in list_end_period:
        date_splitted = date.split('/')

        date = date_splitted[1] + '/' + date_splitted[0] + '/' + date_splitted[2]
        lista_dates_end_enr.append(date)


    final_data['Start Enr Period'] = lista_dates_start_enr
    final_data['End Enr Period'] = lista_dates_end_enr

    # Reordering the columns
    final_data = final_data[['date', 'Page views', 'Organic', 'FB_Clicks', 'Google_clicks', 'Paid', 'Paid (blogs)', 'Organic (blogs)', 'APPS', 'Names', 'CPL', 'Interviews', 'Contracts', 'Total Contracts', 'CAC', 'PVS/APS', 'APS/INT', 'INT/CONT', 'FB_Budget', 'Google_Budget', 'Total Budget', 'Total EP', 'Academies', 'Start Enr Period', 'End Enr Period', 'Avg.time in funnel (Weekly)', 'Avg.time in funnel (Enrollment period)', 'Total Pipeline', 'Total DropOFF', 'APPLIED', 'DO1_MKD_ACADEMY', 'CR2_MKD__UNS_1', 'MKD__UNS_2', 'MKD__UNS_3', 'DO2_MKD_', 'CR3_MKD__POSTPONED', 'MKD__POST_UNS_1', 'MKD__POST_UNS_2', 'DO3_MKD_', 'CR3_MKD__PHONE_INTERVIEW_1', 'CR5_MKD__PHONE_INTERVIEW_2', 'CR4_MKD__SCHEDULED', 'CR5_MKD__INTERVIEW_HELD', 'MKD__NOSHOW', 'MKD__NOSHOW_MESSAGE', 'DO4_MKD_', 'DO5_MKD_', 'MKD__ACCEPTED', 'MKD__THINKING_THEM', 'MKD__THINKING_US', 'CR6_MKD__CALLED_FOR_DETAILS', 'CR6_MKD__DETAILS_WAITING', 'DO6_MKD_', 'CR7_MKD__DETAILS_TAKEN', 'DO7_MKD_', 'DO8_MKD_', 'CR9_MKD__CONTRACT_SENT', 'DO9_MKD_', 'CR10_MKD__SIGNED', 'DO10_MKD_', 'MKD__SENT_TO_ANOTHER_BOOTCAMP', 'MKD__SIGNED_FOR_NEXT_BOOTCAMP']]
    # Dictionairy for all the information from final_data
    data_academies = { 
        'GD': [i for i in list(final_data.iloc[0])],
        'DS': [i for i in list(final_data.iloc[1])],
        'FE': [i for i in list(final_data.iloc[2])],
        'FS': [i for i in list(final_data.iloc[3])],
        'HR': [i for i in list(final_data.iloc[4])],
        'MKT': [i for i in list(final_data.iloc[5])],
        'PPM': [i for i in list(final_data.iloc[6])],
        'QA': [i for i in list(final_data.iloc[7])],
        'UX/UI': [i for i in list(final_data.iloc[8])]
    }

    ######## Writing into google sheets ########   
    data = pd.DataFrame(columns = columns_data)

    # Inserting data into google sheet
    for i in range(len(list_of_sheets)):
        academy = list_of_sheets[i]
        wks = sheet_by_key.worksheet_by_title(academy)

        for key,value in data_academies.items():

            if academy == key:
                wks.insert_rows(wks.rows, values=[str(i) for i in value], inherit=True)
                pass

        data = data.append(wks.get_as_df())

    # Creating new sheet 
    try:
        sheet_by_key.worksheet_by_title('Combined')
        worksheet_exist = True
    except:
        worksheet_exist = False

    # Creating the sheet with all the columns from the dataframe
    if worksheet_exist:
        wks_to_delete = sheet_by_key.worksheet_by_title('Combined')
        sheet_by_key.del_worksheet(wks_to_delete)
        sheet_by_key.add_worksheet('Combined', rows = data.shape[0], cols = data.shape[1])
    else:
        sheet_by_key.add_worksheet('Combined', rows = data.shape[0], cols = data.shape[1])

    new_wks = sheet_by_key.worksheet_by_title('Combined')

    lista_dates = []
    for date in data['DATE']:
        try:
            date = pd.to_datetime(date, format='%d/%m/%Y')
            lista_dates.append(date)
        except:
            date_splitted = date.split('/')

            date = date_splitted[1] + '/' + date_splitted[0] + '/' + date_splitted[2]
            date = pd.to_datetime(date, format='%d/%m/%Y')
            lista_dates.append(date)

    data['DATE'] = lista_dates

    # # Insert into google sheet 
    new_wks.set_dataframe(data,(1,1),copy_index=False)
    
def update_combined_sheet():
    ##################################################################
    # Authorization
    client = pygsheets.authorize(service_file=r"google-analytics.json")

    # Url of the document on google drive
    spreadsheet_url = spreadsheet_url_google
    test = spreadsheet_url.split('/d/')
    id_ = test[1].split('/edit')[0]

    # Opening the document
    sheet_by_key = client.open_by_key(id_)
    sheet_by_url = client.open_by_url(spreadsheet_url)

    # List of sheets name
    list_of_sheets = list_of_sheets

    # Open the sheet with GD name
    wks = sheet_by_key.worksheet_by_title('GD')

    # Taking the columns name of the sheet
    columns_data = list(wks.get_as_df().columns)

    # Creating data frame with those columns name
    data = pd.DataFrame(columns = columns_data)
    
    # Inserting data into google sheet
    for i in range(len(list_of_sheets)):
        academy = list_of_sheets[i]
        wks = sheet_by_key.worksheet_by_title(academy)

        data = data.append(wks.get_as_df())

    # Creating new sheet 
    try:
        sheet_by_key.worksheet_by_title('Combined')
        worksheet_exist = True
    except:
        worksheet_exist = False

    # Creating the sheet with all the columns from the dataframe
    if worksheet_exist:
        wks_to_delete = sheet_by_key.worksheet_by_title('Combined')
        sheet_by_key.del_worksheet(wks_to_delete)
        sheet_by_key.add_worksheet('Combined', rows = data.shape[0], cols = data.shape[1])
    else:
        sheet_by_key.add_worksheet('Combined', rows = data.shape[0], cols = data.shape[1])

    new_wks = sheet_by_key.worksheet_by_title('Combined')

    lista_dates = []
    for date in data['DATE']:
        try:
            date = pd.to_datetime(date, format='%d/%m/%Y')
            lista_dates.append(date)
        except:
            date_splitted = date.split('/')

            date = date_splitted[1] + '/' + date_splitted[0] + '/' + date_splitted[2]
            date = pd.to_datetime(date, format='%d/%m/%Y')
            lista_dates.append(date)

    data['DATE'] = lista_dates

    # # Insert into google sheet 
    new_wks.set_dataframe(data,(1,1),copy_index=False)

data_facebook = FacebookAds()
data_fb_google = GoogleADS(data_facebook)
data_fb_google_gAnalytics = Google_Analytics(data_fb_google)
data_final = Sales_Manago(data_fb_google_gAnalytics)
Google_Sheets_insert(data_final)

# update_combined_sheet()