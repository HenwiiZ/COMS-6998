# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import urllib
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.parse import urlencode
import boto3
import time
from decimal import Decimal
import os

API_KEY = "Your Yelp API"

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'


# Defaults for our simple example.
DEFAULT_TERM = 'Pizza'
DEFAULT_LOCATION = 'New York, NY'
SEARCH_LIMIT = 50
OFFSET = 0
COUNT = 5000

dynamodb = boto3.resource('dynamodb', region_name='us-east-1',
                          aws_access_key_id='Your AWS Access Key',
                          aws_secret_access_key='Your AWS Secret Access Key')


def request(host, path, api_key, url_params=None):
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search_yelp(api_key, term, location):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT,
        'offset': OFFSET
    }

    url_params = url_params or {}
    url = '{host}{path}'.format(host=API_HOST, path=quote(SEARCH_PATH.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {url} ...'.format(url=url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def get_business(api_key, business_id):
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)

def database(file_name):
    table = dynamodb.Table('yelp-restaurants')
    data_list = []
    with open('data.json', 'r') as f:
        for line in f:
            info = json.loads(line)
            info = json.loads(json.dumps(info), parse_float=Decimal)
            data_list.append(info)

    for i in data_list:
        with table.batch_writer() as batch:
            batch.put_item(i)

def query_api(term, location):
    response = search_yelp(API_KEY, term, location)

    businesses = response.get('businesses')

    if not businesses:
        print(u'No businesses for {0} in {1} found.'.format(term, location))
        return

    f1 = open("data.json", "a+", encoding='utf-8')
    f2 = open("es.json", "a+", encoding='utf-8')
    for i in range(len(businesses)):

        global OFFSET
        OFFSET += 1
        print("OFFSET:", OFFSET)

        business_id = businesses[i]["id"]
        response = get_business(API_KEY, business_id)
        print(response==None)
        print(u'Result for business "{0}" found:'.format(business_id))

        if 'id' not in response.keys():
            continue
        info = {}

        if response['id'] is None or response['id'] == "":
            continue
        info['Business ID'] = response['id']
        if response['name'] is None or response['name'] == "":
            continue
        info['Name'] = response['name']
        if not response['location']:
            continue
        if response['location']['address1'] is not None and response['location']['address1'] != "":
            info['Address'] = response['location']['address1']
        else:
            continue
        if response['location']['address2'] is not None and response['location']['address2'] != "":
            info['Address'] = info['Address'] + response['location']['address2']
        if response['location']['address3'] is not None and response['location']['address3'] != "":
            info['Address'] = info['Address'] + response['location']['address3']

        if not response["coordinates"] :
            continue
        info['Coordinates'] = response["coordinates"]

        if response["review_count"] is None or response["review_count"] == "":
            continue
        info['Number_of_Reviews'] = response["review_count"]

        if response['rating'] is None or response["rating"] == "":
            continue
        info['Rating'] = response['rating']

        if response['location']['zip_code'] is None or response['location']['zip_code'] == "":
            continue
        info['Zip Code'] = response['location']['zip_code']
        info['insertedAtTimestamp'] = time.strftime("%b %d %H:%M:%S %Y", time.localtime()) + " GMT-8"
        global COUNT
        COUNT += 1

        json.dump(info, f1)
        f1.write("\n")
        print(json.dumps(info))

        index_temp = {}
        index_temp['_index'] = "restaurants"
        index_temp['_type'] = "Restaurant"
        index_temp['_id'] = COUNT
        es_index = {}
        es_index['index'] = index_temp
        json.dump(es_index, f2)
        f2.write("\n")
        es_content = {}
        es_content['Business ID'] = response['id']
        es_content['cuisine'] = term
        json.dump(es_content, f2)
        f2.write("\n")
    f1.close()
    f2.close()

def main():
    parser = argparse.ArgumentParser(usage='python yourfile.py --term yourcuisine --location yourlocation \n python yourfile.py -t yourcuisine -l yourlocation')

    parser.add_argument('-t', '--term', dest='term', default=DEFAULT_TERM,
                        type=str, help='Search term (default: %(default)s)')
    parser.add_argument('-l', '--location', dest='location',
                        default=DEFAULT_LOCATION, type=str,
                        help='Search location (default: %(default)s)')

    input_values = parser.parse_args()
    # parser.print_help()
    print("Search Term: {term}, Location: {location}".format(term=input_values.term, location=input_values.location))


    try:
        for i in range(20):
            query_api(input_values.term, input_values.location)
        database('data.json')
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )


if __name__ == '__main__':
    main()
