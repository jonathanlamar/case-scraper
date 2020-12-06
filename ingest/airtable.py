from dotenv import load_dotenv
from itertools import islice
import json
import os
import requests as rq
import time
import urllib.parse

import pandas as pd

load_dotenv()

AIRTABLE_URI = 'https://api.airtable.com/v0'

headers = {
    "Authorization": f"Bearer {os.environ.get('AIRTABLE_TOKEN')}",
    "Content-Type": "application/json"
}


def process_df(df):
    """
    Process our dataframe from the scraper into batches

    Args:
        df (list): Dataframe received from the scraper
    """
    start = 0
    end = 10
    test_arr = []
    while start < len(df):
        print(len(df))
        pass_arr = []
        for record in islice(df, start, end):
            pass_arr.append(record)
        test_arr.append(pass_arr)
        start += 10
        end += 10
    return test_arr


def create_payloads(batch_dfs):
    """
    Create a list of payloads to be sent off to airtable

    Args:
        batch_dfs (list): A list of dicts, broken down from the scraper dataframe
    """
    payload_arr = []

    for df in batch_dfs:
        post_payload = {
            "records": []
        }
        for record in df:
            obj = {
                "fields": record
            }
            post_payload["records"].append(obj.copy())

        payload_arr.append(post_payload)

    return payload_arr


def create(payload, county):
    """
    Use the Airtable API to create records

    Args:
        payload (string): The base airtable API URI
        county (dict): County info we are processing
        county.name (string): Name of the county
        county.endpoint_id (string): Airtable ID
    """
    POST_URI = f"{AIRTABLE_URI}/{county['endpoint_id']}/Test"
    try:
        r = rq.post(POST_URI, headers=headers, json=payload)
        r.raise_for_status()
        print(f"Successfully added records to {county['name']} base")
    except rq.exceptions.RequestException as e:
        raise SystemExit(e)


def airtable_create(df, county_config):
    """
    Process the dataframe from the scraper and send it to airtable
    """
    df_json = json.loads(df.to_json(orient='records'))
    county_df = process_df(df_json)
    airtable_payloads = create_payloads(county_df)
    for payload in airtable_payloads:
        create(payload, county_config)
        # Airtable limits calls to 5 per second
        time.sleep(0.2)
