import requests
import pandas as pd
import numpy as np
import time
import paths
import csv
from zipfile import ZipFile


def patentsview_query(fields, startdate, enddate, page, per_page, force_retry, time_retry):
    # Query the Patenstview API for dates from startdate up to (not including)
    # enddate, one page only.
    # Input:
    #    - Fields for API to return
    #    - Startdate in format 'YYYY-MM-DD'
    #    - Enddate in format 'YYYY-MM-DD'
    #    - Page number (10000 results per page maximum)
    #    - Results per page (max 10000)
    # Output:
    #    - Dictionary with query results

    # Set up query parameters
    query = {'_and': [{'_gte': {'patent_date': startdate}}, {'_lt': {'patent_date': enddate}}]}
    options = {'page': page, 'per_page': per_page}
    payload = {'q': query, 'f': fields, 'o': options}

    # Make POST request to API
    retry = True
    while retry:
        r = requests.post('https://api.patentsview.org/patents/query', json=payload)
        if r.status_code == 200:
            retry = False
        else:
            print('Error {}, reason: {}'.format(r.status_code, r.reason))
            if force_retry:
                print('Retrying')
                print('Querying: {} to {} in {} seconds...'.format(startdate, enddate, time_retry))
                time.sleep(time_retry)
                continue
            correct_input = False
            while not correct_input:
                key = input('Try again? y/n\n')
                if key == 'y':
                    correct_input = True
                    print('Querying: {} to {}...'.format(startdate, enddate))
                elif key == 'n':
                    raise KeyboardInterrupt
                else:
                    print('Input must be y or n')
    return r.json()


def get_patent_data(fields, startdate, enddate, per_page, force_retry, time_retry):
    # Query the Patenstview API for dates from startdate up to (not including)
    # enddate, append all pages.
    # Input:
    #    - Fields for API to return
    #    - Startdate in format 'YYYY-MM-DD'
    #    - Enddate in format 'YYYY-MM-DD'
    #    - Results per page (max 10000)
    # Output:
    #    - Dictionary with query results

    # Make an initial query to get total count of patents between dates
    initial_query = patentsview_query(['patent_number'],
                                      startdate=startdate,
                                      enddate=enddate,
                                      page=1,
                                      per_page=25,
                                      force_retry=force_retry,
                                      time_retry=time_retry)
    count = initial_query['total_patent_count']
    assert count < 100000, 'Results count: {}'.format(count)

    # Request all results by page and append
    data = {'patents': []}
    no_patents = 0
    page = 1
    while no_patents < count:
        r = patentsview_query(fields=fields,
                              startdate=startdate,
                              enddate=enddate,
                              page=page,
                              per_page=per_page,
                              force_retry=force_retry,
                              time_retry=time_retry)
        data['patents'].extend(r['patents'])
        no_patents += per_page
        page += 1
    return data


def query_to_df(fields, startdate, enddate, per_page=10000, force_retry=False, time_retry=60):
    # Query the Patenstview API for dates from startdate up to (not including)
    # enddate, process them into pandas dataframe.
    # Input:
    #    - Fields for API to return
    #    - Startdate in format 'YYYY-MM-DD'
    #    - Enddate in format 'YYYY-MM-DD'
    #    - Results per page (max 10000)
    #    - Force retry flag
    #         In some cases the API returns a 500 internal server error.
    #         Often, you can just try again. If force_retry=False you
    #         will be asked if you want to retry or not. If force_retry=
    #         True it will try again without asking.
    #    - Time between retries in seconds, only relevant when
    #      force_retry=True
    # Output:
    #    - Dictionary of pandas dataframes
    #        - Patent
    #        - Assignee
    #        - Inventor
    #        - CPC

    # Get all data between dates in dictionary format
    print('Querying: {} to {}...'.format(startdate, enddate))
    data = get_patent_data(fields=fields,
                           startdate=startdate,
                           enddate=enddate,
                           per_page=per_page,
                           force_retry=force_retry,
                           time_retry=time_retry)

    # Process to pandas dataframes
    df = pd.DataFrame(data['patents'])
    for col in ['applications', 'nbers']:
        df[col] = df[col].explode()
    d = {'patent': df[
        ['patent_number', 'patent_year', 'patent_date', 'patent_processing_time', 'patent_average_processing_time',
         'patent_kind', 'patent_type', 'patent_title']].join(pd.json_normalize(df.applications)).join(
        pd.json_normalize(df.nbers)).drop(columns='app_id')}
    for cat in ['assignee', 'inventor', 'cpc']:
        d[cat] = df[['patent_number', '{}s'.format(cat)]]
        d[cat] = d[cat].explode('{}s'.format(cat)).reset_index(drop=True)
        d[cat] = d[cat].join(pd.json_normalize(d[cat]['{}s'.format(cat)])).drop(columns='{}s'.format(cat))
        if cat != 'cpc':
            d[cat].drop(columns='{}_key_id'.format(cat), inplace=True)
    return d