import requests
import pandas as pd
import numpy as np
import time


def patentsview_query(fields, startdate, enddate, page, per_page, force_retry, time_retry):
    """
    Returns one page of a PatentsView query
    
        Parameters:
            fields (list)     : list of patentsview fields to be downloaded, 
                                see: https://api.patentsview.org/patent.html#field_list
            startdate (str)   : Startdate in format 'YYYY-MM-DD'
            enddate (str)     : Enddate in format 'YYYY-MM-DD', not inclusive
            page (int)        : page number
            per_page (int)    : Results per page (max 10000)
            force_retry (bool): retry automatically when error occurs
            time_retry (int)  : timeout before retrying in seconds
        Returns:
            request.json() (dict): Results from PatentsView query   
    """

    # Set up query parameters
    query = {'_and': [{'_gte': {'patent_date': startdate}}, {'_lt': {'patent_date': enddate}}]}
    options = {'page': page, 'per_page': per_page}
    payload = {'q': query, 'f': fields, 'o': options}

    # Make POST request to API
    retry = True
    while retry:
        request = requests.post('https://api.patentsview.org/patents/query', json=payload)
        if request.status_code == 200:
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
    return request.json()


def get_patentsview_data(fields, startdate, enddate, per_page, force_retry, time_retry):
    """
    Appends all pages of a PatentsView query
    
        Parameters:
            fields (list)     : list of patentsview fields to be downloaded, 
                                see: https://api.patentsview.org/patent.html#field_list
            startdate (str)   : Startdate in format 'YYYY-MM-DD'
            enddate (str)     : Enddate in format 'YYYY-MM-DD', not inclusive
            per_page (int)    : Results per page (max 10000)
            force_retry (bool): retry automatically when error occurs
            time_retry (int)  : timeout before retrying in seconds
        Returns:
            data (dict): Appended results from PatentsView query   
    """

    # Make an initial query to get total count of patents between dates
    initial_query = patentsview_query(['patent_number'],
                                      startdate=startdate,
                                      enddate=enddate,
                                      page=1,
                                      per_page=25,
                                      force_retry=force_retry,
                                      time_retry=time_retry)
    count = initial_query['total_patent_count']
    assert count < 100000, 'Results count: {}, must be less than 100000'.format(count)

    # Request all results by page and append
    data = {'patents': []}
    no_patents = 0
    page = 1
    while no_patents < count:
        query = patentsview_query(fields=fields,
                              startdate=startdate,
                              enddate=enddate,
                              page=page,
                              per_page=per_page,
                              force_retry=force_retry,
                              time_retry=time_retry)
        data['patents'].extend(query['patents'])
        no_patents += per_page
        page += 1
    return data


def patentsvsiew_query_to_dfs(fields, startdate, enddate, per_page=10000, force_retry=False, time_retry=1):
    """
    Queries the patent endpoint of the Patentsview API end transforms the data to proper pandas dataframes
    
        Parameters:
            fields (list)     : list of patentsview fields to be downloaded, 
                                see: https://api.patentsview.org/patent.html#field_list
            startdate (str)   : Startdate in format 'YYYY-MM-DD'
            enddate (str)     : Enddate in format 'YYYY-MM-DD', not inclusive
            per_page (int)    : Results per page (max 10000)
            force_retry (bool): retry automatically when error occurs
            time_retry (int)  : timeout before retrying in seconds, default 1
        Returns:
            dfs (dict): Dictionary of dataframes for each group returned by PatentsView 
    """
    
    if 'patent_number' not in fields:
        fields.append('patent_number')

    # Get all data between dates in dictionary format
    print('Querying: {} to {}...'.format(startdate, enddate))
    data = get_patentsview_data(fields=fields,
                           startdate=startdate,
                           enddate=enddate,
                           per_page=per_page,
                           force_retry=force_retry,
                           time_retry=time_retry)

    # Process to pandas dataframes
    df = pd.DataFrame(data['patents'])
    patents_field_list = [col for col in df.columns if col not in ['inventors', 'rawinventors', 'assignees', 'applications', 'IPCs', 'application_citations', 'cited_patents', 'citedby_patents', 'uspcs', 'cpcs', 'nbers', 'wipos', 'gov_interests', 'lawyers', 'examiners', 'foreign_priority', 'pct_data']]
    explode = [col for col in ['gov_interests', 'inventors', 'rawinventors', 'assignees', 'IPCs', 'application_citations', 'cited_patents', 'citedby_patents', 'uspcs', 'cpcs', 'wipos', 'lawyers', 'examiners', 'foreign_priority', 'pct_data'] if col in df.columns]
    other = [col for col in ['applications', 'nbers'] if col in df.columns]
    dfs = {'patent': df[patents_field_list]}
    for cat in other:
        dfs[cat] = df[['patent_number', cat]]
        dfs[cat] = dfs[cat].join(pd.json_normalize(dfs[cat].explode())).drop(columns=cat)
        if cat == 'applications':
            dfs[cat].drop(columns='app_id', inplace=True)
    for cat in explode:
        dfs[cat] = df[['patent_number', cat]]
        dfs[cat] = dfs[cat].explode(cat).reset_index(drop=True)
        dfs[cat] = dfs[cat].join(pd.json_normalize(dfs[cat][cat])).drop(columns=cat)
        if cat in ['inventors', 'assignees']:
            dfs[cat].drop(columns='{}_key_id'.format(cat[:-1]), inplace=True)
    return dfs
