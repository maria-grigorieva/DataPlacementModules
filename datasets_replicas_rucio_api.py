"""
Get info about all replicas for a list of datasets (datasets from txt file, i.e. datasets.txt)
"""
import pandas as pd
from rucio.client import Client
import os
import typer
import datetime as dt

def main(x509_user_proxy: str,
         account: str,
         auth_type: str,
         input_file: str):

    os.environ['X509_USER_PROXY'] = x509_user_proxy
    os.environ['RUCIO_ACCOUNT'] = account
    os.environ['RUCIO_AUTH_TYPE'] = auth_type

    arr = []

    with open(input_file) as f:
        datasets = [line.rstrip('\n') for line in f]

    CLIENT = Client()
    for d in datasets:
        if ':' in d:
            tmp = d.split(':')
            scope, name = tmp[0], tmp[1]
        else:
            tmp = d.split['.']
            scope, name = tmp[0], d

        replicas = CLIENT.list_dataset_replicas(scope=scope, name=name, deep=True)
        replicas = pd.DataFrame(replicas)
        rules = CLIENT.list_replication_rule_full_history(scope=scope, name=name)
        official_replicas = [i['rse_expression'] for i in rules if i['rse_expression'] in replicas['rse'].values]
        replicas['official'] = replicas.apply (lambda row: row['rse'] in official_replicas, axis=1)
        replicas = replicas[['scope','name','rse','available_bytes','available_length',
                             'created_at','updated_at','accessed_at','length','bytes',
                             'state','official']]
        # TODO:
        # Convert datetime format to 'd-m-Y'
        # datetime_cols = ['created_at','updated_at','accessed_at']

        arr.append(replicas)
    result = pd.concat(arr)
    result['available_TB'] = round(result['available_bytes']/1073741824/1024,4)
    result['TB'] = round(result['bytes'] / 1073741824/1024, 4)
    result.drop(['bytes', 'available_bytes'], axis=1, inplace=True)

    rse_info = []
    for rse in set(result['rse'].values):
        attrs = CLIENT.list_rse_attributes(rse)
        attrs['rse'] = attrs.pop(rse)
        attrs['rse'] = rse
        rse_info.append(attrs)
    rse_info = pd.DataFrame(rse_info)

    rse_info = rse_info[['rse', 'cloud', 'site', 'tier', 'freespace']]
    # freespace is in TERABYTES (primary and secondary replicas)
    rse_info['freespace'] = rse_info['freespace'].apply(pd.to_numeric, errors='coerce')

    info = pd.merge(result, rse_info, left_on='rse', right_on='rse')
    info['datetime'] = dt.datetime.today().strftime("%m-%d-%Y")

    info.to_csv('data_samples/dataset_replicas.csv')
    return info.to_dict('records')

if __name__ == '__main__':
    typer.run(main)