"""
Get replicas for a dataset:

Input Parameters:
-----------------
x509_user_proxy: i.e. ~/proxy,
account: <account>,
auth_type: i.e. x509_proxy,
scope: <scope>,
dataset: <dataset name>

Output example:
[{
	'scope': 'mc16_13TeV',
	'name': 'mc16_13TeV.410219.aMcAtNloPythia8EvtGen_MEN30NLO_A14N23LO_ttmumu.deriv.DAOD_HIGG4D2.e5070_e5984_s3126_r10724_r10726_p4133_tid21196172_00',
	'rse': 'MWT2_DATADISK',
	'available_bytes': 143205099517,
	'available_length': 29,
	'created_at': Timestamp('2021-08-28 09:59:51'),
	'updated_at': Timestamp('2021-09-01 01:02:25'),
	'accessed_at': Timestamp('2021-08-28 10:50:10'),
	'length': 36,
	'bytes': 192140394222,
	'state': 'UNAVAILABLE',
	'official': False,
	'timestamp': Timestamp('2021-09-21 22:05:30.039400'),
	'available_GB': 133.37,
	'GB': 178.94,
	'cloud': 'US',
	'site': 'MWT2',
	'tier': '2',
	'freespace': '1717'
},
...]

"""
import pandas as pd
from rucio.client import Client
import os
import typer


def main(x509_user_proxy: str,
         account: str,
         auth_type: str,
         scope: str,
         dataset: str):
    os.environ['X509_USER_PROXY'] = x509_user_proxy
    os.environ['RUCIO_ACCOUNT'] = account
    os.environ['RUCIO_AUTH_TYPE'] = auth_type

    CLIENT = Client()
    replicas = CLIENT.list_dataset_replicas(scope=scope, name=dataset, deep=True)
    replicas = pd.DataFrame(replicas)
    rules = CLIENT.list_replication_rule_full_history(scope=scope, name=dataset)
    official_replicas = [i['rse_expression'] for i in rules if i['rse_expression'] in replicas['rse'].values]
    replicas['official'] = replicas.apply(lambda row: row['rse'] in official_replicas, axis=1)
    replicas = replicas[
        ['scope', 'name', 'rse', 'available_bytes', 'available_length', 'created_at', 'updated_at', 'accessed_at',
         'length', 'bytes', 'state', 'official']]
    replicas['timestamp'] = pd.to_datetime("today")
    replicas['available_GB'] = round(replicas['available_bytes'] / 1073741824, 2)
    replicas['GB'] = round(replicas['bytes'] / 1073741824, 2)

    rse_info = []
    for rse in set(replicas['rse'].values):
        attrs = CLIENT.list_rse_attributes(rse)
        attrs['rse'] = attrs.pop(rse)
        attrs['rse'] = rse
        rse_info.append(attrs)
    rse_info = pd.DataFrame(rse_info)

    rse_info = rse_info[['rse', 'cloud', 'site', 'tier', 'freespace']]
    result = pd.merge(replicas, rse_info, left_on='rse', right_on='rse')
    print(result.to_dict('records'))
    return replicas.to_dict('records')

if __name__ == '__main__':
    typer.run(main)