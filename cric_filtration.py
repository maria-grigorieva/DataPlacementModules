# Sites Filtration service
# Return only online sites with DATADISKS having more than 200 GB free space
# Exclude TEST queues

import typer
import urllib.parse
import requests
import cx_Oracle
import pandas as pd
import datetime as dt

def main(ssl_cert: str,
         ssl_key: str,
         tls_ca_certificate: str):

    # ssl_cert = '/afs/cern.ch/user/m/mgrigori/.globus/usercert.pem'
    # ssl_key = '/afs/cern.ch/user/m/mgrigori/.globus/userkey.pem'
    #'/etc/ssl/certs/CERN-bundle.pem'
    cric_base_url = 'https://atlas-cric.cern.ch/'
    url_queue = urllib.parse.urljoin(cric_base_url, 'api/atlas/pandaqueue/query/?json')
    cric_queues = requests.get(url_queue, cert=(ssl_cert, ssl_key), verify=tls_ca_certificate).json()
    queues_info = []

    for queue,attrs in cric_queues.items():
        # select only disks with write lan permissions
        datadisks = [[d for d in v if 'DATADISK' in d] for k,v in attrs['astorages'].items() if 'write_lan' in k]
        flat_datadisks = list(set([item for sublist in datadisks for item in sublist]))
        if len(flat_datadisks)>0 and 'test' not in queue.lower():
            queues_info.append({
                'queue': queue,
                'site': attrs['rc_site'],
                'rse': flat_datadisks,
                'cloud': attrs['cloud'],
                'tier_level': attrs['tier_level'],
                'corepower': attrs['corepower'],
                'corecount': attrs['corecount'],
                'nodes': attrs['nodes'],
                'transferringlimit': attrs['transferringlimit'] or 2000
            })
    queues_info = pd.DataFrame(queues_info)
    queues_info = queues_info.explode('rse')

    # # get datadisk free space
    # query_disk_size = """
    #                 SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS datadisk,
    #                 round(free/1073741824, 2) as free_GB
    #                 FROM ATLAS_RUCIO.rse_usage
    #                 WHERE source = 'storage'
    #                 AND ATLAS_RUCIO.id2rse(RSE_ID) LIKE '%DATADISK'
    #     """
    # conn = cx_Oracle.connect(oracle_conn_str)
    # cursor = conn.cursor()
    # cursor.execute(query_disk_size)
    # cursor.rowfactory = lambda *args: dict(zip([e[0] for e in cursor.description], args))
    # disk_sizes = cursor.fetchall()
    # disk_sizes = pd.DataFrame(disk_sizes)
    #
    # result = pd.merge(queues_info, disk_sizes, how='left', left_on='rse', right_on='DATADISK')
    # result.drop('DATADISK', 1, inplace=True)
    # result['datetime'] = dt.datetime.today().strftime("%m-%d-%Y")
    # result = result[result['FREE_GB'] > disk_free_size_limit_GB]
    # result.rename(columns={'FREE_GB': 'free_gb'}, inplace=True)
    # typer.echo(f'Number of sites, available for replicas creation:{queues_info.shape}')
    # typer.echo(result)

    queues_info['datetime'] = dt.datetime.today().strftime("%m-%d-%Y")
    queues_info.to_csv('data_samples/filtered.csv', date_format='%Y-%m-%d')

    return queues_info


if __name__ == '__main__':
    typer.run(main)


