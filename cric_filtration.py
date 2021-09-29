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

    queues_info['datetime'] = dt.datetime.today().strftime("%m-%d-%Y")
    queues_info.to_csv('data_samples/filtered.csv', date_format='%Y-%m-%d')

    return queues_info


if __name__ == '__main__':
    typer.run(main)


