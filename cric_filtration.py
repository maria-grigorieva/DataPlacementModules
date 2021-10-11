# Sites Filtration service

import typer
import urllib.parse
import requests
import cx_Oracle
import pandas as pd
import datetime as dt
import json
from requests import Session
from requests_pkcs12 import Pkcs12Adapter

def main(grid_cert: str,
         grid_pass: str,
         tls_ca_certificate: str):

    # ssl_cert = '/afs/cern.ch/user/m/mgrigori/.globus/usercert.pem'
    # ssl_key = '/afs/cern.ch/user/m/mgrigori/.globus/userkey.pem'
    #'/etc/ssl/certs/CERN-bundle.pem' | '/etc/ssl/certs/ca-bundle.crt' | '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'
    cric_base_url = 'https://atlas-cric.cern.ch/'
    url_queue = urllib.parse.urljoin(cric_base_url, 'api/atlas/pandaqueue/query/?json')

    with Session() as s:
        s.mount('https://atlas-cric.cern.ch/',
                Pkcs12Adapter(pkcs12_filename=grid_cert,
                              pkcs12_password=grid_pass))
        cric_queues = s.get(url_queue, verify=tls_ca_certificate)

        cric_queues = json.loads(cric_queues.text)
        #cric_queues = requests.get(url_queue, cert=(ssl_cert, ssl_key), verify=tls_ca_certificate).json()
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


if __name__ == '__main__':
    typer.run(main)


