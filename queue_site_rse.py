import typer
import urllib.parse
import requests
import pandas as pd
import datetime as dt

def main(ssl_cert: str,
         ssl_key: str,
         tls_ca_certificate: str):

    # ssl_cert = '/afs/cern.ch/user/m/mgrigori/.globus/usercert.pem'
    # ssl_key = '/afs/cern.ch/user/m/mgrigori/.globus/userkey.pem'
    #'/etc/ssl/certs/CERN-bundle.pem'
    cric_base_url = 'https://atlas-cric.cern.ch/'
    url_queues = urllib.parse.urljoin(cric_base_url, 'api/atlas/pandaqueue/query/?json')
    cric_queues = requests.get(url_queues, cert=(ssl_cert, ssl_key), verify=tls_ca_certificate).json()

    queues_info = []

    for queue in cric_queues:
        site = cric_queues[queue]['rc_site']
        cloud = cric_queues[queue]['cloud']
        rses = [v for k,v in cric_queues[queue]['astorages'].items()]
        flat_list = [item for sublist in rses for item in sublist]
        flat_list = list(set([i for i in flat_list if 'DATADISK' in i]))
        queues_info.append({
            'queue': queue,
            'site': site,
            'cloud': cloud,
            'rse': flat_list
        })

    df = pd.DataFrame(queues_info)
    df = df.explode('rse')
    df['datetime'] = dt.datetime.today().strftime("%m-%d-%Y")
    df.to_csv('data_samples/queue_site_disk.csv')

    return df


if __name__ == '__main__':
    typer.run(main)

