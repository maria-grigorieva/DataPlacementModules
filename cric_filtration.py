# Sites Filtration service
# Return only online sites with DATADISKS
# metadata:
# - sitename
# - cloud
# - tier
# - corepower
# - datadisk

import typer
import urllib.parse
import requests

def main(ssl_cert: str,
         ssl_key: str,
         tls_ca_certificate: str):

    # ssl_cert = '/afs/cern.ch/user/m/mgrigori/.globus/usercert.pem'
    # ssl_key = '/afs/cern.ch/user/m/mgrigori/.globus/userkey.pem'
    '/etc/ssl/certs/CERN-bundle.pem'
    print(ssl_key)
    cric_base_url = 'https://atlas-cric.cern.ch/'
    url_site = urllib.parse.urljoin(cric_base_url, 'api/atlas/site/query/?json')
    cric_sites = requests.get(url_site, cert=(ssl_cert, ssl_key), verify=tls_ca_certificate).json()

    sites_info = []

    for site in cric_sites:
        # Get all DDM endpoints
        ddm_endpoints = [d for d in cric_sites[site]['ddmendpoints']]
        # Get all DATADISKS
        datadisks = [string for string in ddm_endpoints if 'DATADISK' in string]
        # Ignore diskless sites
        if len(datadisks) > 0:
            tmp = {}
            tmp['sitename'] = site
            tmp['cloud'] = cric_sites[site]['cloud']
            tmp['tier'] = cric_sites[site]['tier_level']
            tmp['corepower'] = cric_sites[site]['corepower']
            tmp['datadisk'] = datadisks[0]
            sites_info.append(tmp)

    typer.echo(f'Number of sites, available for replicas creation:{len(sites_info)}')
    return sites_info


if __name__ == '__main__':
    typer.run(main)

