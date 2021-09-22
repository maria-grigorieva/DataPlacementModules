# Sites Filtration service
# Return only online sites with DATADISKS having more than 200 GB free space
# metadata:
# - sitename
# - cloud
# - tier
# - corepower
# - datadisk
# - FREE_GB
# Sample Result:
# -------------------------------------------------------------------------------
#            sitename cloud  tier  corepower                  datadisk    FREE_GB
# 0             AGLT2    US     2     10.960            AGLT2_DATADISK  284157.38
# 1   Australia-ATLAS    CA     2     11.377  AUSTRALIA-ATLAS_DATADISK  146160.12
# 2      BEIJING-LCG2    FR     2     19.113     BEIJING-LCG2_DATADISK   34461.96
# 3         BNL-ATLAS    US     1     12.690         BNL-OSG2_DATADISK  183927.31
# 4            BNLHPC    US     3     12.690           BNLHPC_DATADISK   23496.42
# ..              ...   ...   ...        ...                       ...        ...
# 76     ZA-WITS-CORE    NL     3     10.000     ZA-WITS-CORE_DATADISK   38495.07
# 77             ifae    ES     2     12.167             IFAE_DATADISK   57020.10
# 78              pic    ES     1     12.121              PIC_DATADISK  278164.68
# 79       praguelcg2    DE     2     16.752       PRAGUELCG2_DATADISK  171278.79
# 80    wuppertalprod    DE     2      9.800    WUPPERTALPROD_DATADISK  262586.40

import typer
import urllib.parse
import requests
import cx_Oracle
import pandas as pd

def main(ssl_cert: str,
         ssl_key: str,
         tls_ca_certificate: str,
         oracle_conn_str: str,
         disk_free_size_limit_GB: int = 200):

    # ssl_cert = '/afs/cern.ch/user/m/mgrigori/.globus/usercert.pem'
    # ssl_key = '/afs/cern.ch/user/m/mgrigori/.globus/userkey.pem'
    #'/etc/ssl/certs/CERN-bundle.pem'
    cric_base_url = 'https://atlas-cric.cern.ch/'
    url_site = urllib.parse.urljoin(cric_base_url, 'api/atlas/site/query/?json')
    cric_sites = requests.get(url_site, cert=(ssl_cert, ssl_key), verify=tls_ca_certificate).json()

    sites_info = []

    # CRIC returns only online sites by default
    for site in cric_sites:
        # Get all DDM endpoints
        ddm_endpoints = [d for d in cric_sites[site]['ddmendpoints']]
        # Get all DATADISKS
        datadisks = [string for string in ddm_endpoints if 'DATADISK' in string]
        # Ignore sites without disks
        if len(datadisks) > 0:
            tmp = {}
            tmp['sitename'] = site
            tmp['cloud'] = cric_sites[site]['cloud']
            tmp['tier'] = cric_sites[site]['tier_level']
            tmp['corepower'] = cric_sites[site]['corepower']
            tmp['datadisk'] = datadisks
            sites_info.append(tmp)

    sites_info = pd.DataFrame(sites_info)
    sites_info = sites_info.explode('datadisk')

    # get datadisk free space
    query_disk_size = """
                    SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS datadisk, 
                    round(free/1073741824, 2) as free_GB
                    FROM ATLAS_RUCIO.rse_usage
                    WHERE source = 'storage'
                    AND ATLAS_RUCIO.id2rse(RSE_ID) LIKE '%DATADISK'
        """
    conn = cx_Oracle.connect(oracle_conn_str)
    cursor = conn.cursor()
    cursor.execute(query_disk_size)
    cursor.rowfactory = lambda *args: dict(zip([e[0] for e in cursor.description], args))
    disk_sizes = cursor.fetchall()
    disk_sizes = pd.DataFrame(disk_sizes)

    result = pd.merge(sites_info, disk_sizes, left_on='datadisk', right_on='DATADISK')
    result.drop('DATADISK', 1, inplace=True)
    result['datetime'] = pd.to_datetime('today')
    result = result[result['FREE_GB'] > disk_free_size_limit_GB]
    typer.echo(f'Number of sites, available for replicas creation:{sites_info.shape}')
    typer.echo(result)
    #result.to_csv('filtered.csv', date_format='%Y-%m-%d')
    return result


if __name__ == '__main__':
    typer.run(main)

