import cx_Oracle
import pandas as pd
import typer
import urllib.parse
import requests


def main(ssl_cert: str,
         ssl_key: str,
         tls_ca_certificate: str,
         connection: str,
         number_of_days: int = 1):

    query = """
            WITH all_statuses as (
            SELECT * FROM (
            SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite, jobstatus, count(pandaid) as n_jobs
            FROM ATLAS_PANDA.JOBSACTIVE4
            WHERE modificationtime >= sysdate - :n_days
            GROUP BY TRUNC((sysdate - 1),'DAY'), computingsite, jobstatus
            UNION ALL
            (SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite, jobstatus, count(pandaid) as n_jobs
                FROM ATLAS_PANDA.JOBSDEFINED4
            WHERE modificationtime >= sysdate - :n_days
                GROUP BY TRUNC((sysdate - 1),'DAY'), computingsite, jobstatus
            ))
            PIVOT
            (
               SUM(n_jobs)
               for jobstatus in ('running' as running,
                   'defined' as defined,
                   'assigned' as assigned,
                   'activated' as activated,
                   'starting' as starting,
                   'transferring' as transferring,
                   'holding' as holding,
                   'throttled' as throttled
            ))
                    ORDER BY datetime, computingsite),
             rate as (
                 SELECT datetime,
                        computingsite,
                   ROUND(NVL(running/(defined+activated+starting+assigned),0),6) as site_occupancy_rate
                  FROM all_statuses
             )
            SELECT a.datetime,
                   a.computingsite,
                   a.running,
                   a.defined,
                   a.assigned,
                   a.activated,
                   a.starting,
                   a.transferring,
                   a.holding,
                   a.throttled,
                   r.site_occupancy_rate
            FROM all_statuses a
                JOIN rate r ON (a.computingsite = r.computingsite)
            ORDER BY r.site_occupancy_rate DESC
    """

    cx_Oracle.init_oracle_client(lib_dir=r"/usr/lib/oracle/19.3/client64/lib/")
    connection = cx_Oracle.connect(connection)
    cursor = connection.cursor()
    cursor.execute(query, n_days = number_of_days)
    cursor.rowfactory = lambda *args: dict(zip([e[0] for e in cursor.description], args))
    data = cursor.fetchall()

    cric_base_url = 'https://atlas-cric.cern.ch/'
    url_queues = urllib.parse.urljoin(cric_base_url, 'api/atlas/pandaqueue/query/?json')
    cric_queues = requests.get(url_queues, cert=(ssl_cert, ssl_key), verify=tls_ca_certificate).json()

    queue_disk = []
    for queue in cric_queues:
        if 'write_lan' in cric_queues[queue]['astorages']:
            write_lan_rse = [i for i in cric_queues[queue]['astorages']['write_lan'] if 'DATADISK' in i]
            #write_lan_anal_rse = [i for i in cric_queues[queue]['astorages']['write_lan_analysis'] if 'DATADISK' in i]
            queue_disk.append({
                'queue': queue,
                'rse': list(set(write_lan_rse))
            })

    queue_disk = pd.DataFrame(queue_disk)
    print(queue_disk)

    data = pd.DataFrame(data)
    data['site'] = data['COMPUTINGSITE'].apply(lambda x: cric_queues[x]['site'] if x in cric_queues else 'unknown')
    data['cloud'] = data['COMPUTINGSITE'].apply(lambda x: cric_queues[x]['cloud'] if x in cric_queues else 'unknown')
    data['tier_level'] = data['COMPUTINGSITE'].apply(lambda x: cric_queues[x]['tier_level'] if x in cric_queues else None)
    data.rename(columns={'COMPUTINGSITE':'queue'}, inplace=True)
    data.rename(columns={'SITE_OCCUPANCY_RATE':'queue_occupancy_rate'}, inplace=True)
    # remove TEST queues
    data = data[~data['queue'].str.contains('TEST')]
    cols = data.columns
    data.columns = [c.lower() for c in cols]

    result = pd.merge(data, queue_disk, left_on='queue', right_on='queue')
    result = result.explode('rse')
    print(result.to_dict('records'))
    result.to_csv('data_samples/queue_occupancy_rate.csv')


if __name__ == "__main__":
    typer.run(main)
