import cx_Oracle
import pandas as pd
import typer
import urllib.parse
import requests
import datetime as dt

def main(connection: str,
         ssl_cert: str,
         ssl_key: str,
         tls_ca_certificate: str):

    query = """
WITH all_statuses as (
        SELECT NVL(running,0) as running,
               NVL(defined,0) as defined,
               NVL(assigned,0) as assigned,
               NVL(activated,0) as activated,
               NVL(starting,0) as starting,
               NVL(transferring,0) as transferring,
               NVL(failed,0) as failed,
               NVL(finished,0) as finished,
               (NVL(defined,0)+NVL(assigned,0)+NVL(activated,0)+NVL(starting,0)) as queued,
               (NVL(failed,0)+NVL(finished,0)) as completed,
               computingsite as queue,
               datetime
        FROM (
        SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite, jobstatus, count(pandaid) as n_jobs
        FROM ATLAS_PANDA.JOBSACTIVE4
        WHERE modificationtime >= sysdate - 1 and prodsourcelabel = 'user'
        GROUP BY TRUNC((sysdate - 1),'DAY'), computingsite, jobstatus
        UNION ALL
        (SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite, jobstatus, count(pandaid) as n_jobs
            FROM ATLAS_PANDA.JOBSDEFINED4
        WHERE modificationtime >= sysdate - 1 and prodsourcelabel = 'user'
            GROUP BY TRUNC((sysdate - 1),'DAY'), computingsite, jobstatus
        )
        UNION ALL
        (SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite, jobstatus, count(pandaid) as n_jobs
            FROM ATLAS_PANDA.JOBSARCHIVED4
        WHERE modificationtime >= sysdate - 1 and prodsourcelabel = 'user'
            GROUP BY TRUNC((sysdate - 1),'DAY'), computingsite, jobstatus
        )
            UNION ALL
        (SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite, jobstatus, count(pandaid) as n_jobs
            FROM ATLAS_PANDAARCH.JOBSARCHIVED
        WHERE modificationtime >= sysdate - 1 and prodsourcelabel = 'user'
            GROUP BY TRUNC((sysdate - 1),'DAY'), computingsite, jobstatus
        )
        )
        PIVOT
        (
           SUM(n_jobs)
           for jobstatus in ('running' as running,
               'defined' as defined,
               'assigned' as assigned,
               'activated' as activated,
               'starting' as starting,
               'transferring' as transferring,
               'failed' as failed,
               'finished' as finished
        ))
                ORDER BY datetime, computingsite),
         totals as (
             SELECT SUM(running) as total_jobs_running,
                    SUM(completed) as total_jobs_completed,
                    SUM(queued) as total_jobs_queued,
                    SUM(running)+SUM(completed)+SUM(queued) as total
              FROM all_statuses
         ),
        shares as (SELECT a.queue,
               round(a.running/t.total_jobs_running,6) as running_share,
               round(a.queued/t.total_jobs_queued,6) as queued_share,
               round(a.completed/t.total_jobs_completed, 6) as completed_share,
               round((a.running+a.queued+a.completed)/
                     (t.total),6) as total_share
        FROM all_statuses a, totals t)
        select * from shares
order by total_share desc
    """

    cx_Oracle.init_oracle_client(lib_dir=r"/usr/lib/oracle/19.3/client64/lib/")

    connection = cx_Oracle.connect(connection)
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.rowfactory = lambda *args: dict(zip([e[0] for e in cursor.description], args))
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    print(df)

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
                'tier_level': attrs['tier_level']
            })
    queues_info = pd.DataFrame(queues_info)
    queues_info = queues_info.explode('rse')

    result = pd.merge(df, queues_info, left_on='QUEUE', right_on='queue')

    result['datetime'] = dt.datetime.today().strftime("%m-%d-%Y")
    result.to_csv('data_samples/queue_shares.csv', date_format='%Y-%m-%d')
    print(result)


if __name__ == '__main__':
    typer.run(main)