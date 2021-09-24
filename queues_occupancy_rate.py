import cx_Oracle
import pandas as pd
import typer


def main(connection: str,
         number_of_days: int = 1):

    occupancy = """
            WITH all_statuses as (
            SELECT * FROM (
            SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite as queue, jobstatus, count(pandaid) as n_jobs
            FROM ATLAS_PANDA.JOBSACTIVE4
            WHERE modificationtime >= sysdate - :n_days
            GROUP BY TRUNC((sysdate - 1),'DAY'), computingsite, jobstatus
            UNION ALL
            (SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite as queue, jobstatus, count(pandaid) as n_jobs
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
                    ORDER BY datetime, queue),
             rate as (
                 SELECT datetime,
                        queue,
                   ROUND(NVL(running/(defined+activated+starting+assigned),0),6) as queue_occupancy
                  FROM all_statuses
             )
            SELECT a.datetime,
                   a.queue,
                   a.running,
                   a.defined,
                   a.assigned,
                   a.activated,
                   a.starting,
                   a.transferring,
                   a.holding,
                   a.throttled,
                   r.queue_occupancy
            FROM all_statuses a
                JOIN rate r ON (a.queue = r.queue)
            ORDER BY r.queue_occupancy DESC
    """

    cx_Oracle.init_oracle_client(lib_dir=r"/usr/lib/oracle/19.3/client64/lib/")
    connection = cx_Oracle.connect(connection)
    cursor = connection.cursor()
    cursor.execute(occupancy, n_days = number_of_days)
    cursor.rowfactory = lambda *args: dict(zip([e[0] for e in cursor.description], args))
    data = cursor.fetchall()
    occupancy_df = pd.DataFrame(data)
    cols = occupancy_df.columns
    occupancy_df.columns = [i.lower() for i in cols]

    efficiency = """
    SELECT
    computingsite as queue,
    NVL(ROUND(finished / (finished + failed), 4), 0) as queue_efficiency
    FROM
    (SELECT * FROM
    (SELECT computingsite,
     jobstatus,
     count( *) as n_jobs
    FROM
    ATLAS_PANDA.JOBSARCHIVED4
    WHERE modificationtime >= sysdate - :n_days
    GROUP BY computingsite, jobstatus)
    PIVOT
        (
        SUM(n_jobs)
        for jobstatus in ('closed' as closed,
        'cancelled' as cancelled,
        'failed' as failed,
        'finished' as finished
                          ))
    ORDER BY computingsite)
    ORDER BY queue_efficiency DESC
    """

    cursor = connection.cursor()
    cursor.execute(efficiency, n_days = number_of_days)
    cursor.rowfactory = lambda *args: dict(zip([e[0] for e in cursor.description], args))
    data = cursor.fetchall()
    efficiency_df = pd.DataFrame(data)
    cols = efficiency_df.columns
    efficiency_df.columns = [i.lower() for i in cols]

    merged = pd.merge(occupancy_df, efficiency_df,
                      left_on='queue', right_on='queue')

    df = pd.read_csv('data_samples/queue_site_disk.csv', index_col=[0])
    #
    # cric_base_url = 'https://atlas-cric.cern.ch/'
    # url_queues = urllib.parse.urljoin(cric_base_url, 'api/atlas/pandaqueue/query/?json')
    # cric_queues = requests.get(url_queues, cert=(ssl_cert, ssl_key), verify=tls_ca_certificate).json()

    #
    #
    # queue_disk = []
    # for queue in cric_queues:
    #     if 'write_lan' in cric_queues[queue]['astorages']:
    #         write_lan_rse = [i for i in cric_queues[queue]['astorages']['write_lan'] if 'DATADISK' in i]
    #         #write_lan_anal_rse = [i for i in cric_queues[queue]['astorages']['write_lan_analysis'] if 'DATADISK' in i]
    #         queue_disk.append({
    #             'queue': queue,
    #             'rse': list(set(write_lan_rse))
    #         })
    #
    # queue_disk = pd.DataFrame(queue_disk)
    # data = pd.DataFrame(data)
    # data['site'] = data['COMPUTINGSITE'].apply(lambda x: cric_queues[x]['site'] if x in cric_queues else 'unknown')
    # data['cloud'] = data['COMPUTINGSITE'].apply(lambda x: cric_queues[x]['cloud'] if x in cric_queues else 'unknown')
    # data['tier_level'] = data['COMPUTINGSITE'].apply(lambda x: cric_queues[x]['tier_level'] if x in cric_queues else None)
    # data.rename(columns={'COMPUTINGSITE':'queue'}, inplace=True)
    # data.rename(columns={'SITE_OCCUPANCY_RATE':'queue_occupancy_rate'}, inplace=True)
    # # remove TEST queues
    # data = data[~data['queue'].str.contains('TEST')]
    # cols = data.columns
    # data.columns = [c.lower() for c in cols]

    result = pd.merge(merged, df[['queue','site','cloud','rse']], left_on='queue', right_on='queue')
    result = result.explode('rse')
    result.to_csv('data_samples/queues_metrics.csv')


if __name__ == "__main__":
    typer.run(main)
