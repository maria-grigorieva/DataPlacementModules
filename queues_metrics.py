import cx_Oracle
import pandas as pd
import typer


def main(connection: str,
         number_of_days: int = 1):

    occupancy = """
            WITH all_statuses as (
            SELECT NVL(running,0) as running,
                   NVL(defined,0) as defined,
                   NVL(assigned,0) as assigned,
                   NVL(activated,0) as activated,
                   NVL(starting,0) as starting,
                   NVL(transferring,0) as transferring,
                   computingsite as queue,
                   datetime
            FROM (
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
                   'transferring' as transferring
            ))
                    ORDER BY datetime, computingsite),
             rate as (
                 SELECT datetime,
                        queue,
                  round(nvl((running+1)/((activated+assigned+defined+starting+10)*greatest(1,least(2,(assigned/nullif(activated,0))))),0),2) as queue_occupancy
                  FROM all_statuses
             )
            SELECT a.datetime,
                   a.queue,
                   a.running,
                   (a.defined+a.assigned+a.activated+a.starting) as queued,
                   a.transferring,
                   r.queue_occupancy
            FROM all_statuses a
                JOIN rate r ON (a.queue = r.queue)
            WHERE a.activated+a.starting <= 2*a.running
            AND a.defined+a.activated+a.assigned+a.starting <=2*a.running
            ORDER BY r.queue_occupancy DESC
    """

    cx_Oracle.init_oracle_client(lib_dir=r"/usr/lib/oracle/19.3/client64/lib/")

    connection = cx_Oracle.connect(connection)
    cursor = connection.cursor()
    cursor.execute(occupancy, n_days = number_of_days)
    cursor.rowfactory = lambda *args: dict(zip([e[0] for e in cursor.description], args))
    occupancy_result = cursor.fetchall()
    occupancy_df = pd.DataFrame(occupancy_result)
    cols = occupancy_df.columns
    occupancy_df.columns = [i.lower() for i in cols]

    efficiency = """
        SELECT computingsite as queue,
                finished,
                failed,
                NVL(ROUND(finished / NULLIF((finished + failed),0), 4), 0) as queue_efficiency
        FROM
            (SELECT * FROM
                (SELECT computingsite,jobstatus,count( *) as n_jobs
                FROM ATLAS_PANDA.JOBSARCHIVED4
                WHERE modificationtime >= sysdate - :n_days
                AND LOWER(computingsite) NOT LIKE '%test%' 
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
    efficiency_result = cursor.fetchall()
    efficiency_df = pd.DataFrame(efficiency_result)
    cols = efficiency_df.columns
    efficiency_df.columns = [i.lower() for i in cols]

    merged = pd.merge(occupancy_df, efficiency_df,
                      left_on='queue', right_on='queue')
    merged.to_csv('data_samples/queues_metrics.csv')

if __name__ == "__main__":
    typer.run(main)
