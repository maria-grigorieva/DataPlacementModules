-- returns only date, sitename and calculated occupancy rate
SELECT datetime,
       computingsite,
       ROUND(NVL(running/(defined+activated+starting+assigned),0),6) as site_occupancy_rate
FROM (
SELECT * FROM (
SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite, jobstatus, count(pandaid) as n_jobs
FROM ATLAS_PANDA.JOBSACTIVE4
WHERE modificationtime >= sysdate - 1
GROUP BY TRUNC((sysdate - 1),'HH24'), computingsite, jobstatus
UNION ALL
(SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite, jobstatus, count(pandaid) as n_jobs
    FROM ATLAS_PANDA.JOBSDEFINED4
    WHERE modificationtime >= sysdate - 1
    GROUP BY TRUNC((sysdate - 1),'HH24'), computingsite, jobstatus
))
PIVOT
(
   SUM(n_jobs)
   for jobstatus in ('running' as running,
       'defined' as defined,
       'assigned' as assigned,
       'activated' as activated,
       'starting' as starting)
)
ORDER BY datetime, computingsite)
ORDER BY site_occupancy_rate DESC;

-- returns calculated occupancy rate with raw data
WITH all_statuses as (
SELECT * FROM (
SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite, jobstatus, count(pandaid) as n_jobs
FROM ATLAS_PANDA.JOBSACTIVE4
WHERE modificationtime >= sysdate - 1
GROUP BY TRUNC((sysdate - 1),'DAY'), computingsite, jobstatus
UNION ALL
(SELECT TRUNC((sysdate - 1),'DAY') as datetime, computingsite, jobstatus, count(pandaid) as n_jobs
    FROM ATLAS_PANDA.JOBSDEFINED4
WHERE modificationtime >= sysdate - 1
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
ORDER BY r.site_occupancy_rate DESC;