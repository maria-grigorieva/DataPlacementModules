-- weighted queueing time per PQ
-- queue
-- queue_time_avg (seconds)
-- queue_time_max
-- queue_time_min
-- queue_time_median
-- n_jobs
-- weighted_avg_queue_time
-- weighted_median_queue_time
with a as (
  select queue,
       round(avg(lag)) as queue_time_avg,
       max(lag) as queue_time_max,
       min(lag) as queue_time_min,
       round(median(lag)) as queue_time_median
       from (
                  select queue,
                         jeditaskid,
                         jobstatus,
                         modificationtime,
                         LAG(CAST(modificationtime as date), 1)
                             OVER (
                                 PARTITION BY jeditaskid ORDER BY modificationtime ASC) as prev_state,
                         ROUND((CAST(modificationtime as date) - (LAG(CAST(modificationtime as date), 1)
                                                                      OVER (
                                                                          PARTITION BY jeditaskid ORDER BY modificationtime ASC))) *
                               60 * 60 * 24, 3)                                         as lag
                  FROM (SELECT ja4.computingsite as queue,
                               ja4.jeditaskid,
                               js.jobstatus,
                               min(js.modificationtime) as modificationtime
                        FROM ATLAS_PANDA.JOBS_STATUSLOG js
                                 INNER JOIN ATLAS_PANDA.JOBSACTIVE4 ja4 ON (js.pandaid = ja4.pandaid)
                        WHERE js.modificationtime >= sysdate - 1
                          and js.prodsourcelabel = 'user'
                          and js.jobstatus in ('activated', 'running')
                        group by ja4.computingsite,
                                 ja4.jeditaskid,
                                 js.jobstatus)
              )
where jobstatus = 'running'
group by queue
),
     b as (
         select computingsite as queue,
       count(pandaid) as n_jobs
from ATLAS_PANDA.JOBSACTIVE4
where modificationtime >= sysdate - 1
and prodsourcelabel = 'user'
group by computingsite
     ),
     c as (
        select sum(n_jobs) as total_jobs from b
     )
select a.*, b.n_jobs,
       round((a.queue_time_avg * b.n_jobs)/c.total_jobs,3) as weighted_avg_queue_time,
       round((a.queue_time_median * b.n_jobs)/c.total_jobs,3) as weighted_median_queue_time
from a,b,c
where a.queue = b.queue;
