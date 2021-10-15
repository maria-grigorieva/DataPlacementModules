-- queues efficiency only for analysis jobs
SELECT TRUNC(sysdate,'DAY') as datetime,
       computingsite as queue,
       NVL(ROUND(finished/(finished+failed),4),0) as efficiency
       FROM
       (SELECT * FROM
(SELECT computingsite,
       jobstatus,
       count(*) as n_jobs
FROM ATLAS_PANDA.JOBSARCHIVED4
WHERE prodsourcelabel = 'user'
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
ORDER BY efficiency DESC