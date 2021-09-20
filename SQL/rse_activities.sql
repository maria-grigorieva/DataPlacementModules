-- get all activities as all DATADISKs with sites/clouds
WITH a as(
SELECT ATLAS_RUCIO.id2rse(DEST_RSE_ID) AS rse,
	activity,
	count(*) AS n_requests,
	sum(bytes) AS bytes
	FROM ATLAS_RUCIO.requests
	WHERE ATLAS_RUCIO.id2rse(DEST_RSE_ID) LIKE '%DATADISK'
	GROUP BY ATLAS_RUCIO.id2rse(DEST_RSE_ID),
	activity),
	b AS (SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS rse, value AS site FROM ATLAS_RUCIO.RSE_ATTR_MAP WHERE KEY='site'),
c AS (SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS rse, value AS cloud FROM ATLAS_RUCIO.RSE_ATTR_MAP WHERE KEY='cloud'),
d AS (SELECT b.rse, b.site, c.cloud FROM b, c WHERE b.rse = c.rse)
SELECT a.*,
d.site,d.cloud
FROM a,d WHERE a.rse = d.rse