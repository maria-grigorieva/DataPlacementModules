-- free space on disks
WITH a AS (
SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS rse, free
FROM ATLAS_RUCIO.rse_usage
WHERE
source = 'storage'
AND ATLAS_RUCIO.id2rse(RSE_ID) LIKE '%DATADISK'),
b AS (SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS rse, value AS site FROM ATLAS_RUCIO.RSE_ATTR_MAP WHERE KEY='site'),
c AS (SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS rse, value AS cloud FROM ATLAS_RUCIO.RSE_ATTR_MAP WHERE KEY='cloud'),
d AS (SELECT b.rse, b.site, c.cloud FROM b, c WHERE b.rse = c.rse)
SELECT a.*,
d.site,d.cloud
FROM a,d WHERE a.rse = d.rse