WITH a AS (SELECT SCOPE, name, ATLAS_RUCIO.id2rse(RSE_ID) AS rse,
did_type, bytes, state, accessed_at, updated_at, created_at, available_replicas_cnt, length, available_bytes
FROM ATLAS_RUCIO.COLLECTION_REPLICAS 
WHERE SCOPE = 'data18_13TeV' 
AND NAME = 'data18_13TeV.00349014.physics_Main.deriv.DAOD_TOPQ1.f937_m1972_p4513_tid25514135_00'),
b AS (SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS rse, value AS site FROM ATLAS_RUCIO.RSE_ATTR_MAP WHERE KEY='site'),
c AS (SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS rse, value AS cloud FROM ATLAS_RUCIO.RSE_ATTR_MAP WHERE KEY='cloud'),
d AS (SELECT b.rse, b.site, c.cloud FROM b, c WHERE b.rse = c.rse)
SELECT a.*, d.site, d.cloud
FROM a,d 
WHERE a.rse = d.rse;