WITH
a AS (SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS rse FROM ATLAS_RUCIO.RSE_ATTR_MAP WHERE KEY='type' AND value='DATADISK'),
b AS (SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS rse, value AS site FROM ATLAS_RUCIO.RSE_ATTR_MAP WHERE KEY='site'),
c AS (SELECT ATLAS_RUCIO.id2rse(RSE_ID) AS rse, value AS cloud FROM ATLAS_RUCIO.RSE_ATTR_MAP WHERE KEY='cloud')
SELECT a.rse, b.site, c.cloud FROM a, b, c WHERE a.rse = b.rse AND b.rse = c.rse;