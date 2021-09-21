-- free space on disks
-- RSE                               |FREE             |SITE                   |CLOUD|
-- ----------------------------------+-----------------+-----------------------+-----+
-- TAIWAN-LCG2_DATADISK              | 2596929044196508|Taiwan-LCG2            |TW   |
-- GRIF-LAL_DATADISK                 |  214617114119111|GRIF-LAL               |FR   |
-- UKI-SOUTHGRID-RALPP_DATADISK      |  393173778672980|UKI-SOUTHGRID-RALPP    |UK   |
-- BNLHPC_DATADISK                   |   24713389635674|BNLHPC                 |US   |
-- CA-WATERLOO-T2_DATADISK           |  213655125926702|CA-WATERLOO-T2         |CA   |
-- UAM-LCG2_DATADISK                 |  106944959230034|UAM-LCG2               |ES   |
-- AUSTRALIA-ATLAS_DATADISK          |  155365766269751|Australia-ATLAS        |CA   |
-- INFN-GENOVA_DATADISK              |    2586517700608|INFN-GENOVA            |IT   |
-- UKI-LT2-RHUL_DATADISK             |   82464391974032|UKI-LT2-RHUL           |UK   |

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