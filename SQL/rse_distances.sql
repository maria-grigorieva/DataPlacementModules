--get distances between each pair of data disk from Rucio DB
-- SOURCE                            |DEST                              |SRC_SITE               |DEST_SITE              |SRC_CLOUD|DEST_CLOUD|AGIS_DISTANCE|
-- ----------------------------------+----------------------------------+-----------------------+-----------------------+---------+----------+-------------+
-- TAIWAN-LCG2_DATADISK              |TAIWAN-LCG2_DATADISK              |Taiwan-LCG2            |Taiwan-LCG2            |TW       |TW        |            0|
-- SWT2_CPB_DATADISK                 |SWT2_CPB_DATADISK                 |SWT2_CPB               |SWT2_CPB               |US       |US        |            0|
-- TR-10-ULAKBIM_DATADISK            |TR-10-ULAKBIM_DATADISK            |TR-10-ULAKBIM          |TR-10-ULAKBIM          |NL       |NL        |            0|


SELECT sysdate AS timestamp,
ATLAS_RUCIO.id2rse(SRC_RSE_ID) AS SOURCE,
ATLAS_RUCIO.id2rse(DEST_RSE_ID) AS DEST,
(SELECT value AS site FROM ATLAS_RUCIO.RSE_ATTR_MAP
WHERE RSE_ID = SRC_RSE_ID AND KEY='site') AS src_site,
(SELECT value AS site FROM ATLAS_RUCIO.RSE_ATTR_MAP
WHERE RSE_ID = DEST_RSE_ID AND KEY='site') AS dest_site,
(SELECT value AS site FROM ATLAS_RUCIO.RSE_ATTR_MAP
WHERE RSE_ID = SRC_RSE_ID AND KEY='cloud') AS src_cloud,
(SELECT value AS site FROM ATLAS_RUCIO.RSE_ATTR_MAP
WHERE RSE_ID = DEST_RSE_ID AND KEY='cloud') AS dest_cloud,
AGIS_DISTANCE
FROM ATLAS_RUCIO.distances
WHERE ATLAS_RUCIO.id2rse(SRC_RSE_ID) LIKE '%DATADISK'
AND ATLAS_RUCIO.id2rse(DEST_RSE_ID) LIKE '%DATADISK'
ORDER BY AGIS_DISTANCE ASC