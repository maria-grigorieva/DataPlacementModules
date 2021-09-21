-- get all activities as all DATADISKs with sites/clouds

-- RSE                               |ACTIVITY              |N_REQUESTS|BYTES          |SITE                   |CLOUD|
-- ----------------------------------+----------------------+----------+---------------+-----------------------+-----+
-- INFN-ROMA1_DATADISK               |Production Output     |     14473| 10900462875385|INFN-ROMA1             |IT   |
-- DESY-ZN_DATADISK                  |Production Output     |      7495|  1500561067417|DESY-ZN                |DE   |
-- RAL-LCG2-ECHO_DATADISK            |Data Consolidation    |     24134| 32129587751967|RAL-LCG2               |UK   |
-- TAIWAN-LCG2_DATADISK              |Data rebalancing      |     49015| 90407892332030|Taiwan-LCG2            |TW   |
-- GRIF-LAL_DATADISK                 |Production Input      |       158|   107135594066|GRIF-LAL               |FR   |
-- NCG-INGRID-PT_DATADISK            |Functional Test       |       335|      351272960|NCG-INGRID-PT          |ES   |
-- TAIWAN-LCG2_DATADISK              |Data Challenge        |     19587|113507571228197|Taiwan-LCG2            |TW   |
-- BNL-OSG2_DATADISK                 |Production Input      |      2197|  2788257862869|BNL-ATLAS              |US   |
--

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