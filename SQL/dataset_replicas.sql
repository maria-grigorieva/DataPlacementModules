SELECT SCOPE,
name,
ATLAS_RUCIO.id2rse(RSE_ID) AS rse,
did_type,
bytes,
state,
accessed_at,
updated_at,
created_at,
available_replicas_cnt,
LENGTH,
available_bytes
FROM ATLAS_RUCIO.COLLECTION_REPLICAS
WHERE SCOPE = 'data18_13TeV'
AND NAME = 'data18_13TeV.00349014.physics_Main.deriv.DAOD_TOPQ1.f937_m1972_p4513_tid25514135_00'