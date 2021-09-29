# merging filtered data with queues metrics

import pandas as pd

filtered = pd.read_csv('data_samples/filtered.csv',index_col=[0])
queues_metrics = pd.read_csv('data_samples/queues_metrics.csv',index_col=[0])
storage_info = pd.read_csv('data_samples/storage_info.csv', index_col=[0])

merged = pd.merge(filtered, queues_metrics, left_on='queue', right_on='queue', how='inner')
merged.rename(columns={'datetime_x':'datetime'}, inplace=True)
merged.drop('datetime_y', 1, inplace=True)

merged_storage = pd.merge(merged, storage_info, left_on='rse', right_on='rse', how='inner')

# filter by transferring limit
# skip if transferring > max(transferring_limit, 2 x running),
# where transferring_limit limit is defined by site or 2000 if undefined
merged_limit = merged_storage[merged_storage['transferring'] < merged_storage['transferringlimit']]
result = merged_limit[merged_limit['transferring'] < 2*merged_limit['running']]

result.drop(['datetime_y','Storage Timestamp'], axis=1, inplace=True)
result.rename(columns={'datetime_x':'datetime'}, inplace=True)

grouped = result.groupby(['datetime','rse','site','cloud','tier_level']).agg({'queue_efficiency': 'max',
                                                  'queue_occupancy': 'max',
                                                  'Free(storage)': 'max',
                                                  'Total(storage)': 'max',
                                                  'Unlocked': 'max',
                                                  'transferring':'max',
                                                  'transferringlimit': 'max',
                                                  'corecount': 'max',
                                                  'corepower': 'max',
                                                  'running': 'max',
                                                  'queued': 'max',
                                                  'finished': 'max',
                                                  'failed': 'max'})
grouped.reset_index(inplace=True)
grouped.to_csv('data_samples/merged.csv')