# merging filtered data with queues metrics

import pandas as pd

filtered = pd.read_csv('data_samples/filtered.csv',index_col=[0])
queues_metrics = pd.read_csv('data_samples/queues_metrics.csv',index_col=[0])

merged = pd.merge(filtered, queues_metrics, left_on='queue', right_on='queue', how='inner')
merged.rename(columns={'datetime_x':'datetime'}, inplace=True)
merged.drop('datetime_y', 1, inplace=True)
merged.to_csv('data_samples/merged.csv')