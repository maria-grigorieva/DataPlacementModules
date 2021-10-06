import pandas as pd
import typer
import numpy as np

def main(datasetname: str):

    merged = pd.read_csv('data_samples/merged.csv',index_col=[0])

    # applying filters to merged data
    merged = merged[(merged['Difference'] > 15) & (merged['queue_efficiency'] >= 0.8) & (merged['queue_occupancy'] >= 2)]

    distances = pd.read_csv('data_samples/distances.csv',index_col=[0])
    distances.reset_index(inplace=True)

    datasets = pd.read_csv(f'dataset_replicas/{datasetname}.csv',index_col=[0])

    replicas = datasets[datasets['official']==True]['rse'].values

    dynamic_replicas = datasets[datasets['official']==False][['rse','TB','available_TB','official']]

    for r in replicas:
        dist = distances[distances['SOURCE'] == r][['DEST','AGIS_DISTANCE']]
        result = pd.merge(merged, dist, left_on='rse', right_on='DEST')
        result.drop('DEST',axis=1,inplace=True)
        result.rename(columns={'AGIS_DISTANCE':'distance_from_source'},inplace=True)

        # filter distances
        result = result[(result['distance_from_source'] > 0) & (result['distance_from_source'] < 5)]

        dist_max = distances['AGIS_DISTANCE'].max()
        result['closeness'] = dist_max - result['distance_from_source']

        grouped_dynamic_replicas = pd.merge(result, dynamic_replicas, left_on='rse',
                                            right_on='rse', how='outer')
        grouped_dynamic_replicas['dataset_size_TB'] = dynamic_replicas['TB'].values[0]
        grouped_dynamic_replicas.drop('TB',axis=1,inplace=True)
        grouped_dynamic_replicas['available_TB'].fillna(0,inplace=True)

        grouped = grouped_dynamic_replicas.groupby(['datetime',
                                   'rse',
                                   'site',
                                   'cloud',
                                   'tier_level']).agg({'queue_efficiency': 'max',
                                                       'queue_occupancy': 'max',
                                                       'Difference': 'max',
                                                       'Unlocked': 'max',
                                                       'closeness': 'max',
                                                       'distance_from_source': 'max',
                                                       'transferring_availability': 'max',
                                                       'dataset_size_TB':'max',
                                                       'available_TB':'max'})


        # Normalization
        norm_df = grouped.apply(lambda x: round((x - np.mean(x)) / (np.max(x) - np.min(x)),3))
        norm_df.reset_index(inplace=True)

        norm_df['rse_weight'] = norm_df['queue_efficiency']+\
                                norm_df['queue_occupancy']+\
                                norm_df['Difference']+\
                                norm_df['Unlocked']+\
                                norm_df['closeness']+\
                                norm_df['transferring_availability']+\
                                norm_df['available_TB']

        grouped.reset_index(inplace=True)

        grouped['rse_weight'] = round(norm_df['rse_weight'],3)
        grouped['source_replica'] = r

        grouped.to_csv(f'suggestions/{datasetname}_{r}.csv')

if __name__ == '__main__':
    typer.run(main)