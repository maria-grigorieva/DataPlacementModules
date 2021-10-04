import pandas as pd
import typer
import numpy as np

def main(datasetname: str):

    merged = pd.read_csv('data_samples/merged_v1.csv',index_col=[0])
    distances = pd.read_csv('data_samples/distances.csv',index_col=[0])
    distances.reset_index(inplace=True)
    datasets = pd.read_csv(f'dataset_replicas/{datasetname}.csv',index_col=[0])

    replicas = datasets[datasets['official']==True]['rse'].values

    for r in replicas:
        dist = distances[distances['SOURCE'] == r][['DEST','AGIS_DISTANCE']]
        result = pd.merge(merged, dist, left_on='rse', right_on='DEST')
        result.drop('DEST',axis=1,inplace=True)
        result.rename(columns={'AGIS_DISTANCE':f'{r}_distance'},inplace=True)
        # Normalization
        dist_max = distances['AGIS_DISTANCE'].max()
        result[f'{r}_closeness'] = dist_max - result[f'{r}_distance']
        result.set_index(['rse','site','cloud','datetime','tier_level','queue'], inplace=True)

        grouped = result.groupby(['datetime',
                                   'rse',
                                   'site',
                                   'cloud',
                                   'tier_level']).agg({'queue_efficiency': 'max',
                                                       'queue_occupancy': 'max',
                                                       'Difference': 'max',
                                                       'Unlocked': 'max',
                                                       f'{r}_closeness': 'max',
                                                       f'{r}_distance': 'max',
                                                       'transferring_availability': 'max'})

        norm_df = grouped.apply(lambda x: round((x - np.mean(x)) / (np.max(x) - np.min(x)),3))
        norm_df.reset_index(inplace=True)

        norm_df['rse_weight'] = norm_df['queue_efficiency']+\
                                norm_df['queue_occupancy']+\
                                norm_df['Difference']+\
                                norm_df['Unlocked']+\
                                norm_df[f'{r}_closeness']+\
                                norm_df['transferring_availability']

        grouped.reset_index(inplace=True)

        grouped['rse_weight'] = round(norm_df['rse_weight'],3)

        grouped.to_csv(f'suggestions/{datasetname}_{r}.csv')

if __name__ == '__main__':
    typer.run(main)