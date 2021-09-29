import pandas as pd
import typer
from sklearn import preprocessing
import numpy as np

def main(datasetname):

    merged = pd.read_csv('data_samples/merged.csv',index_col=[0])
    distances = pd.read_csv('data_samples/distances.csv',index_col=[0])
    distances.reset_index(inplace=True)
    datasets = pd.read_csv('data_samples/dataset_replicas.csv',index_col=[0])

    replicas = datasets[(datasets['name']==datasetname) & datasets['official']==True]['rse'].values

    for r in replicas:
        dist = distances[distances['SOURCE'] == r][['DEST','AGIS_DISTANCE']]
        result = pd.merge(merged, dist, left_on='rse', right_on='DEST')
        result.drop('DEST',axis=1,inplace=True)
        result.rename(columns={'AGIS_DISTANCE':f'{r}_distance'},inplace=True)
        result.to_csv(f'suggestions/{datasetname}-{r}.csv')

        # Normalization
        dist_max = distances['AGIS_DISTANCE'].max()
        result[f'{r}_closeness'] = dist_max - result[f'{r}_distance']
        result.set_index(['rse','site','cloud','queue','datetime'], inplace=True)
        norm_df = result.apply(lambda x: round((x - np.mean(x)) / (np.max(x) - np.min(x)),3))
        norm_df.reset_index(inplace=True)
        norm_df.groupby(['queue','site','cloud']).agg({'queue_efficiency': 'mean',
                     'queue_occupancy': 'mean',
                     'transferring': 'max',
                     'Free(storage)': 'max',
                     'Total(storage)': 'max',
                     f'{r}_closeness': 'max'})
        norm_df['rse_weight'] = norm_df['queue_efficiency']+norm_df['queue_occupancy']+norm_df['transferring']+norm_df['Free(storage)']+norm_df['Total(storage)']+norm_df[f'{r}_closeness']
        norm_df.to_csv(f'suggestions/__norm_{datasetname}-{r}.csv')


if __name__ == '__main__':
    typer.run(main)