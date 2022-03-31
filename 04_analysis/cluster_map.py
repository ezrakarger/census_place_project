import geopandas as gpd
import pandas as pd
import random
import os
import matplotlib.pyplot as plt
import matplotlib


wd = 'G:/Dropbox/research/census_geotag'
os.chdir(wd)

matplotlib.use('Agg')

states_geo = gpd.read_file('data/shape_files/states2010/contiguous_states.shp')
states_geo = states_geo.to_crs('epsg:4326')

counties_geo = gpd.read_file('data/shape_files/US_county_1940_conflated.shp')
counties_geo = counties_geo.to_crs('epsg:4326')

cluster_df = pd.read_stata('data/geo_data/place_component_crosswalk.dta')
cluster_df['city_geometry'] = gpd.points_from_xy(cluster_df['lon'], cluster_df['lat'], crs='epsg:4326')
cluster_df = gpd.GeoDataFrame(cluster_df, geometry='city_geometry')
cluster_df = cluster_df.to_crs('epsg:4326')

for cluster_level in [5, 10, 50, 100, 200, 300, 500]:
    consistent_column = 'consistent_place_' + str(cluster_level)
    cluster_df['r'] = cluster_df.groupby(consistent_column)[consistent_column].transform(lambda x: random.random())
    cluster_df['g'] = cluster_df.groupby(consistent_column)[consistent_column].transform(lambda x: random.random())
    cluster_df['b'] = cluster_df.groupby(consistent_column)[consistent_column].transform(lambda x: random.random())

    cluster_df['color'] = cluster_df[['r', 'g', 'b']].values.tolist()

    base = states_geo.to_crs('epsg:5070').plot(color='white', edgecolor='black', figsize=(30, 30))
    base.axis('off')
    cluster_df.to_crs('epsg:5070').plot(ax=base, marker='o', color=cluster_df['color'], markersize=0.5)

    fig = base.get_figure()
    fig.savefig('figures_march2022/national_map_cluster_level_' + str(cluster_level) + '.png', bbox_inches='tight')
    plt.close(fig)

    for state in list(counties_geo['STATENAM'].unique()):
        state_geo = counties_geo[counties_geo['STATENAM'] == state]
        cluster_state_df = gpd.sjoin(cluster_df, state_geo, how='inner', op='within')
        if len(cluster_state_df) == 0:
            continue
        base = state_geo.plot(color='white', edgecolor='black', figsize=(10, 10), linewidth=0.1)
        base.axis('off')
        cluster_state_df.plot(ax=base, marker='o', color=cluster_state_df['color'], markersize=1)

        fig = base.get_figure()
        fig.savefig('figures_march2022/by_state/county_map/' + state + '_county_map_cluster_level_' + str(cluster_level) + '.png', bbox_inches='tight')
        plt.close(fig)

    for state in list(states_geo['STUSPS10'].unique()):
        state_geo = states_geo[states_geo['STUSPS10'] == state]
        cluster_state_df = gpd.sjoin(cluster_df, state_geo, how='inner', op='within')
        if len(cluster_state_df) == 0:
            continue
        base = state_geo.plot(color='white', edgecolor='black', figsize=(10, 10), linewidth=0.1)
        base.axis('off')
        cluster_state_df.plot(ax=base, marker='o', color=cluster_state_df['color'], markersize=1)

        fig = base.get_figure()
        fig.savefig('figures_march2022/by_state/state_map/' + state + '_state_map_cluster_level_' + str(cluster_level) + '.png', bbox_inches='tight')
        plt.close(fig)

    for state in list(states_geo['STUSPS10'].unique()):
        state_geo = states_geo[states_geo['STUSPS10'] == state]
        cluster_state_df = gpd.sjoin(cluster_df, state_geo, how='inner', op='within')
        if len(cluster_state_df) == 0:
            continue
        base = state_geo.plot(color='white', edgecolor='black', figsize=(10, 10), linewidth=0.1)
        base.axis('off')
        cluster_state_df.plot(ax=base, marker='o', color=cluster_state_df['color'], markersize=1)

        cluster_state_df = cluster_state_df.sort_values(by='fracpop', ascending=False)
        cluster_pop_df = cluster_state_df.groupby(consistent_column)['fracpop'].aggregate('sum').reset_index()
        cluster_pop_df = cluster_pop_df.sort_values(by='fracpop', ascending=False)

        count = 0
        for idx, row in cluster_pop_df.iterrows():
            myrow = cluster_state_df[cluster_state_df[consistent_column] == row[consistent_column]].iloc[0]
            plt.annotate(text=myrow['potential_match'].title(), xy=(myrow['lon'], myrow['lat']),
                         ha='center', va='bottom', color='blue', weight='bold', fontsize=11)
            count += 1
            if count == 5:
                break

        fig = base.get_figure()
        fig.savefig('figures_march2022/by_state/state_map_w_city_names/' + state + '_state_map_cluster_level_' + str(cluster_level) + '.png', bbox_inches='tight')
        plt.close(fig)
