import argparse
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape, Point
import requests
import json

API_KEY = ""
URL = "https://maps.googleapis.com/maps/api/geocode/json"

def preprocess_df(file_name):
  df = pd.read_csv(file_name)
  df.replace(np.nan, "", inplace=True)
  df['full_address'] = df['Address Line 1'] + " " + df['Address Line 2'] + ", " + df['City'] + ", " + df['State'] + " " + df['Zip'] + ", USA"
  df['geocode_error'] = False
  
  for column in ['lat', 'lon', 'DQ_address', 'DQ_zip']:
      if column not in df.columns:
          df[column] = ""
          
  return df

def build_payload(address, api_key):
  return {
      'address': address,
      'key': api_key
  }

def geo_code_addresses(df, api_key):
  for i in df.index:
      payload = build_payload(df.at[i, 'full_address'], api_key)
      r = requests.get(URL, params=payload)
      json_response = r.json()

      if json_response['status'] != "OK":
          df.at[i, 'lat'] = 0
          df.at[i, 'lon'] = 0
          df.at[i, 'geocode_error'] = True
          print "ERROR at {}: {} for address {}".format(i, json_response['status'], df.at[i, 'full_address'])
          continue

      json_results = json_response['results'][0]
      lat, lon = json_results['geometry']['location'].values()

      df.at[i, 'lat'] = lat
      df.at[i, 'lon'] = lon
      df.at[i, 'DQ_address'] = json_results['formatted_address']

      address_components = {component_type['types'][0]: component_type['short_name'] for component_type in json_results['address_components']}
      if ('postal_code_suffix' in address_components) and ('postal_code' in address_components):
          df.at[i, 'DQ_zip'] = address_components['postal_code'] + "-" + address_components['postal_code_suffix']
  return df

political_districts = [
  ('districts/USCongress2010/USCongress2010.shp', 'DISTRICT', 'us_congress'),
  ('districts/ILSenate2010/ILSenate2010.shp', 'DISTRICT', 'il_senate'),
  ('districts/ILHouse2010/ILHouse2010.shp', 'DISTRICT', 'il_house'),
  ('districts/ccgisdata - Commissioner Districts - Current.geojson', 'district_n', 'commissioner_district'),
  ('districts/Boundaries - Community Areas (current).geojson', 'community', 'community_area'),
  ('districts/Boundaries - Wards (2015-).geojson', 'ward', 'ward'),
  ('districts/Precincts (current).geojson', 'full_text', 'chicago_precinct'),
  ('districts/ccgisdata - Election Precinct Data - 2015 to 2016.geojson', 'idpct', 'cook_county_precinct'),
  ('districts/Chicago Public Schools - Local School Council Voting Districts 2018-2020.geojson', 'school_nam', 'school_boundary')
]

def match_to_districts(gdf, gis_file_name, feature_name, new_feature_name):  
  districts = gpd.read_file(gis_file_name).to_crs({'init': 'epsg:4326'})
  intersection = gpd.sjoin(gdf, districts[[feature_name, 'geometry']], how='left', op='intersects')
  intersection.rename(columns={feature_name: new_feature_name}, inplace=True)
  intersection[new_feature_name] = intersection[new_feature_name].fillna('')
  intersection.drop('index_right', axis=1, inplace=True)
  return intersection

def reduce_school_district_rows(df):
  if 'school_boundary' not in df.columns:
      return df
  
  flattened = df.groupby('AK_id').school_boundary.apply(lambda x: ', '.join(x)).reset_index().rename(columns={'school_boundary': 'school_list'})
  merged = df.merge(flattened, on='AK_id', how='outer').drop_duplicates('AK_id').reset_index(drop=True)
  merged.drop('school_boundary', axis=1, inplace=True)
  return merged


def main(in_fn, out_fn, api_key):
  df = preprocess_df(in_fn)
  df = geo_code_addresses(df, api_key)

  gdf = gpd.GeoDataFrame(df, geometry=[Point(xy) for xy in zip(df.lon, df.lat)])
  for gis_file_name, feature_name, new_feature_name in political_districts:
      gdf = match_to_districts(gdf, gis_file_name, feature_name, new_feature_name)
  gdf = reduce_school_district_rows(gdf)
      
  gdf.to_csv(out_fn, index=False)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('input')
  parser.add_argument('-o', '--output', default="output.csv")
  parser.add_argument('-a', '--api', default=API_KEY)
  args = parser.parse_args()
  main(args.input, args.output, args.api)