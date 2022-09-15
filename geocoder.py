import argparse
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape, Point
import requests
import json
import os.path


# import secrets from file in gitignore to ignore accidentally posting creds
filename = "secrets.json"
if filename:
    with open(filename, 'r') as f:
        datastore = json.load(f)

#Use the new datastore datastructure
API_KEY = datastore["API-KEY"]

API_URL = "https://maps.googleapis.com/maps/api/geocode/json"
DISTRICTS_DIR = "districts"
BRANCH_FILE = "branches_2020.tsv"
BRANCH_MAPPER = {
  'Blue': "North Side Blue",
  'Red': "North Side Red",
  'South': "South Side",
  'West': "West Cook",
  'NA': "Unknown"
}

with open('config.json') as config_file:
  political_districts = json.load(config_file)

def preprocess_df(file_name):
  df = pd.read_csv(file_name)
  df.replace(np.nan, "", inplace=True)
  df['full_address'] = (
    df['Mailing_Address1'] + " "
    + df['Mailing_Address2'] + ", "
    + df['Mailing_City'] + ", "
    + df['Mailing_State'] + " "
    + df['Mailing_Zip'] + ", USA"
  )
  
  if 'geocode_error' not in df.columns:
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
  # useful if script needs to be restarted due to error with geocoder API, connection, etc
  for i in df[df.lat == ""].index:
    if not i % 100:
      print(f"Processing Record {i}...")

    payload = build_payload(df.at[i, 'full_address'], api_key)
    r = requests.get(API_URL, params=payload)
    json_response = r.json()

    if json_response['status'] != "OK":
      df.at[i, 'lat'] = 0
      df.at[i, 'lon'] = 0
      df.at[i, 'geocode_error'] = True
      print(f"ERROR at {i}: {json_response['status']} for address {df.at[i, 'full_address']}")
    else:
      json_results = json_response['results'][0]
      lat, lon = json_results['geometry']['location'].values()

      df.at[i, 'lat'] = lat
      df.at[i, 'lon'] = lon
      df.at[i, 'DQ_address'] = json_results['formatted_address']

      address_components = {component_type['types'][0]: component_type['short_name'] for component_type in json_results['address_components']}
      if ('postal_code_suffix' in address_components) and ('postal_code' in address_components):
        df.at[i, 'DQ_zip'] = address_components['postal_code'] + "-" + address_components['postal_code_suffix']

def match_to_districts(gdf, gis_file_name, feature_name, new_feature_name):  
  gis_path = os.path.join(DISTRICTS_DIR, gis_file_name)
  districts = gpd.read_file(gis_path).to_crs(epsg=4326)
  intersection = gpd.sjoin(gdf, districts[[feature_name, 'geometry']], how='left', predicate='intersects')
  intersection.rename(columns={feature_name: new_feature_name}, inplace=True)
  intersection[new_feature_name] = intersection[new_feature_name].fillna('')
  intersection.drop('index_right', axis=1, inplace=True)
  return intersection

def reduce_school_district_rows(df):
  if 'school_boundary' not in df.columns:
    return df
  
  flattened = df.groupby('AK_ID').school_boundary.apply(lambda x: ', '.join(x)).reset_index().rename(columns={'school_boundary': 'school_list'})
  merged = df.merge(flattened, on='AK_ID', how='outer').drop_duplicates('AK_ID').reset_index(drop=True)
  merged.drop('school_boundary', axis=1, inplace=True)
  return merged

def tag_branch(df):
  branches_path = os.path.join(DISTRICTS_DIR, BRANCH_FILE)
  branches = pd.read_csv(branches_path, sep="\t")
  branches["zip"] = branches["zip"].astype(str)
  branches.branch = branches.branch.map(BRANCH_MAPPER)
  branch_dict = {zc: b for zc, b in zip(branches.zip, branches.branch)}
  
  mask = df.DQ_zip.isin(branches.zip)
  df.loc[mask, "branch"] = df.loc[mask].DQ_zip.map(branch_dict)
  df.loc[~mask, "branch"] = "Outside Chapter"

def main(in_fn, out_fn, api_key):
  df = preprocess_df(in_fn)

  try:
    geo_code_addresses(df, api_key)
  except:
    print("something went wrong")
    df.to_csv(f"PARTIAL_{out_fn}", index=False)
  else:
    gdf = gpd.GeoDataFrame(df, geometry=[Point(xy) for xy in zip(df.lon, df.lat)], crs=4326)
    for d in political_districts:
      gdf = match_to_districts(gdf, d['gis_file_name'], d['feature_name'], d['new_feature_name'])
    gdf = reduce_school_district_rows(gdf)
    tag_branch(gdf)
    gdf.to_csv(out_fn, index=False)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('input')
  parser.add_argument('-o', '--output', default="output.csv")
  parser.add_argument('-a', '--api', default=API_KEY)
  args = parser.parse_args()
  main(args.input, args.output, args.api)