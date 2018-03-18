import pandas as pd
import numpy as np


# should this eventually connect to the google spreadsheet instead? probably.
path = ""
file = ""
writer = pd.ExcelWriter('output.xlsx')

inputFile = path + file


def member_file_import():
  df = pd.read_excel(inputFile)

  df['email_DQ'] = df['Email']
  df.loc[df['email_DQ'].isnull(), 'email_DQ'] = 'CDSA' + df['AK_id'].astype(str) + '@fakeDomain.com'

  df['Home Phone'] = df['Home Phone'].astype(str)
  df['Mobile Phone'] = df['Mobile Phone'].astype(str)
  # Replacing nan with '' since the astype(str) above converted all null values to a nan string
  df = df.replace('nan', '', regex=True)

  # if a column contains two phone numbers, split it into two. This will bomb out if national
  # ever sends three phone numbers in a single phone field. lol.
  df[['home1_DQ', 'home2_DQ']] = df['Home Phone'].str.split(',', expand=True)
  df[['cell1_DQ', 'cell2_DQ']] = df['Mobile Phone'].str.split(',', expand=True)

  df.fillna(value='', inplace=True)

  cleanse_phone(df, ['home1_DQ', 'home2_DQ', 'cell1_DQ', 'cell2_DQ'])

  df.to_excel(writer, 'Sheet1')
  writer.save()


def cleanse_phone(df, series):
  for column in series:
    # remove special characters
    df[column] = df[column].str.replace('\W', '')
    # trim to 10 characters. kinda risky if someone prefixed with 1. might remove this
    df[column] = df[column].str[:10]
    # optional formatting, but we'll keep it for now
    df[column] = np.where(
      df[column] == '', 
      '',
      df[column].astype(str).apply(
        lambda x: '(' + x[:3] + ')' + x[3:6] + '-' + x[6:10]
        )
      )


member_file_import()