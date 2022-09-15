from __future__ import print_function
import pandas as pd
from collections import OrderedDict
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# load the national file from a local directory
path = ""
file = "chicago_membership_list.csv"
# quarterly, download the entire action network database and save it to the same directory and name it ANDatabase.xls
# we use this as a reference to maintain data provided directly by members, instead of always trusting national
compare = "ANDatabase.xlsx"
landlines = ""

inputFile = path + file
compareFile = path + compare

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Get the phone corrections from the corrections sheet Joe used to maintain.
# https://docs.google.com/spreadsheets/d/1dWdi87PCZVRaf8nVaDd6fszNDE3zo7G9-EF1zHwLTHE/edit#gid=413746868
# import secrets from file in gitignore to ignore accidentally posting creds
filename = "secrets.json"
if filename:
    with open(filename, 'r') as f:
        datastore = json.load(f)

SAMPLE_SPREADSHEET_ID = datastore["SAMPLE_SPREADSHEET_ID"]
SAMPLE_RANGE_NAME = 'PhoneCorrections!A1:D'
REMOVE_RANGE_NAME = 'Remove!A1:C'


def main():
    # go get the google sheet values
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    # these people need correcting
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    # these people opt out, so remove them
    result_remove = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=REMOVE_RANGE_NAME).execute()

    header = result.get('values', [])[0]   # Assumes first line is header!
    values = result.get('values', [])[1:]  # Everything else is data.

    removeheader = result_remove.get('values', [])[0]   # Assumes first line is header!
    removevalues = result_remove.get('values', [])[1:]  # Everything else is data.

    # write results to dataframe
    df_google = pd.DataFrame(values, columns=header)
    df_remove = pd.DataFrame(removevalues, columns=removeheader)

    # call the main method
    df_cleansed = member_file_import(df_google)

    # this removes people from the 'Remove' tab of the spreadsheet
    df_output = remove_contact(df_remove, df_cleansed)

    # export, modified to handle unicode
    df_output.to_csv("output.csv", index=False)

    # old export, in case you don't want to use xlsxwriter
    # writer = pd.ExcelWriter('output.xlsx')
    # df_output.to_excel(writer, 'Sheet1')
    # writer.save()


def column_expander(df, column, prefix, foo):

    df2 = df[column].apply(foo)
    a = len(df2.columns)
    i = 1
    x = ""
    list = []
    while i <= a:
        x = prefix + str(i)
        list.append(x)
        i += 1
    return list


def remove_contact(dfg, df):
    dfg = dfg.set_index('AK_ID')
    df = pd.merge(df, dfg, left_index=True, right_index=True, indicator=True
                  , suffixes=('_left', '_right'), how='outer').query('_merge=="left_only"').drop(['_merge', 'Why', 'Notes'], axis=1)
    # df = df.join(dfg.set_index('AK_ID'), on='AK_ID', how="left")
    # df = pd.merge(df, dfg, how='left', on='AK_ID')
    return df


def member_file_import(dfg):
    # get monthly file to dataframe
    df = pd.read_csv(inputFile)
    # get AnDatabase file to dataframe
    df2 = pd.read_excel(compareFile, engine='openpyxl')

    df['email_DQ'] = df['Email']

    # if the member does not have an email address, set it to their national ID + fake
    df.loc[df['email_DQ'].isnull(), 'email_DQ'] = 'CDSA' + df['AK_ID'].astype(str) + '@fakeDomain.com'

    df = df.astype(str)
    df2 = df2.astype(str)

    foo = lambda x: pd.Series([i for i in reversed(x.split(','))])

    # the national file contains multiple phone numbers in 1 column as csv. Expand each
    home = column_expander(df, "Home_Phone", "Home_", foo)

    df[home] = df['Home_Phone'].apply(foo)

    mobile = column_expander(df, "Mobile_Phone", "Mobile_", foo)
    df[mobile] = df['Mobile_Phone'].apply(foo)

    work = column_expander(df, "Work_Phone", "Work_", foo)
    df[work] = df['Work_Phone'].apply(foo)

    # regardless of phone number type, shove them all into an array
    cleanse = []
    cleanse = home + mobile + work

    # fix for the new national file format
    for index, row in df.iterrows():
        a = df.loc[index, 'Mailing_Address1']
        if a == 'nan':
            row['Mailing_Address1'] = row['Billing_Address_Line_1']
            row['Mailing_Address2'] = row['Billing_Address_Line_2']
            row['Mailing_City'] = row['Billing_City']
            row['Mailing_State'] = row['Billing_State']
            row['Mailing_Zip'] = row['Billing_Zip']

    # remove special characters from national file phone numbers
    df[cleanse] = df[cleanse].astype(str).replace('\.0', '', regex=True)
    df2 = df2.astype(str).replace('\.0', '', regex=True)

    # format andatabase file phone columns to match df
    df2.rename(columns={"Phone1": "anPhone1", "Phone2": "anPhone2"
            , "Phone3": "anPhone3", "Phone4": "anPhone4", "National ID": "AK_ID"}, inplace=True)

    # subset df2
    df2 = df2[['AK_ID', 'Phone', 'phoneNumber']]

    # call cleanse function (below)
    cleanse_phone(df, cleanse)
    cleanse_phone(df2, ['Phone', 'phoneNumber'])

    df = df.replace('nan', '', regex=True)
    df2 = df2.replace('nan', '', regex=True)
    df.fillna(value='', inplace=True)
    df2.fillna(value='', inplace=True)

    # merge datasets, keeping only unique values
    df = df.set_index('AK_ID').join(df2.set_index('AK_ID'), on='AK_ID', how="left")

    # add in 2 more columns for reasons I've forgotten
    anNumbers = ['Phone', 'phoneNumber']
    cleanse.extend(anNumbers)

    df['Test'] = df[cleanse].apply(lambda x: x.str.cat(sep=','), axis=1)
    df['Test'] = df['Test'].map(lambda x: x.lstrip(',').rstrip(', ')).str.replace('-', '')
    df['Test'] = df['Test'].str.replace('\W+', ' ')

    # this deduplicates the values from the test column
    df['Desired'] = df['Test'].str.split().apply(lambda x: OrderedDict.fromkeys(x).keys()).str.join(' ')

    foo = lambda x: pd.Series([i for i in reversed(x.split(' '))])

    # format everything into the expected shape
    cols = column_expander(df, "Desired", "Phone", foo)
    df[cols] = df['Desired'].apply(foo)

    fincols = ['first_name', 'last_name', 'middle_name', 'Mailing_Address1', 'Mailing_Address2', 'Mailing_City', 'Mailing_State'
            , 'Mailing_Zip', 'Mail_preference', 'Do_Not_Call', 'Join_Date', 'Xdate', 'membership_status', 'email_DQ'
            , 'membership_type', 'monthly_dues_status', 'union_member', 'union_name', 'union_local', 'student_yes_no'
            , 'student_school_name', 'YDSA Chapter']
    fincols.extend(cols)

    df = df[fincols]

    # replace stuff from the google sheet
    # If IN/DC/WN - delete the phone number outright
    # If CH - replace the phone number value
    # If LL - Validate through the API and lookup list

    # Google columns: NationalID Result PhoneNumber ChangeNumber
    for index, row in dfg.iterrows():
        if row['NationalID'] in df.index:
            for i in cols:
                try:
                    if row['Result'] == 'CH' and df.at[row['NationalID'], i] == row['PhoneNumber']:
                        df.at[row['NationalID'], i] = row['ChangeNumber']
                    elif row['Result'] in ['IN', 'WN', 'DC'] and df.at[row['NationalID'], i] == row['PhoneNumber']:
                        df.at[row['NationalID'], i] = ''
                except:
                    continue
                # if row['Result'] == 'CH':
                #     try:
                #         for i in cols:
                #             if df.at[row['NationalID'], i] == row['PhoneNumber'] and row['ChangeNumber'] != '':
                #                 print(df.at[row['NationalID'], i])
                #                 df.at[row['NationalID'], i] = row['ChangeNumber']
                #     except:
                #         continue
                # elif row['Result'] in ['IN', 'WN', 'DC']:
                #     # print(row['Result'])
                #     try:
                #         for i in cols:
                #             if df.at[row['NationalID'], i] == row['PhoneNumber']:
                #                 df.at[row['NationalID'], i] = ''
                    # except:
                    #     continue
            else:
                continue
        else:
            continue

    return df

# helper function to remove formatting from phone number
def cleanse_phone(df, series):
    for column in series:
        # remove special characters
        df[column] = df[column].str.replace(' ', '')
        df[column] = df[column].str.replace('-', '')
        df[column] = df[column].str.replace('.', '')
        df[column] = df[column].str.replace('(', '')
        df[column] = df[column].str.replace(')', '')
        df[column] = df[column].str.replace(' ', '')
        df[column] = df[column].str.replace('+', '')
        df[column] = df[column].str.replace('*', '')
        df[column] = df[column].apply(lambda x: x[1:] if x.startswith("1") and len(x) > 10 else x)
        df[column] = df[column].apply(lambda x: x[2:] if x.startswith(",1") else x)


if __name__ == '__main__':
    main()
