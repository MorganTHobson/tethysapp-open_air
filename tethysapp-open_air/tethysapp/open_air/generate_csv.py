import pandas as pd
import boto3 as bt
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
from conversion_helpers import datetime2str

def generate_csv():
    # DataFrame
    df = pd.DataFrame(columns=['id','timest','H2S_avg','H2S_std','NO2_avg','NO2_std','O3_avg','O3_std','SO2_avg','SO2_std','battAV','hum1','hum2','hum3','temp1','temp2','temp3'])

    # Get DynamoDB table
    dynamodb = bt.resource('dynamodb')
    table = dynamodb.Table('BaltimoreOpenAir2017') # Remember to update table here

    response = table.query(
        KeyConditionExpression=Key('id').eq('24') & Key('timest').gt(int(datetime2str(datetime.now() - timedelta(days=7))))
    )

    i = 0

    # Extract points from table response
    for entry in response['Items']:
        row = {'id':[entry['id']],
               'timest':[entry['timest']],
               'H2S_avg':[entry['H2S_avg']],
               'H2S_std':[entry['H2S_std']],
               'NO2_avg':[entry['NO2_avg']],
               'NO2_std':[entry['NO2_std']],
               'O3_avg':[entry['O3_avg']],
               'O3_std':[entry['O3_std']],
               'SO2_avg':[entry['SO2_avg']],
               'SO2_std':[entry['SO2_std']],
               'battAV':[entry['battAV']],
               'hum1':[entry['hum1']],
               'hum2':[entry['hum2']],
               'hum3':[entry['hum3']],
               'temp1':[entry['temp1']],
               'temp2':[entry['temp2']],
               'temp3':[entry['temp3']]}
        df2 = pd.DataFrame(data=row)
        df.loc[i]=row
        print(df)
        i = i+1

    df.to_csv(path_or_buf='test.csv')


generate_csv()
