import json
import boto3
import urllib3
import datetime

# REPLACE WITH YOUR DATA FIREHOSE NAME
FIREHOSE_NAME = 'PUT-S3-XXX'

def lambda_handler(event, context):
    
    http = urllib3.PoolManager()
    
    r = http.request("GET", "https://archive-api.open-meteo.com/v1/archive?latitude=48.864716&longitude=2.349014&start_date=2024-11-25&end_date=2024-12-04&hourly=temperature_2m&timezone=Europe%2FBerlin")
    
    # turn it into a dictionary
    r_dict = json.loads(r.data.decode(encoding='utf-8', errors='strict'))
    
    time_list = []
    for val in r_dict['daily']['time']:
        time_list.append(val)
    
    temp_list = []
    for temp in r_dict['daily']['temperature_2m_max']:
        
        # handle null values
        # if we don't, the crawler may get confused
        if temp == None:
            temp = 0.0
        
        temp_list.append(temp)
    
    # extract pieces of the dictionary
    processed_dict = {}
    
    # append to list records_to_push
    # each record is a new list item
    records_to_push = []
    for i in range(len(time_list)):
        # construct each record
        processed_dict['latitude'] = r_dict['latitude']
        processed_dict['longitude'] = r_dict['longitude']
        processed_dict['time'] = time_list[i]
        processed_dict['temp'] = temp_list[i]
        processed_dict['row_ts'] = str(datetime.datetime.now())
    
        # add a newline to denote the end of a record
        # add each record to the records_to_push list
        msg = str(processed_dict) + '\n'
        records_to_push.append({'Data': msg})
    
    fh = boto3.client('firehose')
    
    reply = fh.put_record_batch(
        DeliveryStreamName=FIREHOSE_NAME,
        Records = records_to_push
    )

    return reply