import json
import boto3
import urllib3
import datetime
from urllib3.exceptions import HTTPError
import logging  

# REPLACE WITH YOUR DATA FIREHOSE NAME
FIREHOSE_NAME = 'PUT-S3-XXX'

logger = logging.getLogger()

fh = boto3.client('firehose')

def lambda_handler(event, context):
    
    records_to_push = get_data_to_push()
    
    reply = fh.put_record_batch(
        DeliveryStreamName=FIREHOSE_NAME,
        Records = records_to_push
    )

    return reply

def get_data_to_push():
    http = urllib3.PoolManager()
    
    try:
        # no context managesr use possible in a lambda => use of try / catch
        response = http.request("GET", "https://historical-forecast-api.open-meteo.com/v1/forecast?latitude=48.86&longitude=2.3399997&start_date=2024-11-24&end_date=2024-12-07&hourly=temperature_2m&temperature_unit=fahrenheit&timezone=Europe%2FBerlin")
        if response.status >= 400:
            logger.error(f"HTTP error status code: {response.status}")
        # turn it into a dictionary
        r_dict = json.loads(response.data.decode(encoding='utf-8', errors='strict'))
        
        time_list = []
        for val in r_dict['hourly']['time']:
            time_list.append(val)
        
        temp_list = []
        for temp in r_dict['hourly']['temperature_2m']:
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
        processed_dict['latitude'] = r_dict['latitude']
        processed_dict['longitude'] = r_dict['longitude']
        processed_dict['unit'] = r_dict['hourly_units']['temperature_2m']
        for i in range(len(time_list)):
            # construct each record
            processed_dict['time'] = time_list[i]
            processed_dict['temp'] = temp_list[i]
            processed_dict['row_ts'] = str(datetime.datetime.now())
        
            # add a newline to denote the end of a record
            # add each record to the records_to_push list
            msg = str(processed_dict) + '\n'
            records_to_push.append({'Data': msg})
    except HTTPError as e:
        logger.error(f"HTTP error encountered: {e}")  

    return records_to_push