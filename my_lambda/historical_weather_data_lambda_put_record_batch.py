from enum import Enum
import boto3
import datetime
import json
import logging  
import urllib3

from urllib3.exceptions import HTTPError
from pydantic import BaseModel, ValidationError, computed_field, Field, AliasPath

# REPLACE WITH YOUR DATA FIREHOSE NAME
FIREHOSE_NAME = 'PUT-S3-XXX'

logger = logging.getLogger()

fh = boto3.client('firehose')

class HourlySamples(BaseModel):
    every_hours: list[str] = Field(validation_alias=AliasPath('time'))
    every_temperatures: list[float] = Field(validation_alias=AliasPath('temperature_2m'))

class TemperatureUnit(Enum):
    CELCIUS = "°C"
    FAHRENHEIT = "°F"

class HourlyUnits(BaseModel):
    time: str
    temperature_2m: TemperatureUnit


class Temperature(BaseModel):
    hourlySamples: HourlySamples = Field(validation_alias=AliasPath('hourly'))
    latitude: float
    longitude: float
    hourly_units: HourlyUnits


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
        try:
            # turn it into a dictionary
            r_dict = json.loads(response.data.decode(encoding='utf-8', errors='strict'))
            temperature = Temperature.model_validate(r_dict)
            time_list = []
            for val in temperature.hourlySamples.every_hours:
                time_list.append(val)
            
            temp_list = []
            for temp in temperature.hourlySamples.every_temperatures:
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
            processed_dict['latitude'] = temperature.latitude
            processed_dict['longitude'] = temperature.longitude
            processed_dict['unit'] = temperature.hourly_units.temperature_2m.value
            for i in range(len(time_list)):
                # construct each record
                processed_dict['time'] = time_list[i]
                processed_dict['temp'] = temp_list[i]
                processed_dict['row_ts'] = str(datetime.datetime.now())
            
                # add a newline to denote the end of a record
                # add each record to the records_to_push list
                msg = str(processed_dict) + '\n'
                records_to_push.append({'Data': msg})
        except ValidationError as e:
            return {"result": "error", "message": e.errors(include_url=False)}  
          
    except HTTPError as e:
        logger.error(f"HTTP error encountered: {e}")  

    return records_to_push