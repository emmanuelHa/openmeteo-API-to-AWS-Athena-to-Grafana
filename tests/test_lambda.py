import pytest
import json
from my_lambda.historical_weather_data_lambda_put_record_batch import get_data_to_push

# Mark this as an integration test
def test_get_data_to_push_real_api():
    # Act
    result = get_data_to_push()

    # Assert
    assert isinstance(result, list)
    
    # Basic validation of the response structure
    if len(result) > 0:
        first_record = result[0]
        assert 'Data' in first_record
        data = json.loads(first_record['Data'].replace("\n", "").replace("'", "\""))
        
        # Verify all required fields are present
        expected_fields = {
            'latitude', 'longitude', 'unit', 'time', 'temp', 'row_ts'
        }
        assert all(field in data for field in expected_fields)
        
        # Verify data types
        assert isinstance(data['latitude'], (int, float))
        assert isinstance(data['longitude'], (int, float))
        assert isinstance(data['unit'], str)
        assert isinstance(data['time'], str)
        assert isinstance(data['temp'], (int, float))
        assert isinstance(data['row_ts'], str)

    print("\nActual API Response:")
    print(f"Number of records: {len(result)}")
    if len(result) > 0:
        print("Sample record:")
        print(result[0])
