from datetime import datetime
import requests
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import execute_values

DB_HOST = 'localhost'
DB_PORT = '5433'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'


class ShiftDataProcessor:
    def __init__(self, db_config: Dict[str, str], api_url: str):
        self.db_config = db_config
        self.api_url = api_url
        self.shifts = []
        self.breaks = []
        self.allowances = []
        self.award_interpretations = []

    def fetch_data(self) -> Optional[Dict]:
        try:
            response = requests.get(self.api_url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None
        except ValueError as e:
            print(f"Error parsing JSON: {e}")
            return None

    @staticmethod
    def map_dict_keys(data: List[Dict], column_mapping: Dict[str, str]) -> List[Dict]:
        return [
            {column_mapping.get(key, key): value for key, value in record.items()}
            for record in data
        ]

    @staticmethod
    def process_nested_records(results: List[Dict], record_key: str, parent_key: str) -> List[Dict]:
        return [
            {**record, parent_key: result['id']}
            for result in results
            for record in result[record_key]
        ]

    def process_json(self, json_data: Dict) -> None:
        results = json_data['results']
        
        # Process shifts
        self.shifts = [
            {
                'shift_id': result['id'],
                'shift_date': result['date'],
                'shift_cost': round(
                    sum(allowance['cost'] for allowance in result['allowances']) + 
                    sum(award['cost'] for award in result['award_interpretations']),
                    4
                ),
                'shift_start': (
                    datetime.fromtimestamp(result['start'] / 1000) if isinstance(result['start'], int) and result['start'] > 0
                    else None
                ),
                'shift_finish': (
                    datetime.fromtimestamp(result['finish'] / 1000) if isinstance(result['finish'], int) and result['finish'] > 0
                    else None
                )
            }
            for result in results
        ]
        
        # Process breaks
        self.breaks = [
            {
                'break_id': break_record['id'],
                'shift_id': result['id'],
                'break_start': (
                    datetime.fromtimestamp(break_record['start'] / 1000) if isinstance(break_record['start'], int) and break_record['start'] > 0
                    else None
                ),
                'break_finish': (
                    datetime.fromtimestamp(break_record['finish'] / 1000) if isinstance(break_record['finish'], int) and break_record['finish'] > 0
                    else None
                ),
                'is_paid': break_record['paid']
            }
            for result in results
            for break_record in result['breaks']
        ]

        # Process allowances
        self.allowances = self.map_dict_keys(
            self.process_nested_records(results, 'allowances', 'shift_id'),
            {'id': 'allowance_id', 'value': 'allowance_value', 'cost': 'allowance_cost'}
        )
        
        # Process award interpretations
        self.award_interpretations = self.map_dict_keys(
            self.process_nested_records(results, 'award_interpretations', 'shift_id'),
            {'id': 'award_id', 'date': 'award_date', 'units': 'award_units', 'cost': 'award_cost'}
        )

    def insert_data(self, table_name: str, columns: List[str], data: List[Dict]) -> None:
        column_names = ', '.join(columns)
        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES %s"

        # Prepare the data as a list of tuples, where each tuple contains values in column order
        data_tuples = [tuple(record[col] for col in columns) for record in data]

        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Use execute_values to insert all data in one go
            execute_values(cursor, insert_query, data_tuples)

            conn.commit()
            print(f"Successfully inserted {len(data)} records into {table_name}")

        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    db_config = {
        'dbname': DB_NAME,
        'user': DB_USER,
        'password': DB_PASSWORD,
        'host': DB_HOST,
        'port': DB_PORT,
    }
    api_url = 'http://localhost:8000/api/shifts'
    processor = ShiftDataProcessor(db_config, api_url)

    json_data = processor.fetch_data()
    if json_data:
        processor.process_json(json_data)
        processor.insert_data('shifts', ['shift_id', 'shift_date', 'shift_start', 'shift_finish', 'shift_cost'], processor.shifts)
        processor.insert_data('breaks', ['break_id', 'shift_id', 'break_start', 'break_finish', 'is_paid'], processor.breaks)
        processor.insert_data('allowances', ['allowance_id', 'shift_id', 'allowance_value', 'allowance_cost'], processor.allowances)
        processor.insert_data('award_interpretations', ['award_id', 'shift_id', 'award_date', 'award_units', 'award_cost'], processor.award_interpretations)
