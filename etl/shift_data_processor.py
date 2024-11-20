from datetime import datetime
import requests
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import execute_values
from urllib.parse import urljoin
import logging

DB_HOST = 'localhost'
DB_PORT = '5433'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'

# Configure logging to output to the console
logging.basicConfig(
    level=logging.DEBUG,  # Set the level to DEBUG to capture all log messages
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Define log format
    handlers=[logging.StreamHandler()]  # This will output logs to the console
)

class ShiftDataProcessor:
    def __init__(self, db_config: Dict[str, str], api_url: str):
        self.db_config = db_config
        self.api_url = api_url
        self.shifts = []
        self.breaks = []
        self.allowances = []
        self.award_interpretations = []

    def fetch_data(self, url: str) -> Optional[Dict]:
        """Fetches data from the API."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred: {e}")
            return None
        except ValueError as e:
            logging.error(f"Error parsing JSON: {e}")
            return None

    @staticmethod
    def map_dict_keys(data: List[Dict], column_mapping: Dict[str, str]) -> List[Dict]:
        """Maps the dictionary keys to new names based on the provided mapping."""
        return [
            {column_mapping.get(key, key): value for key, value in record.items()}
            for record in data
        ]

    @staticmethod
    def process_nested_records(results: List[Dict], record_key: str, parent_key: str) -> List[Dict]:
        """Processes nested records to flatten the structure."""
        return [
            {**record, parent_key: result['id']}
            for result in results
            for record in result[record_key]
        ]

    def process_json(self, json_data: Dict) -> None:
        """Processes the JSON response and extracts relevant data."""
        results = json_data['results']

        # Process the different components of the data
        self.shifts = self.process_shifts(results)
        self.breaks = self.process_breaks(results)
        self.allowances = self.process_allowances(results)
        self.award_interpretations = self.process_award_interpretations(results)

    def process_shifts(self, results: List[Dict]) -> List[Dict]:
        """Processes shift data."""
        return [
            {
                'shift_id': result['id'],
                'shift_date': result['date'],
                'shift_cost': round(
                    sum(allowance['cost'] for allowance in result['allowances']) + 
                    sum(award['cost'] for award in result['award_interpretations']),
                    4
                ),
                'shift_start': self.parse_timestamp(result.get('start')),
                'shift_finish': self.parse_timestamp(result.get('finish'))
            }
            for result in results
        ]

    def process_breaks(self, results: List[Dict]) -> List[Dict]:
        """Processes break data."""
        return [
            {
                'break_id': break_record['id'],
                'shift_id': result['id'],
                'break_start': self.parse_timestamp(break_record.get('start')),
                'break_finish': self.parse_timestamp(break_record.get('finish')),
                'is_paid': break_record['paid']
            }
            for result in results
            for break_record in result['breaks']
        ]

    def process_allowances(self, results: List[Dict]) -> List[Dict]:
        """Processes allowance data."""
        return self.map_dict_keys(
            self.process_nested_records(results, 'allowances', 'shift_id'),
            {'id': 'allowance_id', 'value': 'allowance_value', 'cost': 'allowance_cost'}
        )

    def process_award_interpretations(self, results: List[Dict]) -> List[Dict]:
        """Processes award interpretation data."""
        return self.map_dict_keys(
            self.process_nested_records(results, 'award_interpretations', 'shift_id'),
            {'id': 'award_id', 'date': 'award_date', 'units': 'award_units', 'cost': 'award_cost'}
        )

    def parse_timestamp(self, timestamp: Optional[int]) -> Optional[datetime]:
        """Converts a timestamp to a datetime object, or returns None if invalid."""
        if isinstance(timestamp, int) and timestamp > 0:
            return datetime.fromtimestamp(timestamp / 1000)
        return None

    def insert_data(self, table_name: str, columns: List[str], data: List[Dict]) -> None:
        """Inserts data into the database."""
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
            logging.info(f"Successfully inserted {len(data)} records into {table_name}")

        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def get_next_url(self, json_data):
        """Extracts the next page URL from the response data."""
        next_url = json_data['links'].get('next', None)
        base_url = json_data['links'].get('base', None)
        if next_url and base_url:
            return urljoin(base_url, next_url)
        return None

    def process_and_insert_data(self, json_data):
        """Process the current page and insert data into the database."""
        # Process data for this page
        self.process_json(json_data)

        # Insert data for each table
        self.insert_data('shifts', ['shift_id', 'shift_date', 'shift_start', 'shift_finish', 'shift_cost'], self.shifts)
        self.insert_data('breaks', ['break_id', 'shift_id', 'break_start', 'break_finish', 'is_paid'], self.breaks)
        self.insert_data('allowances', ['allowance_id', 'shift_id', 'allowance_value', 'allowance_cost'], self.allowances)
        self.insert_data('award_interpretations', ['award_id', 'shift_id', 'award_date', 'award_units', 'award_cost'], self.award_interpretations)

    def process_all_pages(self):
        """Fetches, processes, and inserts data for all pages."""
        url = self.api_url
        while url:
            try:
                # Fetch the data for the current page
                json_data = self.fetch_data(url)
                if json_data:
                    self.process_and_insert_data(json_data)

                    # Get the next page URL and update `url`
                    url = self.get_next_url(json_data)
                    if url:
                        logging.info(f"Fetching next page: {url}")
                    else:
                        logging.info("No more pages to fetch.")
                        break  # Exit loop if no 'next' URL is found
                else:
                    logging.error("Failed to fetch data, stopping.")
                    break  # Stop if data fetch failed

            except Exception as e:
                logging.error(f"Error processing data: {e}")
                break  # Stop the loop on error

        # After all pages are processed, compute KPIs
        self.compute_kpis()

    def compute_kpis(self) -> None:
        """
        Computes and inserts KPIs into the kpis table in a single query.
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            insert_query = """
            INSERT INTO kpis (kpi_name, kpi_date, kpi_value)
            VALUES
            (
                'mean_break_length_in_minutes', 
                CURRENT_DATE, 
                (SELECT EXTRACT(EPOCH FROM AVG(break_finish - break_start)) / 60 FROM breaks)
            ),
            (
                'mean_shift_cost',
                CURRENT_DATE,
                (SELECT AVG(shift_cost) FROM shifts)
            ),
            (
                'max_allowance_cost_14d',
                CURRENT_DATE,
                (
                    SELECT MAX(allowance_cost) 
                    FROM allowances 
                    INNER JOIN shifts ON allowances.shift_id = shifts.shift_id 
                    WHERE shift_date >= CURRENT_DATE - INTERVAL '14 days'
                )
            ),
            (
                'max_break_free_shift_period_in_days',
                CURRENT_DATE,
                (
                    WITH grps AS (
                        SELECT *,
                            SUM(CASE WHEN break_id IS NULL THEN 1 ELSE 0 END) OVER(ORDER BY shift_date) AS grp
                        FROM shifts
                        LEFT JOIN breaks ON shifts.shift_id = breaks.shift_id
                    )
                    SELECT COUNT(*) - CASE WHEN grp = 0 THEN 0 ELSE 1 END AS cnt
                    FROM grps
                    GROUP BY grp
                    ORDER BY cnt DESC 
                    LIMIT 1
                )
            ),
            (
                'min_shift_length_in_hours',
                CURRENT_DATE,
                (SELECT MIN(EXTRACT(EPOCH FROM (shift_finish - shift_start)) / 3600) FROM shifts)
            ),
            (
                'total_number_of_paid_breaks',
                CURRENT_DATE,
                (SELECT COUNT(*) FROM breaks WHERE is_paid = true)
            );
            """
            
            cursor.execute(insert_query)

            conn.commit()

            logging.info("Successfully inserted KPI values into the kpis table")

        except Exception as e:
            logging.error(f"Error computing and inserting KPIs: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
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
    processor.process_all_pages()
