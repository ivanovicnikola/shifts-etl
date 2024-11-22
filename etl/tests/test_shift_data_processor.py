import logging
import unittest
from unittest.mock import Mock, patch, MagicMock
import psycopg2
from app.shift_data_processor import ShiftDataProcessor

# Configure logging to output to the console
logging.basicConfig(
    level=logging.DEBUG,  # Set the level to DEBUG to capture all log messages
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Define log format
    handlers=[logging.StreamHandler()]  # This will output logs to the console
)

class TestShiftDataProcessor(unittest.TestCase):

    def setUp(self):
        # Configure test database
        self.db_config = {
            'dbname': 'test_db',
            'user': 'postgres',
            'password': 'postgres',
            'host': 'localhost',
            'port': 5433
        }

        # Setup the test database
        self.setup_test_database(self.db_config, '../initdb.sql')
    
    def tearDown(self):
        # Clear all tables in the database
        self.clear_all_tables(self.db_config)

    def clear_all_tables(self, db_config):
        """Clears all tables in the test database."""
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()

            cursor.execute("DELETE FROM shifts CASCADE;")
            cursor.execute("DELETE FROM kpis CASCADE;")
            
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error while clearing tables: {e}")

    @patch('requests.get')  # Mocking the requests.get function
    def test_process_and_insert_data(self, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {
                    "id": "b2b9437a-28df-4ec4-8e4a-2bbdc241330b",
                    "date": "2023-11-27",
                    "start": 1701077400000,
                    "finish": 1701108900000,
                    "breaks": [
                        {
                            "id": "16419f82-8b9d-4434-a465-e150bd9c66b3",
                            "start": 1701085620000,
                            "finish": 1701087005277,
                            "paid": False
                        }
                    ],
                    "allowances": [
                        {
                            "id": "815ef6d1-3b8f-4a18-b7f8-a88b17fc695a",
                            "value": 0.5,
                            "cost": 2.5
                        },
                        {
                            "id": "b38a088c-a65e-4389-b74d-0fb132e70629",
                            "value": 0.5,
                            "cost": 29.7
                        },
                        {
                            "id": "cf36d58b-4737-4190-96da-1dac72ff5d2a",
                            "value": 1.5,
                            "cost": 12.2
                        }
                    ],
                    "award_interpretations": []
                },
                {
                    "id": "d453dd32-4b0d-4b41-8d52-88f1142c3fe8",
                    "date": "2023-11-28",
                    "start": 1701160200000,
                    "finish": 1701198000000,
                    "breaks": [
                        {
                            "id": "6142ea7d-17be-4111-9a2a-73ed562b0f79",
                            "start": 1701168180000,
                            "finish": 1701169724388,
                            "paid": True
                        }
                    ],
                    "allowances": [],
                    "award_interpretations": [
                        {
                            "id": "bacfb3d0-0b1f-4163-8e9f-f57f43b7a3a6",
                            "date": "2023-11-28",
                            "units": 1.0,
                            "cost": 62.8
                        },
                        {
                            "id": "60e7a113-ec1b-4ca1-b91e-1d4c1ff49b78",
                            "date": "2023-11-28",
                            "units": 1.5,
                            "cost": 55.9
                        }
                    ]}
                ]
            }
        mock_get.return_value = mock_response

        processor = ShiftDataProcessor(db_config=self.db_config, api_url="http://localhost:8000/api/shifts")

        json_data = mock_response.json()
        processor.process_and_insert_data(json_data)

        self.verify_inserted_data(self.db_config)

        self.clear_all_tables(self.db_config)

    @patch('requests.get')  # Mock the requests.get method
    def test_process_all_pages(self, mock_get):
        # Define the mock behavior
        def mock_get_response(url, params=None):
            if url == 'http://localhost:8000/api/shifts':
                # Return a mock response for the first page
                response = Mock()
                response.status_code = 200
                response.json.return_value = {
                    "results": [
                        {
                            "id": "b2b9437a-28df-4ec4-8e4a-2bbdc241330b",
                            "date": "2023-11-27",
                            "start": 1701077400000,
                            "finish": 1701108900000,
                            "breaks": [
                                {
                                    "id": "16419f82-8b9d-4434-a465-e150bd9c66b3",
                                    "start": 1701085620000,
                                    "finish": 1701087005277,
                                    "paid": False
                                }
                            ],
                            "allowances": [
                                {
                                    "id": "815ef6d1-3b8f-4a18-b7f8-a88b17fc695a",
                                    "value": 0.5,
                                    "cost": 2.5
                                },
                                {
                                    "id": "b38a088c-a65e-4389-b74d-0fb132e70629",
                                    "value": 0.5,
                                    "cost": 29.7
                                },
                                {
                                    "id": "cf36d58b-4737-4190-96da-1dac72ff5d2a",
                                    "value": 1.5,
                                    "cost": 12.2
                                }
                            ],
                            "award_interpretations": []
                        }
                        ],
                        "links": {
                        "base": "http://localhost:8000",
                        "next": "/api/shifts?start=1&limit=1"
                    },
                    "start": 0,
                    "limit": 1,
                    "size": 1
                    }
                return response
            elif url == 'http://localhost:8000/api/shifts?start=1&limit=1':
                # Return a mock response for the second page
                response = Mock()
                response.status_code = 200
                response.json.return_value = {
                    "results": [
                        {
                            "id": "d453dd32-4b0d-4b41-8d52-88f1142c3fe8",
                            "date": "2023-11-28",
                            "start": 1701160200000,
                            "finish": 1701198000000,
                            "breaks": [
                                {
                                    "id": "6142ea7d-17be-4111-9a2a-73ed562b0f79",
                                    "start": 1701168180000,
                                    "finish": 1701169724388,
                                    "paid": True
                                }
                            ],
                            "allowances": [],
                            "award_interpretations": [
                                {
                                    "id": "bacfb3d0-0b1f-4163-8e9f-f57f43b7a3a6",
                                    "date": "2023-11-28",
                                    "units": 1.0,
                                    "cost": 62.8
                                },
                                {
                                    "id": "60e7a113-ec1b-4ca1-b91e-1d4c1ff49b78",
                                    "date": "2023-11-28",
                                    "units": 1.5,
                                    "cost": 55.9
                                }
                            ]}
                        ],
                        "links": {
                        "base": "http://localhost:8000",
                        "prev": "/api/shifts?start=0&limit=1"
                    },
                    "start": 1,
                    "limit": 1,
                    "size": 1
                    }
                return response
            else:
                # Mock response for unknown URLs
                response = Mock()
                response.status_code = 404
                response.json.return_value = {'error': 'Not found'}
                return response

        # Set the side effect of the mock
        mock_get.side_effect = mock_get_response

        processor = ShiftDataProcessor(db_config=self.db_config, api_url="http://localhost:8000/api/shifts")

        processor.process_all_pages()

        self.verify_inserted_data(self.db_config)
        self.verify_computed_kpis(self.db_config)

        self.clear_all_tables(self.db_config)

    def test_bulk_insert_failure(self):
        # Sample data where the second shift has the same break_id as the first
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {
                    "id": "b2b9437a-28df-4ec4-8e4a-2bbdc241330b",
                    "date": "2023-11-27",
                    "start": 1701077400000,
                    "finish": 1701108900000,
                    "breaks": [
                        {
                            "id": "16419f82-8b9d-4434-a465-e150bd9c66b3",
                            "start": 1701085620000,
                            "finish": 1701087005277,
                            "paid": False
                        }
                    ],
                    "allowances": [],
                    "award_interpretations": []
                },
                {
                    "id": "d453dd32-4b0d-4b41-8d52-88f1142c3fe8",
                    "date": "2023-11-28",
                    "start": 1701160200000,
                    "finish": 1701198000000,
                    "breaks": [
                        {
                            "id": "16419f82-8b9d-4434-a465-e150bd9c66b3",  # Same break_id as the previous shift
                            "start": 1701168180000,
                            "finish": 1701169724388,
                            "paid": True
                        }
                    ],
                    "allowances": [],
                    "award_interpretations": []
                }
            ]
        }

        processor = ShiftDataProcessor(db_config=self.db_config, api_url='http://localhost:8000/api/shifts')

        # Simulate inserting the data, but raise an exception for the duplicate break_id
        with self.assertRaises(psycopg2.IntegrityError):
            processor.process_and_insert_data(json_data=mock_response.json())

        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            # Assert that the data was not inserted into the database
            cursor.execute("SELECT * FROM shifts")
            shifts = cursor.fetchall()
            self.assertEqual(shifts, [])

            cursor.execute("SELECT * FROM breaks")
            breaks = cursor.fetchall()
            self.assertEqual(breaks, [])
        except Exception as e:
            logging.error(f"Error verifying inserted data: {e}")
            raise

    def setup_test_database(self, db_config, sql_file_path):
        """Sets up the test database tables."""
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()

            with open(sql_file_path, 'r') as sql_file:
                sql_commands = sql_file.read()

            cursor.execute(sql_commands)
            conn.commit()

            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error setting up database: {e}")
            raise

    def verify_inserted_data(self, db_config):
        """Verifies that the data was inserted correctly into the database."""
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()

            # Check if shifts were inserted
            cursor.execute("SELECT COUNT(*) FROM shifts")
            shift_count = cursor.fetchone()[0]
            self.assertEqual(shift_count, 2, "Expected 2 shifts to be inserted")

            # Check if breaks were inserted
            cursor.execute("SELECT COUNT(*) FROM breaks")
            break_count = cursor.fetchone()[0]
            self.assertEqual(break_count, 2, "Expected 2 breaks to be inserted")

            # Check if allowances were inserted
            cursor.execute("SELECT COUNT(*) FROM allowances")
            allowance_count = cursor.fetchone()[0]
            self.assertEqual(allowance_count, 3, "Expected 3 allowance to be inserted")

            # Check if award_interpretations were inserted
            cursor.execute("SELECT COUNT(*) FROM award_interpretations")
            award_count = cursor.fetchone()[0]
            self.assertEqual(award_count, 2, "Expected 2 awards to be inserted")

            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error verifying inserted data: {e}")
            raise

    def verify_computed_kpis(self, db_config):
        """Verifies that KPIs are computed and inserted into the kpis table."""
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()

            # Check if KPIs were inserted
            cursor.execute("SELECT COUNT(*) FROM kpis")
            kpi_count = cursor.fetchone()[0]
            self.assertEqual(kpi_count, 6, "Expected 6 KPIs to be inserted")

            # Check specific KPI values
            cursor.execute("SELECT kpi_value FROM kpis WHERE kpi_name = 'mean_break_length_in_minutes'")
            mean_break_length = cursor.fetchone()[0]
            self.assertEqual(float(mean_break_length), 24.41, "Expected 'mean_break_length_in_minutes' value is 24.41")

            cursor.execute("SELECT kpi_value FROM kpis WHERE kpi_name = 'mean_shift_cost'")
            mean_shift_cost = cursor.fetchone()[0]
            self.assertEqual(float(mean_shift_cost), 81.55, "Expected 'mean_shift_cost' value is 81.55")

            cursor.execute("SELECT kpi_value FROM kpis WHERE kpi_name = 'max_allowance_cost_14d'")
            max_allowance_cost = cursor.fetchone()[0]
            self.assertEqual(float(max_allowance_cost), 0, "Expected 'max_allowance_cost_14d' value is 0")

            cursor.execute("SELECT kpi_value FROM kpis WHERE kpi_name = 'max_break_free_shift_period_in_days'")
            max_break_free_shift_period = cursor.fetchone()[0]
            self.assertEqual(float(max_break_free_shift_period), 0, "Expected 'max_break_free_shift_period_in_days' value is 0")

            cursor.execute("SELECT kpi_value FROM kpis WHERE kpi_name = 'min_shift_length_in_hours'")
            min_shift_length = cursor.fetchone()[0]
            self.assertEqual(float(min_shift_length), 8.75, "Expected 'min_shift_length_in_hours' value is 8.75")

            cursor.execute("SELECT kpi_value FROM kpis WHERE kpi_name = 'total_number_of_paid_breaks'")
            number_of_paid_breaks = cursor.fetchone()[0]
            self.assertEqual(float(number_of_paid_breaks), 1, "Expected 'total_number_of_paid_breaks' value is 1")

            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error verifying computed KPIs: {e}")
            raise

if __name__ == '__main__':
    unittest.main()
