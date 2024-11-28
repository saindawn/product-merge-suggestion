import logging,os
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def setup_logger(script_name):
    """
    Sets up a logger that writes to a log file named after the script calling it.
    """
    log_filename = f"{script_name}.log"
    log_path = os.path.join(os.getcwd(), log_filename)

    logger = logging.getLogger(script_name)
    if not logger.hasHandlers():  # Avoid adding multiple handlers to the same logger
        handler = logging.FileHandler(log_path)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

class DatabaseHandler:
    """Handles database operations."""
    def __init__(self, connection_string, script_name):
        self.engine = create_engine(connection_string)
        self.logger = setup_logger(script_name)

    def fetch_dataframe(self, query, **params):
        """Executes a SELECT query and returns the result as a Pandas DataFrame."""
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(query, con=conn, params=params)
        except SQLAlchemyError as e:
            self.logger.error(f"Database fetch error: {str(e)}")
            raise

    def fetch_group_title(self, query, **params):
        """Executes a SELECT query and parse to get `group_title`."""
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, con=conn, params=params)
                return tuple(df['group_title'].tolist())

        except SQLAlchemyError as e:
            self.logger.error(f"Database fetch error: {str(e)}")
            raise

    def execute(self, query, params=None):
        """Executes a non-SELECT query."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query), params)
                conn.commit()
        except SQLAlchemyError as e:
            self.logger.error(f"Database execution error: {str(e)}")
            raise

class MergeSuggestionManager:
    """Handles product duplicate merge logic."""
    def __init__(self, db_handler, script_name):
        self.db_handler = db_handler
        self.logger = setup_logger(script_name)
    
    def group_duplicates(self, detail_df):
        """Groups duplicate products details by title."""
        try:
            grouped_data = {}
            for index, row in detail_df.iterrows():
                extracted_data = {
                    'external_id': row['external_id'],
                    'product_id': row['id']
                }
                grouped_data.setdefault(row['group_title'], []).append(extracted_data)
            self.logger.info(f"Grouped duplicate products details into {len(grouped_data)} groups.")
            return grouped_data
        
        except KeyError as e:
            self.logger.error(f"Missing key during grouping: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error during grouping: {str(e)}")
            raise

    def _insert_product_duplicates(self, group_title):
        """Inserts a new product duplicate group and returns its ID."""
        try:
            self.db_handler.execute("""
                INSERT INTO product_duplicates (title, created_at, updated_at)
                VALUES (:title, NOW(), NOW())
            """, {'title': group_title})
            product_duplicate_id = self.db_handler.fetch_single_value("""
                SELECT id FROM product_duplicates
                WHERE title = :title
                ORDER BY created_at DESC LIMIT 1
            """, title=group_title)

            self.logger.info(f"Product duplicate inserted successfully with ID: {product_duplicate_id}")
            return product_duplicate_id
        
        except SQLAlchemyError as e:
            self.logger.error(f"Database error during product_duplicate insertion: {str(e)}")
            raise  # Reraise exception after logging

        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            raise  # Reraise exception for unhandled cases

    def _insert_product_duplicate_list(self, product_duplicate_id, duplicate_details):
        """Inserts details into `product_duplicate_lists`."""
        try:
            for detail in duplicate_details:
                detail['product_duplicate_id'] = product_duplicate_id
                self.db_handler.execute("""
                    INSERT INTO product_duplicate_lists (
                        product_duplicate_id, external_id, product_id, deleted_at, created_at, updated_at
                    ) VALUES (
                        :product_duplicate_id, :external_id, :product_id, NULL, NOW(), NOW()
                    )
                """, detail)

            self.logger.info(f"Inserted {len(duplicate_details)} product details for product_duplicate_id: {product_duplicate_id}")

        except SQLAlchemyError as e:
            self.logger.error(f"Database error during product_duplicate_lists insertion: {str(e)}")
            raise  # Reraise exception after logging

        except Exception as e:
            self.logger.error(f"Unexpected error during product_duplicate_lists insertion: {str(e)}")
            raise

    def insert_groups(self, grouped_data):
        """Inserts groups into `product_duplicates` and `product_duplicate_lists`."""
        for group_title, duplicate_details in grouped_data.items():
            product_duplicate_id = self._insert_product_duplicates(group_title)
            self._insert_product_duplicate_list(product_duplicate_id, duplicate_details)