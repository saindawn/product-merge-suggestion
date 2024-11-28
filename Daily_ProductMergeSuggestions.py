import os
from datetime import date, timedelta

from merge_utils import DatabaseHandler, MergeSuggestionManager, setup_logger

# Set up logging
filename = os.path.basename(__file__).replace('.py','')
logger   = setup_logger(filename)

class MergeSuggestion:
    """Handles deduplication logic."""
    def __init__(self, db_handler, merge_manager):
        self.db_handler = db_handler
        self.merge_manager = merge_manager
    
    def find_duplicate_title(self, start_interval_date, end_interval_date, description):
        """Fetches existing merged suggestions entries from the database."""
        try:
            new_and_unmarked_query = f"""
            SELECT
                group_title,
                COUNT(1)
            FROM (
            SELECT 
                CONCAT(UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(tags, 'ProductID: ', -1), ',', 1)),'-',
                UPPER(category),'-', 
                gender) group_title
            FROM 
                products
            WHERE 
                created_at >= %(start_interval_date)s 
                AND created_at < %(end_interval_date)s 
                AND deleted_at IS NULL
            UNION ALL
            SELECT 
                CONCAT(UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(tags, 'ProductID: ', -1), ',', 1)),'-',
                UPPER(category),'-', 
                gender) group_title
            FROM 
                products
            WHERE 
                id NOT IN (SELECT product_id FROM product_duplicate_lists)
                AND deleted_at IS NULL 
                AND created_at < %(start_interval_date)s) as init_data
            GROUP BY group_title
            HAVING COUNT(1) > 1
            """

            new_and_merged_query = f"""
            SELECT
                group_title,
                COUNT(1)
            FROM (
            SELECT 
                CONCAT(UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(tags, 'ProductID: ', -1), ',', 1)),'-',
                UPPER(category),'-', 
                gender) group_title
            FROM 
                products
            WHERE 
                created_at >= %(start_interval_date)s 
                AND created_at < %(end_interval_date)s 
                AND deleted_at IS NULL
            UNION ALL
            SELECT 
                title
            FROM 
                product_duplicates) as init_data
            GROUP BY group_title
            HAVING COUNT(1) > 1
            """

            if description == 'new_and_unmarked':
                exec_query = new_and_unmarked_query
            else:
                exec_query = new_and_merged_query

            params = {
            "start_interval_date": start_interval_date,
            "end_interval_date": end_interval_date,
            }
            return self.db_handler.fetch_group_title(exec_query, **params)
        except Exception as e:
            logger.error(f"Error fetching merged suggestions: {str(e)}")
            raise
    
    def get_data_details(self, group_titles_tuple):
        try:
            products_query = f"""
                SELECT 
                    CONCAT(UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(tags, 'ProductID: ', -1), ',', 1)),'-',
                    UPPER(category),'-', 
                    gender) group_title,
                    id,
                    external_id
                FROM 
                    products
                WHERE 
                    deleted_at IS NULL
                    AND CONCAT(UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(tags, 'ProductID: ', -1), ',', 1)),'-',
                    UPPER(category),'-', 
                    gender) IN {group_titles_tuple}
                """
            products_df = self.db_handler.fetch_dataframe(products_query)
            
            return products_df
        except Exception as e:
            logger.error(f"Error during merge of new products: {str(e)}")
            raise

    def grouped_data_details(self, group_title1, group_title2):
        grouped_data = {}

        def get_and_group(group_title,i):
            try:
                data_details = self.get_data_details(group_title)
                return self.merge_manager.group_duplicates(data_details)
            except Exception as e:
                logger.error(f"Error processing {group_title}: {e}")
                return {}

        group_titles = [group_title1, group_title2]

        for i, group_title in enumerate(group_titles):
            if group_title:
                grouped_data.update(get_and_group(group_title,i))
        
        return grouped_data

def main():
    try:
        connection_string = "mysql+pymysql://<user>:<password>@localhost/<database-name>"
        db_handler = DatabaseHandler(connection_string,filename)
        merge_manager = MergeSuggestionManager(db_handler,filename)
        merge = MergeSuggestion(db_handler,merge_manager)

        start_interval_date = (date.today()-timedelta(days=1)).isoformat()
        end_interval_date   = date.today().isoformat()

        # get duplicate data for new products and umarked products
        new_and_unmarked_group_title = merge.find_duplicate_title(start_interval_date,end_interval_date,description='new_and_unmarked')
        new_and_merged_group_title = merge.find_duplicate_title(start_interval_date,end_interval_date,description='new_and_merged')        

        merged_data = merge.grouped_data_details(new_and_unmarked_group_title,new_and_merged_group_title)
        
        merge_manager.insert_groups(merged_data)

        logger.info("Deduplication process completed successfully.")
    except Exception as e:
        logger.critical(f"Critical error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
