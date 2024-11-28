import os
from datetime import date

from merge_utils import DatabaseHandler, MergeSuggestionManager, setup_logger


filename = os.path.basename(__file__).replace('.py','')
logger   = setup_logger(filename)

def main():
    try:
        # Database configuration
        connection_string = "mysql+pymysql://root:admin@localhost/baltiny_dummy1"
        db_handler = DatabaseHandler(connection_string,filename)

        # Initialize the deduplication process
        merge = MergeSuggestionManager(db_handler,filename)

        # Fetch all products
        initiation_date = date.now().isoformat()
        products_query = f"""
        SELECT 
            CONCAT(UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(tags, 'ProductID: ', -1), ',', 1)),'-',
            UPPER(category),'-', 
            gender) group_title,
            COUNT(1)
        FROM 
            products
        WHERE 
            deleted_at IS NULL
            AND created_at < %(initiation_date)s
        GROUP BY group_title
        HAVING COUNT(1) > 1
        """
        params = {"initiation_date": initiation_date}
        group_titles_tuple = db_handler.fetch_group_title(products_query,
                                                        **params)

        # Step 4: Construct the next query, ensuring parameterized use
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
        products_df = db_handler.fetch_dataframe(products_query)
        grouped_data = merge.group_duplicates(products_df)
        print(len(grouped_data))
        merge.insert_groups(grouped_data)
    
        logger.info("Initiation product merge suggestions process completed successfully.")
    except Exception as e:
        logger.critical(f"Critical error in main function: {str(e)}")
        raise


if __name__ == "__main__":
    main()
