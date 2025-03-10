import logging

from data_warehouse_loader import DataWarehouseLoader


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def load_main_script(event, context):
    try:
        logging.info("Uploading to DataWarehouse started")
        loader = DataWarehouseLoader()
        loader.process_new_files()
        logging.info("Data uploaded to DataWarehouse successfully")

        return {"message":"Updated successfully"}

    except Exception as e:
       logging.error(f"ERROR - Update failed:{e}")
       return {"message": "Update failed"}