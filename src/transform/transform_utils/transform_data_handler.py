import pandas as pd
import json
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from transform_utils.ingestion_s3_handler import (
    IngestionS3Handler,
)
import logging

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


class PandaTransformation:

    def __init__(self):
        load_dotenv()
        self.ingestion_bucket_name = os.getenv("S3_BUCKET_NAME")
        self.processed_bucket_name = os.getenv("PROCESSED_S3_BUCKET_NAME")
        self.dim_date_prefix = "dim_date/"
        self.s3_client = boto3.client("s3")
        self.ingestion_handler = IngestionS3Handler()
        self.raw_data = self.ingestion_handler.get_data_from_ingestion()

    def transform_currency_data(self):
        """
        Transforms currency data to match final database dimensions

        Returns:
            Dataframe: Currency data
        """
        try:
            with open("currencies_lookup.json", "r") as file:
                currencies_lookup = json.load(file)

            df_currency = pd.DataFrame(self.raw_data["currency"])
            df_currency.drop(
                columns=["created_at", "last_updated"], inplace=True
            )
            df_currency["currency_name"] = df_currency["currency_code"].map(
                currencies_lookup
            )
            logging.info("Currency data transformed successfully")
            return df_currency
        except Exception as e:
            logging.info(f"Unable to transform currency data due to {e}")
            return None

    def transform_location_data(self):
        """
        Transforms location data to match final database dimensions

        Returns:
            Dataframe: Location data
        """
        try:
            df_location = pd.DataFrame(self.raw_data["address"])
            del df_location["created_at"]
            del df_location["last_updated"]
            df_location.rename(
                columns={"address_id": "location_id"}, inplace=True
            )
            logging.info("Location data transformed successfully")
            return df_location
        except Exception as e:
            logging.info(f"Unable to transform location data due to {e}")
            return None

    def transform_staff_data(self):
        """
        Transforms staff data to match final database dimensions

        Returns:
            Dataframe: Staff data
        """
        try:
            df_staff = pd.DataFrame(self.raw_data["staff"])
            del df_staff["created_at"]
            del df_staff["last_updated"]
            df_department = pd.DataFrame(self.raw_data["department_all_data"])
            merged_df = pd.merge(
                df_staff, df_department, on="department_id", how="left"
            )
            del merged_df["department_id"]
            del merged_df["manager"]
            del merged_df["created_at"]
            del merged_df["last_updated"]
            logging.info("Staff data transformed successfully")
            return merged_df[
                [
                    "staff_id",
                    "first_name",
                    "last_name",
                    "department_name",
                    "location",
                    "email_address",
                ]
            ]
        except Exception as e:
            logging.info(f"Unable to transform staff data due to {e}")
            return None

    def transform_design_data(self):
        """
        Transforms design data to match final database dimensions

        Returns:
            Dataframe: Design data
        """
        try:
            df_design = pd.DataFrame(self.raw_data["design"])
            del df_design["created_at"]
            del df_design["last_updated"]
            logging.info("Design data transformed successfully")
            return df_design
        except Exception as e:
            logging.info(f"Unable to transform design data due to {e}")
            return None

    def transform_counterparty_data(self):
        """
        Transforms counterparty data to match final database dimensions

        Returns:
            Dataframe: Counterparty data
        """
        try:
            df_counterparty = pd.DataFrame(self.raw_data["counterparty"])
            del df_counterparty["created_at"]
            del df_counterparty["last_updated"]
            df_counterparty.rename(
                columns={"legal_address_id": "address_id"}, inplace=True
            )
            df_address = pd.DataFrame(self.raw_data["address_all_data"])
            merged_df = pd.merge(
                df_counterparty, df_address, on="address_id", how="left"
            )
            merged_df.rename(
                columns={
                    "address_line_1": "counterparty_legal_address_line_1",
                    "address_line_2": "counterparty_legal_address_line_2",
                    "district": "counterparty_legal_district",
                    "city": "counterparty_legal_city",
                    "postal_code": "counterparty_legal_postal_code",
                    "country": "counterparty_legal_country",
                    "phone": "counterparty_legal_phone_number",
                },
                inplace=True,
            )
            logging.info("Counterparty data transformed successfully")
            return merged_df[
                [
                    "counterparty_id",
                    "counterparty_legal_name",
                    "counterparty_legal_address_line_1",
                    "counterparty_legal_address_line_2",
                    "counterparty_legal_district",
                    "counterparty_legal_city",
                    "counterparty_legal_postal_code",
                    "counterparty_legal_country",
                    "counterparty_legal_phone_number",
                ]
            ]
        except Exception as e:
            logging.info(f"Unable to transform counterparty data due to {e}")
            return None

    def transform_sales_order_data(self):
        """
        Transforms sales order data to match final database dimensions

        Returns:
            Dataframe: Sales order data
        """
        try:
            df_sales_order = pd.DataFrame(self.raw_data["sales_order"])
            df_sales_order[["created_date", "created_time"]] = df_sales_order[
                "created_at"
            ].str.split(" ", n=1, expand=True)
            df_sales_order[["last_updated_date", "last_updated_time"]] = (
                df_sales_order["last_updated"].str.split(" ", n=1, expand=True)
            )
            df_sales_order.rename(
                columns={"staff_id": "sales_staff_id"}, inplace=True
            )
            logging.info("Sales order data obtained successfully")
            return df_sales_order[
                [
                    "sales_order_id",
                    "created_date",
                    "created_time",
                    "last_updated_date",
                    "last_updated_time",
                    "sales_staff_id",
                    "counterparty_id",
                    "units_sold",
                    "unit_price",
                    "currency_id",
                    "design_id",
                    "agreed_payment_date",
                    "agreed_delivery_date",
                    "agreed_delivery_location_id",
                ]
            ]
        except Exception as e:
            logging.info(f"Unable to transform sales order data due to {e}")
            return None

    def transform_date_data(self):
        """
        Obtains date data matching final database dimensions

        Returns:
            Dataframe: Date data
        """
        try:
            start_date = "2022-01-01"
            end_date = "2047-12-31"
            date_range = pd.date_range(
                start=start_date, end=end_date, freq="D"
            )
            dim_date = pd.DataFrame({"date_id": date_range})
            dim_date["year"] = dim_date["date_id"].dt.year
            dim_date["month"] = dim_date["date_id"].dt.month
            dim_date["day"] = dim_date["date_id"].dt.day
            dim_date["day_of_week"] = dim_date["date_id"].dt.dayofweek
            dim_date["day_name"] = dim_date["date_id"].dt.strftime("%A")
            dim_date["month_name"] = dim_date["date_id"].dt.strftime("%B")
            dim_date["quarter"] = dim_date["date_id"].dt.quarter
            logging.info("Date data obtained successfully")
            return dim_date

        except Exception as e:
            logging.error(f"ERROR: Creating dim_date: {e}")
            return None

    def check_date_file_exists(self):
        """
        Checks whether date file exists in the Processed S3 Bucket

        Returns:
            Boolean
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.processed_bucket_name, Prefix=self.dim_date_prefix
            )
            if "Contents" in response:
                logging.info(f"Files exist in {self.processed_bucket_name}")
                return True
            else:
                logging.info(
                    f"Files not found in {self.processed_bucket_name}"
                )
                return False
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logging.info("File does not exist.")
            else:
                logging.error(f"ERROR: Unexpected error occurred: {e}")
            return False

    def returns_dictionary_of_dataframes(self):
        """
        Collects dataframes into one dictionary

        Returns:
            Dict: Dataframes
        """
        try:
            transform_currency_data = (
                PandaTransformation.transform_currency_data
            )
            df_currency = transform_currency_data(self)
            logging.info("Created df_currencies")

            transform_location_data = (
                PandaTransformation.transform_location_data
            )
            df_location = transform_location_data(self)
            logging.info("Created df_location")

            transform_staff_data = PandaTransformation.transform_staff_data
            df_staff = transform_staff_data(self)
            logging.info("Created df_staff")

            transform_design_data = PandaTransformation.transform_design_data
            df_design = transform_design_data(self)
            logging.info("Created df_design")

            transform_counterparty_data = (
                PandaTransformation.transform_counterparty_data
            )
            df_counterparty = transform_counterparty_data(self)
            logging.info("Created df_counterparty")

            transform_sales_order_data = (
                PandaTransformation.transform_sales_order_data
            )
            df_sales_order = transform_sales_order_data(self)
            logging.info("Created df_sales_order")

            if self.check_date_file_exists():
                logging.info("File found - no new df_date generated")
                df_date = None
            else:
                transform_date_data = PandaTransformation.transform_date_data
                df_date = transform_date_data(self)
                logging.info("File not found - created df_date")

            initial_output = {
                "dim_currency": df_currency,
                "dim_location": df_location,
                "dim_staff": df_staff,
                "dim_design": df_design,
                "dim_counterparty": df_counterparty,
                "dim_date": df_date,
                "fact_sales_order": df_sales_order,
            }
            output = {}
            for key, value in initial_output.items():
                if value is not None:
                    output[key] = value
            logging.info("Data successfully collected")
            return output
        except Exception as e:
            logging.error(f"ERROR: Data unable to be collected due to {e}")
        return None
