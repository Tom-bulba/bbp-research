import boto3
import pandas as pd
import io
import os


class AWSHandler():
    """
    AWSHandler get/put file in s3 bucket
    """
    def __init__(self, process_name):
        self.process_name = process_name

    def get_s3_client(self):
        """
        get_s3_client initialize connection to s3
        """
        try:
            self.s3_client = boto3.client('s3')
        except Exception as e:
            # logger.error(f"process name {self.process_name}, could not connect to bucket {self.bucket_name} with error {e}")
            print(e)
    
    def build_file_path_in_s3(self, working_date, file_name):
        """
        build_file_path_in_s3 build file path inside the s3 bucket

        Parameters
        ----------
        working_date : string
            working date in the form of DD-MM-YYYY
        file_name : string
            file name to get back, example market_data.csv

        Returns
        -------
        string
            full file path and name reletive inside the s3 bucket
        """
        file_path = os.path.join(working_date, file_name)
        return file_path


    def get_file_from_bucket(self, working_date, file_name_to_get, bucket_name, file_column_names:list = []):
        """
        get_file_from_bucket load the file, from the working_date folder inside the bucket into the memory

        Parameters
        ----------
        working_date : string
            working date in the form of DD-MM-YYYY
        file_name_to_get : string
            file name to get back, example market_data.csv
        bucket_name : string
            bucket name as in s3
        file_column_names: list
            list of column names to use as csv headers

        Returns
        -------
        DataFrame
            df with the file data inside
        """
        self.get_s3_client()
        file_path = self.build_file_path_in_s3(working_date, file_name_to_get)
        response = self.s3_client.get_object(Bucket=bucket_name, 
                                             Key=file_path)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        df_data = None
        if status == 200:
            # logger.info(f"process name {self.process_name}, Successful S3 get_object response. Status - {status}")
            # merged_raw_data = pd.read_csv(response.get("Body")) #TODO: uncomment this!!!!
            if len(file_column_names) > 0:
                # df_data = pd.read_csv(response.get("Body"), names=file_column_names, nrows=10000, skiprows=200000)
                df_data = pd.read_csv(response.get("Body"), names=file_column_names, header=0)
            else:
                df_data = pd.read_csv(response.get("Body"), nrows=10000, skiprows=200000)
                # df_data = pd.read_csv(response.get("Body"), nrows=10000)
                # df_data = pd.read_csv(response.get("Body"))
        else:
            # logger.error(f"process name {self.process_name}, Unsuccessful S3 get_object response. Status - {status}")
            print(status)
        return df_data

    def check_if_folder_exists_in_bucket(self, bucket_name, folder_path):
        folder_path_to_check = f"{folder_path}/"
        folder_exist = False
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=folder_path_to_check)
            folder_exist = True
        except Exception as e:
            # logger.error(f"process name {self.process_name}, could not connect to bucket {bucket_name} and folder {folder_path_to_check} with error {e}")
            print(e)
        return folder_exist

    def create_folder_in_bucket(self, bucket_name, working_date):
        """
        create_folder_in_bucket create a new folder in s3 bucket

        Parameters
        ----------
        bucket_name : string
            bucket name as in s3
        working_date : string
            working date in the form of DD-MM-YYYY
        """
        self.s3_client.put_object(Bucket=bucket_name, Key=(working_date+'/'))

    def save_file_in_bucket(self, working_date, df_to_save, file_name_to_save, bucket_name):
        """
        save_file_in_bucket save file in csv format in the bucket name, inside the working date folder

        Parameters
        ----------
        working_date : string
            working date in the form of DD-MM-YYYY
        df_to_save : DataFrame
            df to save
        file_name_to_save : string
            file name to use to save the dataframe, for example market_data.csv
        bucket_name : string
            bucket name as in s3
        """
        self.get_s3_client()
        if not self.check_if_folder_exists_in_bucket(bucket_name, working_date):
            self.create_folder_in_bucket(bucket_name, working_date)
        file_path = self.build_file_path_in_s3(working_date, file_name_to_save)
        with io.StringIO() as csv_buffer:
            df_to_save.to_csv(csv_buffer, index=False)
            response = self.s3_client.put_object(Bucket=bucket_name, 
                                                 Key=file_path, 
                                                 Body=csv_buffer.getvalue())
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                # logger.info(f"process name {self.process_name}, Successful S3 put_object response. Status - {status}")
                print(status)
            else:
                # logger.error(f"process name {self.process_name}, Unsuccessful S3 put_object response. Status - {status}")
                print(status)


# if __name__ == "__main__":
#     aws_handler = AWSHandler("testing")
#     merged_raw_data = aws_handler.get_file_from_bucket("02-12-2021", "ebs_market_data.csv", "ebs-trading-arena")
#     print(merged_raw_data.head(10))
    