import os
from http.client import HTTPException

from fastapi import FastAPI
from uvicorn import run
import boto3
from boto3.session import Session
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from botocore.exceptions import NoCredentialsError
import pandas as pd
from io import BytesIO


aws_access_key = ''
aws_secret_key = '/'
aws_bucket = ''
file_name = f'.xlsx'
file_list = {"order_file": f'.xlsx',
             "stock_file": f'.xlsx'}


app = FastAPI(
    title="",

)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)


# app.include_router(all_routes)
# to join path of directory
running_destination = os.path.join(os.getcwd(), 'data')
download_destination = os.path.join(os.getcwd(), 'data')


# to create aws client connection

def get_aws_client_connection():
    global aws_access_key, aws_secret_key, aws_bucket, file_name, file_list
    try:
        client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,

        )
        # to get list of stock files from aws
        obj = client.get_object(
            Bucket=aws_bucket,
            Key=file_list['stock_file']
        )
        # print(obj['Body'].read())
        read_access_stock_file_data(obj['Body'].read(), client, 'stock')

        # to get list of order files from aws
        obj = client.get_object(
            Bucket=aws_bucket,
            Key=file_list['order_file']
        )
        # print(obj['Body'].read())
        read_access_stock_file_data(obj['Body'].read(), client, 'stock')
    except HTTPException as e:
        logger.debug(f'{e}')
        raise e
    except Exception as e:
        logger.error(f"Error in get_aws_client_connection: {e}")


def read_access_stock_file_data(data, client, file):
    try:
        xls1 = pd.read_excel(BytesIO(data), engine='openpyxl')
        rows1 = xls1.to_dict('records')
        if file == 'stock':
            response = write_stock_file_data(rows1)
            if response['data'] == "success":
                logger.info(f"{file_name} File processed successfully")
                tel_msg(f"SU {len(file_name)} file processed successfully")
        elif file == 'order':
            response = write_order_file_data(rows1)
            if response['data'] == "success":
                logger.info(f"{file_name} File processed successfully")
                tel_msg(f"SU {len(file_name)} file processed successfully")
        # to access current working file form my storage and read it
        client.download_file(aws_bucket, file_name, running_destination)
        # # to move file from running to processed
        # shutil.move(running_destination, processed_destination)
    except HTTPException as e:
        logger.debug(f'{e}')
        raise e
    except Exception as e:
        logger.error(f"Error in read_access_file_data: {e}")


def get_aws_session():
    logger.info("Started get_aws_session")
    try:

        # this is using session #####################3
        # session = Session(aws_access_key_id=aws_access_key,
        #                   aws_secret_access_key=aws_secret_key)
        #
        # # session is authenticated and can access the resource in question
        # session.resource('s3', aws_access_key_id=aws_access_key,
        #                  aws_secret_access_key=aws_secret_key,
        #                  ).Bucket(aws_bucket).download_file('Style Union Stock Report_2023-01-27.xlsx',
        #                                                     '/home/smartiam/Desktop/Style Union Stock Report_2023-01-28.xlsx')
        ################################################################

        # this is using resource #####################
        # s3 = boto3.resource('s3', aws_access_key_id=aws_access_key,
        #                     aws_secret_access_key=aws_secret_key)
        #
        # for key in s3.Bucket(aws_bucket).objects.all():
        #     print(key['Key'])

        # for bucket in s3.buckets.all():
        #     logger.debug(f"BUCKET NAME: {bucket.name}")
        #############################################################################
        # this is  using d6tpipe keep permissions locally for security
        #     import d6tpipe
        #     api = d6tpipe.api.APILocal()  # keep permissions locally for security
        #
        #     settings = \
        #         {
        #             'name': 'my-files',
        #             'protocol': 's3',
        #             'location': aws_bucket,
        #             'readCredentials': {
        #                 'aws_access_key_id': aws_access_key,
        #                 'aws_secret_access_key': aws_secret_key
        #             }
        #         }
        #
        #     d6tpipe.api.create_pipe_with_remote(api, settings)
        #
        #     pipe = d6tpipe.Pipe(api, 'my-files')
        #     pipe.scan_remote()  # show all files
        #     pipe.pull_preview()  # preview
        #     pipe.pull(['Style Union Stock Report_2023-01-27.xlsx'])  # download single file
        #     pipe.pull()  # download all files
        #
        #     pipe.files()  # show files
        #     file = open(pipe.dirpath / 'Style Union Stock Report_2023-01-27.xlsx')  # access file
        ########################################################################
        # this is using boto3 session #####################
        # session = Session(aws_access_key_id=aws_access_key,
        #                   aws_secret_access_key=aws_secret_key,
        #                   region_name="eu-west-1")
        #
        # session.resource('s3', aws_access_key_id=aws_access_key,
        #                  aws_secret_access_key=aws_secret_key, ).Bucket(aws_bucket).download_file(
        #     Key="logs/20221122_0_5ee03da676ac566336e2279decfc77b3.gz", Filename=file_name)
        ################################################################
        ############ using client   ############################
        client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,

        )
        # clientResponse = client.list_buckets()
        #
        # # Print the bucket names one by one
        # print('Printing bucket names...')
        # for bucket in clientResponse['Buckets']:
        #     print(f'Bucket Name: {bucket["Name"]}')
        obj = client.get_object(
            Bucket=aws_bucket,
            Key=file_name
        )
        # print(obj['Body'].read())
        pd.read_excel(BytesIO(obj['Body'].read()), engine='openpyxl')
        client.download_file(aws_bucket, file_name, destination)
        dst_path = "/home/smartiam/Desktop/Amol/tagsmart-api-test/data/processed/"
        shutil.move(src_path, dst_path)
        ########################################################
        ################# using access bucket obhjects ############################
        # how to access bucket objects using boto3 resource
        # access denied error
        # s3 = boto3.resource('s3', aws_access_key_id=aws_access_key,
        #                     aws_secret_access_key=aws_secret_key)
        # bucket = s3.Bucket(aws_bucket)
        # for obj in bucket.objects.all():
        #     print(obj.key)
        #     print(obj.last_modified)
        #     print(obj.size)
        #     print(obj.storage_class)
        #     print(obj.e_tag)
        #     print(obj.version_id)
        #     print(obj.website_redirect_location)
        #     print(obj.metadata)
        #     print(obj.content_type)
        # An error occurred (AccessDenied) when calling the ListObjects operation: Access Denied




    except FileNotFoundError:

        logger.error("The file was not found")

        return False

    except NoCredentialsError:

        logger.error("Credentials not available")

        return False

    except Exception as e:

        logger.error(f"{e}")


if __name__ == '__main__':
    logger.info("Started main")
    get_aws_session()
    # run("main:app", host="0.0.0.0", port=5017, reload=True)
