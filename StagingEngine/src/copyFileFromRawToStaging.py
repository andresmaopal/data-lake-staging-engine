import re
import traceback

import boto3
from dateutil import parser
from dateutil.tz import gettz
import awswrangler
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from s3fs import S3FileSystem


class CopyFileFromRawToStagingException(Exception):
    pass


s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    '''
    lambda_handler Top level lambda handler ensuring all exceptions
    are caught and logged.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    :raises CopyFileFromRawToStagingException: On any error or exception
    '''
    try:
        return copy_file_from_raw_to_staging(event, context)
    except CopyFileFromRawToStagingException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise CopyFileFromRawToStagingException(e)


def copy_file_from_raw_to_staging(event, context):
    '''
    copy_file_from_raw_to_staging Copies the file from the data lake raw
    bucket to the staging bucket.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    '''
    try:
        
        raw_bucket = event['fileDetails']['bucket']
        raw_key = event['fileDetails']['key']
        raw_table_name = event['fileDetails']['db_Table']
        raw_file_name = event['fileDetails']['fileName']
        
        staging_bucket = event['settings']['stagingBucket']
        metadata = event['combinedMetadata']

        staging_key = _get_staging_key(
            event['fileDetails'],
            event['fileSettings'],
            metadata)
            
        country_code = ''
        if event['requiredMetadata']['country']:
            country_code = event['requiredMetadata']['country'] + '/'
            
        if "landing/" in staging_key:
            raw_key_partitioned =  "{}/{}".format(staging_key.replace("landing/", "partitioned/", 1),raw_file_name)
        else:
            raw_key_partitioned = "partitioned/{}/{}".format(staging_key,raw_file_name)
            
        print("##* raw_key_partitioned="+raw_key_partitioned) 
        

        #RAW LANDING TO RAW PARTITIONED
        #-------------------------------------------------------------------------------
        #Copy the object to Raw partitioned and apply the specified tags and metadata.
        
        print('Copying Raw object: {} from Raw bucket: {} to key {} in Raw bucket partitioned: {}'.format(
            raw_key, raw_bucket, raw_key_partitioned, raw_bucket))
            
        copy_source = {'Bucket': raw_bucket, 'Key': raw_key} 
        s3.copy(
            copy_source,
            raw_bucket,
            raw_key_partitioned,
            ExtraArgs={"Metadata": metadata, "MetadataDirective": "REPLACE"})
            
        event['fileDetails'].update({"rawPartitionedKey": raw_key_partitioned})

        # Generate the tag list.
        tagList = []
        for tagKey in event['requiredTags']:
            tag = {'Key': tagKey, 'Value': event['requiredTags'][tagKey]}
            tagList.append(tag)

        # Apply the tag list.
        s3.put_object_tagging(
            Bucket=raw_bucket,
            Key=raw_key_partitioned,
            Tagging={'TagSet': tagList})
            
        event['fileDetails'].update({"stagingKey": staging_key})
        
        #COPY File from RAW PARTITIONED TO STAGING 
        #-------------------------------------------------------------------------------
        #Copy the object to Staging partitioned and apply the specified tags and metadata.
        
        obj = s3.get_object(Bucket=raw_bucket, Key=raw_key)
        df = pd.read_csv(obj['Body'])
        table = pa.Table.from_pandas(df,preserve_index=False) 
        
        
        staging_folder_partitioned = staging_key.replace("landing/","", 1)
        
        print("###INFO staging folder partitioned file")
        print(staging_folder_partitioned)
        
        output_file = 's3://{}/{}'.format(staging_bucket,staging_folder_partitioned)
        
        print("#INFO output file")
        print(output_file)

        print('Copying object: {} from Raw bucket: {} to folder: {} in bucket {} on path: {}'.format(
            raw_key, raw_bucket, staging_folder_partitioned, staging_bucket, output_file)) 
            
        pq.write_to_dataset(table=table, root_path=output_file, filesystem=S3FileSystem() )

        return event
        
    except Exception as e:
        traceback.print_exc()
        raise CopyFileFromRawToStagingException(e)


def _get_staging_key(file_details, file_settings, metadata):
    '''
    _get_staging_key Given the supplied file details, settings and
    metadata, returns the appropriate staging key (folders + filename).
    If a staging_folder_path is provided - use it. If not, use the
    same path as in raw.
    If staging_partition_settings are provided - use them to set
    date partitioning.

    :param file_details: The file_details from the input event
    :type file_details: Python Object
    :param file_settings: The file_settings from the input event
    :type file_settings: Python Object
    :param metadata: The metadata from the input event
    :type metadata: Python Object
    :return: The staging key of this file
    :rtype: Python String
    '''
    raw_key = file_details['key']
    raw_table_name = file_details['db_Table']

    staging_folder_path = file_settings['stagingFolderPath']\
        if 'stagingFolderPath' in file_settings\
        else None

    staging_partition_settings = file_settings['stagingPartitionSettings']\
        if 'stagingPartitionSettings' in file_settings\
        else None

    if staging_folder_path is not None:
        staging_key = staging_folder_path
    else:
        staging_key = _get_folder_path_from_key(raw_key)

    if staging_partition_settings is not None:
        staging_expression = file_settings['stagingPartitionSettings']\
            ['expression']
        staging_timezone = file_settings['stagingPartitionSettings']\
            ['timezone']
        created_date = metadata['created_date']

        created_datetime = parser.parse(created_date)
        staging_key = _remove_datetime_partitions_from_key(staging_key)
        datetme_in_timezone = created_datetime.astimezone(
            gettz(staging_timezone))

        staging_key = "{}/{}".format(
            staging_key,
            datetme_in_timezone.strftime(staging_expression))

    # Add the filename, and remove any double slashes. This stops the config
    # of datasources being too draconian regarding start and end slashes.
    staging_key = '{}'.format(staging_key).replace('//', '/')
    
    print("**INFO staging_key="+staging_key)

    return staging_key


def _get_folder_path_from_key(key):
    '''
    _get_folder_path_from_key Retrieves the s3 folder path from
    the key name. This is the input key without the filename.

    :param key: The S3 key name (folders + filename)
    :type key: Python String
    :return: The folder path
    :rtype: Python String
    '''
    last_folder_ends = key.rfind('/')
    if last_folder_ends == -1:
        return ''
    else:
        return key[:last_folder_ends + 1]


def _remove_datetime_partitions_from_key(key):
    '''
    _remove_datetime_partitions_from_key Removes any existing
    date / time partitions from the folder path. These will be
    replaced with the configured timezone.

    :param key: The S3 key name (folders + filename)
    :type key: Python String
    :return: The S3 key name without any year/month/day/hour paritions
    :rtype: Python String
    '''
    regex_list = ['/[A-Za-z0-9_]*=[0-9]+']
    new_key = key
    for regex_match in regex_list:
        new_key = re.sub(regex_match, '', new_key)
    return new_key
