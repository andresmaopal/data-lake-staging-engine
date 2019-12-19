import time
import traceback
import json
import boto3


class RecordSuccessfulStagingException(Exception):
    pass


sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
glue_client = boto3.client('glue')



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
    :raises RecordSuccessfulStagingException: On any error or exception
    '''
    try:
        return record_successfull_staging(event, context)
    except RecordSuccessfulStagingException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise RecordSuccessfulStagingException(e)


def record_successfull_staging(event, context):
    """
    record_successfull_staging Records the successful staging in the data
    catalog and raises an SNS notification.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """
    record_successful_staging_in_data_catalog(event, context)
    send_successful_staging_sns(event, context)
    return event
    
    
def create_glue_crawler(
                         name,
                         database_name,
                         targets,
                         role="AWSGlueServiceRoleDefault"
                         ):
    
    try: 
        
        print("#ROOOL es:{}".format(role))
        
        response = glue_client.create_crawler(
        Name=name,
        Role=role,
        DatabaseName=database_name,
        Targets=targets,
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        },
        Configuration='{ "Version": 1.0, "CrawlerOutput": { "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" }, "Tables": {"AddOrUpdateBehavior": "MergeNewColumns" } } }'
        )
        
        print( "#OK crawler {} created succesfully".format(name) )

        
        return response
    
    except Exception as e:
        traceback.print_exc()
        print("#ERROR Failed to create new Glue Crawler: {} ".format(name))
    



def record_successful_staging_in_data_catalog(event, context):
    '''
    record_successful_staging_in_data_catalog Records the successful staging
    in the data catalog.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''

    raw_key = event['fileDetails']['key']
    raw_bucket = event['fileDetails']['bucket']
    staging_key = event['fileDetails']['stagingKey']
    file_table = event['fileDetails']['db_Table']
    file_schema = event['fileDetails']['db_Schema']
    file_database = event['fileDetails']['db_DataBase']
    content_length = event['fileDetails']['contentLength']
    staging_execution_name = event['fileDetails']['stagingExecutionName']
    file_type = event["fileType"]
    fileName = event['fileDetails']['fileName']
    country_code = event['requiredMetadata']['country']
    staging_bucket = event['settings']['stagingBucket']
    data_catalog_table = event["settings"]["dataCatalogTableName"]
    staging_database_prefix = event["crawlerSettings"]["stagingDatabasePrefix"]
    glue_role_name =  "service-role/"+event["crawlerSettings"]["glueRoleName"]
    

    tags = event['requiredTags']
    metadata = event['combinedMetadata']

    if 'stagingPartitionSettings' in event['fileSettings']:
        staging_partition_settings = \
            event['fileSettings']['stagingPartitionSettings']
            
            
    #GLUE DATA CATALOG DATA STORE SYNC - STAGING
    
    #Define Glue Crawler's new Data Store path
    n_raw_key = raw_key.replace('landing/','',1).replace('/'+fileName,'',1)
    update_path = "s3://{}/{}/".format(staging_bucket,n_raw_key)
    
    crawler_name = "{}_{}_{}".format(country_code,file_database,file_schema)
    
    print("#INFO S3 PATH: "+update_path)
    

    crawlersList = glue_client.list_crawlers()
    
    print("#INFO CRAWLER LIST")
    print(crawlersList['CrawlerNames'])
    print(crawler_name)

    
    if crawler_name in crawlersList['CrawlerNames']:
        
        # If the crawler exists
        response_gt = glue_client.get_crawler(Name=crawler_name)
        
        print("#OK Crawler {} exists".format(crawler_name) )

        #Check if the data source exists in the Crawler
        found=False
        for tgt in response_gt['Crawler']['Targets']['S3Targets']:
            if tgt['Path'] == update_path:
                print("#INFO S3 target {} exits!".format(update_path) )
                found=True
                
        targets_j=response_gt['Crawler']['Targets']['S3Targets']
        targets_j.append( {'Path': update_path, 'Exclusions': [] })
        targets_string = targets_j
        
                
        if found == False:
            print("Data Store S3 target {} Not found. Adding the new path... ".format(update_path))
            response_wr = glue_client.update_crawler(Name=crawler_name, 
            Targets={'S3Targets': targets_string } )
            
            print("S3 Path {} added as a new data store in the {} crawler. ".format(update_path,crawler_name) )
            
    else:
        
        #If crawler does not exists create a new one (crawler_name)
        
        print( "#INFO Crawler {} does not exist, attempting to create it...".format(crawler_name) )

        
        database_name="{}_{}".format(staging_database_prefix,country_code)
        
        responseGetDatabases = glue_client.get_databases()
        
        databaseList = responseGetDatabases['DatabaseList']

        #If database does not exist create a new one (database_name)
        
        flag_db_exist=False
        
        for databaseDict in databaseList:
        
            if databaseDict['Name']==database_name:
                flag_db_exist=True
                break
                
        if flag_db_exist == False:
            
            print( "#INFO Database {} does not exist, attempting to create it".format(database_name) )            

            try:
                response = glue_client.create_database(
                            DatabaseInput={
                                'Name': database_name  # Required
                            }
                        )
                        
                print( "#OK Database {} created succesfully".format(database_name) )

            except Exception as e:
                traceback.print_exc()
                print("Failed to create new Glue Database: {} ".format(database_name))
        
        
        targets_j={
                    'S3Targets': [
                            {
                                'Path': update_path,
                                'Exclusions': []
                            },
                        ]
                    }
                    
        
        try:
            
            print("#INFO Database Name")
            print(database_name)

            create_glue_crawler(crawler_name,database_name,targets_j,glue_role_name)
            
            
        except Exception as e:
            traceback.print_exc()
            print("Failed to create new Glue Crawler: {} ".format(crawler_name))            
        
            
    
    try:
        
        #DYNAMODB OBJECT LOGGING
        
        dynamodb_item = {
            'rawKey': raw_key,
            'catalogTime': int(time.time() * 1000),
            'rawBucket': raw_bucket,
            'stagingKey': staging_key,
            'stagingBucket': staging_bucket,
            'contentLength': content_length,
            'fileType': file_type,
            'stagingExecutionName': staging_execution_name,
            'stagingPartitionSettings': staging_partition_settings,
            'tags': tags,
            'metadata': metadata
        }
        dynamodb_table = dynamodb.Table(data_catalog_table)
        dynamodb_table.put_item(Item=dynamodb_item)

    except Exception as e:
        traceback.print_exc()
        raise RecordSuccessfulStagingException(e)


def send_successful_staging_sns(event, context):
    '''
    send_successful_staging_sns Sends an SNS notifying subscribers
    that staging was successful.

    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    raw_key = event['fileDetails']['key']
    raw_bucket = event['fileDetails']['bucket']
    file_type = event['fileType']

    subject = 'Data Lake - ingressed file staging success'
    message = 'File:{} in Bucket:{} for DataSource:{} successfully staged' \
        .format(raw_key, raw_bucket, file_type)

    if 'fileSettings' in event \
            and 'successSNSTopicARN' in event['fileSettings']:
        successSNSTopicARN = event['fileSettings']['successSNSTopicARN']
        send_sns(successSNSTopicARN, subject, message)


def send_sns(topic_arn, subject, message):
    '''
    send_sns Sends an SNS with the given subject and message to the
    specified ARN.

    :param topic_arn: The SNS ARN to send the notification to
    :type topic_arn: Python String
    :param subject: The subject of the SNS notification
    :type subject: Python String
    :param message: The SNS notification message
    :type message: Python String
    '''
    sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
