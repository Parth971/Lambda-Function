import boto3


def get_latest_file_with_cdms(files):
    latest_file = None

    for file in files:
        file_path = file['Key']
        file_name = file_path.split('/')[-1]
        if 'CDMS' in file_name:
            if latest_file:
                latest_file_date = latest_file['LastModified']
                file_date = file['LastModified']
                if file_date > latest_file_date:
                    latest_file = file
            else:
                latest_file = file

    return latest_file


def get_latest_file_without_cdms(files):
    latest_file = None

    for file in files:
        file_path = file['Key']
        file_name = file_path.split('/')[-1]
        if 'CDMS' not in file_name:
            if latest_file:
                latest_file_date = latest_file['LastModified']
                file_date = file['LastModified']
                if file_date > latest_file_date:
                    latest_file = file
            else:
                latest_file = file

    return latest_file


def lambda_handler(event, context):
    S3_BUCKET = 's3'
    GLUE_WORKFLOW = 'glue'
    OLD_FOLDER_PATH = ''  # E.g.  'old_zip_files/'
    NEW_FOLDER_PATH = ''  # E.g.  'zip-files/'
    GLUE_JOB1_NAME = ''
    GLUE_JOB2_NAME = ''
    WITH_CDMS = True

    # Parse bucket name from event
    BUCKET_NAME = event['Records'][0]['s3']['bucket']['name']

    s3_client = boto3.client(S3_BUCKET)
    s3_resource = boto3.resource(S3_BUCKET)
    glue_client = boto3.client(GLUE_WORKFLOW)

    # Get all files from s3
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=OLD_FOLDER_PATH)
    files = response['Contents']

    if WITH_CDMS:
        # With CDMS COPY to new location and DELETE from old location
        cdms = get_latest_file_with_cdms(files)
    else:
        # Without CDMS COPY to new location and DELETE from old location
        cdms = get_latest_file_without_cdms(files)

    cdms_old_path = cdms['Key']
    with_cdms_file_name = cdms_old_path.split('/')[-1]
    source = {
        'Bucket': BUCKET_NAME,
        'Key': cdms_old_path
    }
    s3_resource.meta.client.copy(source, BUCKET_NAME, NEW_FOLDER_PATH + with_cdms_file_name)
    s3_resource.Object(BUCKET_NAME, cdms_old_path).delete()

    # Starting Glue Workflows
    try:
        response = glue_client.start_job_run(JobName=GLUE_JOB1_NAME)
        status = glue_client.get_job_run(JobName=GLUE_JOB1_NAME, RunId=response['JobRunId'])
        print("Job Status : ", status['JobRun']['JobRunState'])

        response = glue_client.start_job_run(JobName=GLUE_JOB2_NAME)
        status = glue_client.get_job_run(JobName=GLUE_JOB2_NAME, RunId=response['JobRunId'])
        print("Job Status : ", status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)
