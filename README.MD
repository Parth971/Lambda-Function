
# Lambda Function

## Aim

To create a AWS lambda function which is triggered every time a file is uploaded to AWS S3-bucket. The file can be `with cdms` or `without cdms` suffix. Depending on file uploaded to S3 bucket, script retrieves a list of files already present in `OLD_FOLDER_PATH` in S3-bucket. 

Now, after detecting latest file (last file) uploded in `OLD_FOLDER_PATH` folder, script will copy that file to `NEW_FOLDER_PATH` and deletes file from `OLD_FOLDER_PATH`. 

Afterwards, lambda function will start two workflows using `glue_client`.

### Variables Definition

```
OLD_FOLDER_PATH = 'old_zip_files_folder_path/'
NEW_FOLDER_PATH = 'new_zip_files_folder_path/'
GLUE_JOB1_NAME = 'work_flow_name1'
GLUE_JOB2_NAME = 'work_flow_name2'
WITH_CDMS = True  # This value denotes if lambda function detects CDMS
```
