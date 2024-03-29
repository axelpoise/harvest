# Athena
## Athena Query with boto3
setup boto3 session
```python 
import boto3
from time import sleep

session = boto3.session.Session(profile_name='data')
client = session.client('athena')
```
create athena query.
```python
result = client.start_query_execution(
    QueryString="""--sql
    SELECT * FROM treesdb<<name>>.<<table_name>> LIMIT 10
    """,
    QueryExecutionContext={
        'Database': 'treesdb<<name>>'
    },
    ResultConfiguration={
        'OutputLocation': 's3://<<outputbucket_name>>/athena-logs'

    },
    WorkGroup='primary'
)
```
read query result to file.
```python
print(f'query execution id {result["QueryExecutionId"]}')
sleep(3)
s3 = session.client('s3')
response = s3.get_object(
    Bucket='<<outputbucket_name>>',
    Key=f'athena-logs/{result["QueryExecutionId"]}.csv'
)
body = response['Body'].read().decode("utf-8")
with open("athena.csv", mode='wt', encoding='utf-8') as f:
    f.write(body)
    f.close()
```

## Run queries
run the file
```shell
python athena_queries.py
```
Run the following to get table columns
```shell
aws glue get-tables --database-name 'treesdb<<name>>' --profile data
```