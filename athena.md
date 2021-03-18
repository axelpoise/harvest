# Athena
## Athena Query with boto3
setup boto3 session
```python
session = boto3.session.Session(profile_name='assumed-analyst')
client = session.client('athena')
```
create athena query.
```python
result = client.start_query_execution(
    QueryString='SELECT * FROM harvestdb.base_setup_rawbucket0c3ee094_14eptovdemao LIMIT 10',
    QueryExecutionContext={
        'Database': 'harvestdb'
    },
    ResultConfiguration={
        'OutputLocation': 's3://base-setup-modifiedbucket9b9e950b-1nk6t0unvltlq/athena-logs'

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
    Bucket='base-setup-modifiedbucket9b9e950b-1nk6t0unvltlq',
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
aws glue get-tables --database-name 'harvestdb' --profile assumed-analyst
```