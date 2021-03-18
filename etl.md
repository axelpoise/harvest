# Create ETL

## The ETL Script
### Starting up the docker
to run the docker open any terminal and run:
```shell
# Terminal 1
docker run -itd --name glue_without_notebook amazon/aws-glue-libs:glue_libs_1.0.0_image_01
docker exec -it glue_without_notebook bash
```
In another terminal run:
```shell
# Terminal 2
docker run -itd -p 8888:8888 -p 4040:4040 -v ~/.aws:/root/.aws:ro --name glue_jupyter \amazon/aws-glue-libs:glue_libs_1.0.0_image_01 \
/home/jupyter/jupyter_start.sh
```

### Glue and spark
load context:
```python
import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
```
create dynamic frame
```python
dyf_json = DynamicFrame.fromDF(df_json, glueContext, "dyf_json")
```
relationize
```python
dyf_relationize = dyf_json.relationalize("root", "/home/glue/GlueLocalOutput")
dyf_relationize.keys()
```
select table
```python
dyf_root = dyf_relationize.select('root')
dyf_root.toDF().show()
```
filter
```python
dyf_filter_root_ownership = Filter.apply(
frame = dyf_root, f = lambda x: x["statementType"] == 'ownershipOrControlStatement'
)

dyf_filter_root_ownership.toDF().select(['`interestedParty.unspecified.description`']).show()
```
filter again
```python
dyf_filter_root_no_ownership = Filter.apply(
frame = dyf_filter_root_ownership, f = lambda x: x["interestedParty.unspecified.description"] != None
)
dyf_filter_root_no_ownership.toDF().show()
```
write away
```python
glueContext.write_dynamic_frame.from_options( \
    frame = dyf_filter_root_no_ownership, \
    connection_options = {'path': ‘/home/glue/GlueLocalOutput/'}, \
    connection_type = 's3', \
    format = 'json')
```

## The ETL Workflow
create new stack in new python package. Add the following to the `__init__.py`
```python
class ETLStack(core.Stack):

    def __init__(self, scope, construct_id, raw_bucket: aws_s3.Bucket, script_bucket: aws_s3.Bucket):
        super().__init__(scope, construct_id, env=get_environment())
```
create glue database
```python
self.glue_database = aws_glue.Database(self, "HarvestDB",
                                  database_name="harvestdb",
                                  location_uri=raw_bucket.bucket_arn)
```
create service role method
```python
def create_glue_service_role(self) -> aws_iam.Role:
    glue_service_role = aws_iam.Role(self, "GlueServiceRole",
                                     assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"))
    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:GetObject",
            "glue:*",
            "s3:GetBucketLocation",
            "s3:ListBucket",
            "s3:ListAllMyBuckets",
            "s3:GetBucketAcl",
            "ec2:DescribeVpcEndpoints",
            "ec2:DescribeRouteTables",
            "ec2:CreateNetworkInterface",
            "ec2:DeleteNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DescribeSecurityGroups",
            "ec2:DescribeSubnets",
            "ec2:DescribeVpcAttribute",
            "iam:ListRolePolicies",
            "iam:GetRole",
            "iam:GetRolePolicy",
            "cloudwatch:PutMetricData"
        ], effect=aws_iam.Effect.ALLOW, resources=["*"]))

    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "s3:CreateBucket"
        ],
        effect=aws_iam.Effect.ALLOW,
        resources=[
            "arn:aws:s3:::aws-glue-*"
        ]))

    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject"
        ],
        effect=aws_iam.Effect.ALLOW,
        resources=[
            "arn:aws:s3:::aws-glue-*/*",
            "arn:aws:s3:::*/*aws-glue-*/*"
        ]))

    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "s3:GetObject"
        ],
        effect=aws_iam.Effect.ALLOW,
        resources=[
            "arn:aws:s3:::crawler-public*",
            "arn:aws:s3:::aws-glue-*"
        ]))

    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:AssociateKmsKey"
        ],
        effect=aws_iam.Effect.ALLOW,
        resources=[
            "arn:aws:logs:*:*:/aws-glue/*"
        ]))

    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "ec2:CreateTags",
            "ec2:DeleteTags"
        ],
        effect=aws_iam.Effect.ALLOW,
        resources=[
            "arn:aws:ec2:*:*:network-interface/*",
            "arn:aws:ec2:*:*:security-group/*",
            "arn:aws:ec2:*:*:instance/*"
        ],
        conditions={"ForAllValues:StringEquals": {
            "aws:TagKeys": [
                "aws-glue-service-resource"
            ]
        }}))

    return glue_service_role
```
create workflow method
```python
def create_workflow(self,
                    database: aws_glue.Database,
                    script_bucket: aws_s3.Bucket,
                    raw_bucket: aws_s3.Bucket,
                    glue_service_role: aws_iam.Role):
```
create crawler
```python
crawler_target = CfnCrawler.S3TargetProperty(path=f"s3://{raw_bucket.bucket_name}/")
targets = CfnCrawler.TargetsProperty(s3_targets=[crawler_target])
raw_crawler = CfnCrawler(self, "S3GlueCrawler",
                         role=glue_service_role.role_arn,
                         targets=targets,
                         database_name=database.database_name,
                         name="RawCrawler"
                         )
```
create job
```python
command = CfnJob.JobCommandProperty(name="glueetl",
                                    python_version="3",
                                    script_location=f"s3://{script_bucket.bucket_name}/modifying_dataset.py")

missing_owner_job = CfnJob(self, "FindMissingOwnersGlueJob",
                           command=command,
                           role=glue_service_role.role_arn,
                           execution_property=CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
                           max_retries=0,
                           name="FindMissingOwnersJob",
                           glue_version="2.0",
                           number_of_workers=2,
                           worker_type="Standard",
                           timeout=2,
                           )
```
create workflow
```python
harvest_workflow = aws_glue.CfnWorkflow(self, "HarvestWorkflow",
                                        default_run_properties=None,
                                        description="harvest workflow",
                                        name="HarvestWorkflow")
```
create crawler trigger
```python
class TriggerType:
    ON_DEMAND = "ON_DEMAND"
    CONDITIONAL = "CONDITIONAL"
    SCHEDULED = "SCHEDULED"

crawler_trigger = aws_glue.CfnTrigger(self, "CrawlerTrigger",
                                      actions=[
                                          aws_glue.CfnTrigger.ActionProperty(crawler_name=raw_crawler.name)
                                      ],
                                      type=TriggerType.ON_DEMAND,
                                      description="crawler trigger",
                                      name="CrawlerTrigger",
                                      workflow_name=harvest_workflow.name)

```
create job trigger
```python
job_trigger = aws_glue.CfnTrigger(self, "JobTrigger",
                                  actions=[
                                      aws_glue.CfnTrigger.ActionProperty(job_name=missing_owner_job.name)
                                  ],
                                  type=TriggerType.CONDITIONAL,
                                  description="job trigger",
                                  name="JobTrigger",
                                  predicate=aws_glue.CfnTrigger.PredicateProperty(conditions=[
                                      aws_glue.CfnTrigger.ConditionProperty(logical_operator="EQUALS",
                                                                            crawler_name=raw_crawler.name,
                                                                            crawl_state='SUCCEEDED')], ),
                                  start_on_creation=True,
                                  workflow_name=harvest_workflow.name)

```
get buckets
```shell
aws s3api list-buckets --query "Buckets[].Name" --profile harvest
```
change script with good paths
```python
dyf_json = glueContext.create_dynamic_frame_from_catalog(database="harvestdb", table_name="<bucket-name>")
```
upload script
```shell
aws s3 cp ./script.py s3://<scripts-bucket-name/script-name> --profile harvest
```
upload data
```shell
aws s3 cp ./harvest.json s3://<raw-bucket-name> --profile harvest
```
add stack to application
```python
etl = ETLStack(app, "etl-setup", base.raw_bucket, base.script_bucket)

```
synthesize stack
```shell
cdk synth --app 'python3 app.py' --profile harvest
```
deploy stack
```shell
cdk deploy etl-setup --profile harvest
```
run workflow
```shell
aws glue start-workflow-run --name HarvestWorkflow --profile harvest
```
see if table has been created
```shell
aws glue get-tables --database-name 'harvestdb' --profile harvest
```
see if data has been added to job location
```shell
aws s3 ls s3://<bucket-name>/ --profile harvest
```

### ETL Script example for upload.


```python
import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

dyf_json = glueContext.create_dynamic_frame_from_catalog(database="harvestdb", table_name="base_setup_rawbucket0c3ee094_1bfdqv3sqfw1y")

dyf_relationize = dyf_json.relationalize("root", "s3://base-setup-modifiedbucket9b9e950b-14uguw1y5ohy")

dyf_selectFromCollection = SelectFromCollection.apply(dyf_relationize, 'root')

dyf_root = dyf_relationize.select('root')

dyf_filter_root_ownership = Filter.apply(frame = dyf_root, f = lambda x: x["statementType"] == 'ownershipOrControlStatement')

dyf_filter_root_no_ownership = Filter.apply(frame = dyf_filter_root_ownership, f = lambda x: x["interestedParty.unspecified.description"] != None)

glueContext.write_dynamic_frame.from_options(\
    frame = dyf_filter_root_no_ownership,\
    connection_options={'path': 's3://base-setup-modifiedbucket9b9e950b-14uguw1y5ohy'},\
    connection_type='s3',\
    format='parquet')



```
