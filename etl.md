# Create ETL

## The ETL Workflow
Create a new stack in a new python package (etl_pipeline). Add the following to the `__init__.py`
```python
import aws_cdk as cdk
import aws_cdk.aws_glue_alpha as glue

from environment import get_environment
from constructs import Construct

class ETLStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, bronze_bucket: cdk.aws_s3.Bucket, script_bucket: cdk.aws_s3.Bucket, name: str):
        super().__init__(scope, construct_id, env=get_environment())
```
create glue database
```python
        self.glue_database = glue.Database(self, f"TreesDB{name}",
                                          database_name=f"treesdb{name}",
                                          location_uri=bronze_bucket.bucket_arn)
```
create service role method
```python
    def create_glue_service_role(self, name: str) -> cdk.aws_iam.Role:
        glue_service_role = cdk.aws_iam.Role(self, f"GlueServiceRole{name}",
                                         assumed_by=cdk.aws_iam.ServicePrincipal("glue.amazonaws.com"))

        glue_service_role.add_to_policy(cdk.aws_iam.PolicyStatement(
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
            ], effect=cdk.aws_iam.Effect.ALLOW, resources=["*"]))

        glue_service_role.add_to_policy(cdk.aws_iam.PolicyStatement(
            actions=[
                "s3:CreateBucket"
            ],
            effect=cdk.aws_iam.Effect.ALLOW,
            resources=[
                "arn:aws:s3:::aws-glue-*"
            ]))

        glue_service_role.add_to_policy(cdk.aws_iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            effect=cdk.aws_iam.Effect.ALLOW,
            resources=[
                "arn:aws:s3:::aws-glue-*/*",
                "arn:aws:s3:::*/*aws-glue-*/*"
            ]))

        glue_service_role.add_to_policy(cdk.aws_iam.PolicyStatement(
            actions=[
                "s3:GetObject"
            ],
            effect=cdk.aws_iam.Effect.ALLOW,
            resources=[
                "arn:aws:s3:::crawler-public*",
                "arn:aws:s3:::aws-glue-*"
            ]))

        glue_service_role.add_to_policy(cdk.aws_iam.PolicyStatement(
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:AssociateKmsKey"
            ],
            effect=cdk.aws_iam.Effect.ALLOW,
            resources=[
                "arn:aws:logs:*:*:/aws-glue/*"
            ]))

        glue_service_role.add_to_policy(cdk.aws_iam.PolicyStatement(
            actions=[
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            effect=cdk.aws_iam.Effect.ALLOW,
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
create job and crawler method
```python
    def create_job_and_crawler(self,
                        database: glue.Database,
                        script_bucket: cdk.aws_s3.Bucket,
                        bronze_bucket: cdk.aws_s3.Bucket,
                        glue_service_role: cdk.aws_iam.Role,
                        name: str):
```
create crawler
```python
        crawler_target = cdk.aws_glue.CfnCrawler.S3TargetProperty(path=f"s3://{bronze_bucket.bucket_name}/")
        targets = cdk.aws_glue.CfnCrawler.TargetsProperty(s3_targets=[crawler_target])
        self.bronze_crawler = cdk.aws_glue.CfnCrawler(self, f"S3GlueCrawler{name}",
                                 role=glue_service_role.role_arn,
                                 targets=targets,
                                 database_name=database.database_name,
                                 name=f"BronzeCrawler{name}"
                                 )
```
create job
```python
        command = cdk.aws_glue.CfnJob.JobCommandProperty(name="glueetl",
                                            python_version="3",
                                            script_location=f"s3://{script_bucket.bucket_name}/modifying_dataset.py")

        self.trees_job = cdk.aws_glue.CfnJob(self, f"MapTreesGlueJob{name}",
                                   command=command,
                                   role=glue_service_role.role_arn,
                                   execution_property=cdk.aws_glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
                                   max_retries=0,
                                   name=f"MapTrees{name}",
                                   glue_version="4.0",
                                   number_of_workers=2,
                                   worker_type="Standard",
                                   timeout=2
                                   )
```

create a workflow placeholder
```python
        self.trees_workflow = cdk.aws_glue.CfnWorkflow(self, f"TreesWorkflow{name}",
                                                default_run_properties=None,
                                                description="trees workflow",
                                                name=f"TreesWorkflow{name}")
```

create workflow stack
```python
class ETLWorkflowStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str,
        crawler: cdk.aws_glue.CfnCrawler, job: cdk.aws_glue.CfnJob, workflow: cdk.aws_glue.CfnWorkflow, name: str):
        super().__init__(scope, construct_id, env=get_environment())
```

create crawler trigger
```python
        class TriggerType:
            ON_DEMAND = "ON_DEMAND"
            CONDITIONAL = "CONDITIONAL"
            SCHEDULED = "SCHEDULED"

        crawler_trigger = cdk.aws_glue.CfnTrigger(self, f"CrawlerTrigger{name}",
                                              actions=[
                                                  cdk.aws_glue.CfnTrigger.ActionProperty(crawler_name=crawler.name)
                                              ],
                                              type=TriggerType.ON_DEMAND,
                                              description="crawler trigger",
                                              name=f"CrawlerTrigger{name}",
                                              workflow_name=workflow.name)

```
create job trigger
```python
        job_trigger = cdk.aws_glue.CfnTrigger(self, f"JobTrigger{name}",
                                          actions=[
                                              cdk.aws_glue.CfnTrigger.ActionProperty(job_name=job.name)
                                          ],
                                          type=TriggerType.CONDITIONAL,
                                          description="job trigger",
                                          name=f"JobTrigger{name}",
                                          predicate=cdk.aws_glue.CfnTrigger.PredicateProperty(conditions=[
                                              cdk.aws_glue.CfnTrigger.ConditionProperty(logical_operator="EQUALS",
                                                                                    crawler_name=crawler.name,
                                                                                    crawl_state='SUCCEEDED')], ),
                                          start_on_creation=True,
                                          workflow_name=workflow.name)

```

add stack to application
```python
etl = ETLStack(app, f"etl-setup-{name}", storage.bronze_bucket, storage.script_bucket, name)

```

add workflow stack to application
```python
workflow = ETLWorkflowStack(app, f"etl-workflow-{name}", etl.bronze_crawler, etl.trees_job, etl.trees_workflow, name)
```

synthesize
```shell
cdk synth
```

## The ETL Script
### Starting up the docker
to run the docker open any terminal and run:
```shell
# Terminal 1
docker run -it -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_3.0.0_image_01 /home/glue_user/jupyter/jupyter_start.sh
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
load json
```python
df_row = spark.createDataFrame([
    Row(json=u'{ "type": "Feature", "properties": { "Lat": 2582424.95945, "Long": 377739.06066299998, "Area": 4.7500000098835846 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ 377738.810662999982014, 2582426.20945 ], [ 377738.810662999982014, 2582425.70945 ], [ 377738.310662999982014, 2582425.70945 ], [ 377737.810662999982014, 2582425.70945 ], [ 377737.810662999982014, 2582423.70945 ], [ 377739.310662999982014, 2582423.70945 ], [ 377739.310662999982014, 2582424.20945 ], [ 377739.810662999982014, 2582424.20945 ], [ 377739.810662999982014, 2582424.70945 ], [ 377740.310662999982014, 2582424.70945 ], [ 377740.310662999982014, 2582425.70945 ], [ 377739.810662999982014, 2582425.70945 ], [ 377739.810662999982014, 2582426.20945 ], [ 377738.810662999982014, 2582426.20945 ] ] ] } }'),
    Row(json=u'{ "type": "Feature", "properties": { "Lat": 2582322.20945, "Long": 393815.81066299998, "Area": 3.7500000098835846 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ 393814.810662999982014, 2582323.20945 ], [ 393814.810662999982014, 2582321.20945 ], [ 393816.310662999982014, 2582321.20945 ], [ 393816.310662999982014, 2582321.70945 ], [ 393816.810662999982014, 2582321.70945 ], [ 393816.810662999982014, 2582323.20945 ], [ 393814.810662999982014, 2582323.20945 ] ] ] } }')
    
])
df_json = spark.read.json(df_row.rdd.map(lambda r: r.json))
df_json.printSchema()
```
create dynamic frame
```python
dyf_json = DynamicFrame.fromDF(df_json, glueContext, "dyf_json")
```
relationize
```python
dyf_relationize = dyf_json.relationalize("root", "/home/glue_user/workspace")
dyf_relationize.keys()
```
select table
```python
dyf_selectFromCollection = SelectFromCollection.apply(dyf_relationize, 'root')
dyf_selectFromCollection.toDF().show()

dyf_root = dyf_relationize.select('root')
```

rename fields
```python
dyf_rename_1 = RenameField.apply(dyf_root, "`properties.Lat`", "lat")
dyf_rename_2 = RenameField.apply(dyf_rename_1, "`properties.Long`", "lon")
dyf_rename_3 = RenameField.apply(dyf_rename_2, "`properties.Area`", "area")

dyf_rename_3.toDF().show()
```

filter on area
```python
dyf_small = Filter.apply(frame=dyf_rename_3, f=lambda x: x['area'] < 4)
dyf_small.toDF().show()

```

write away
```python
glueContext.write_dynamic_frame.from_options( \
    frame = dyf_small, \
    connection_options = {'path': '/home/glue_user/workspace/'}, \
    connection_type = 's3', \
    format = 'json')
```

get buckets
```shell
aws s3api list-buckets --query "Buckets[].Name" --profile data
```
change script with good paths
```python
dyf_json = glueContext.create_dynamic_frame_from_catalog(database="treesdb", table_name="<bucket-name>")
```
upload script
```shell
aws s3 cp ./script.py s3://<scripts-bucket-name/script-name> --profile data
```
upload data
```shell
aws s3 cp ./data.json s3://<bronze-bucket-name> --profile data
```

deploy etl setup stack
```shell
cdk deploy etl-setup-<<<name>>> --profile data
```
deploy etl workflow stack
```shell
cdk deploy etl-workflow-<<<name>>> --profile data
```

run workflow
```shell
aws glue start-workflow-run --name TreesWorkflow<<<name>>> --profile data
```
see if table has been created
```shell
aws glue get-tables --database-name 'treesdb<<<name>>>' --profile data
```
see if data has been added to job location
```shell
aws s3 ls s3://<bucket-name>/ --profile data
```

### ETL Script example for upload.


```python
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

dyf_json = glueContext.create_dynamic_frame_from_catalog(
    database="treesdb<<name>>", table_name="<<bucket-name-bronze>>")


dyf_relationize = dyf_json.relationalize("root", "s3://base-setup-scriptsbucket40feb4b1-esjpmoevpj2q/")
dyf_relationize.keys()

dyf_selectFromCollection = SelectFromCollection.apply(dyf_relationize, 'root')
dyf_selectFromCollection.toDF().show()

dyf_root = dyf_relationize.select('root')

dyf_rename_1 = RenameField.apply(dyf_root, "`properties.Lat`", "lat")
dyf_rename_2 = RenameField.apply(dyf_rename_1, "`properties.Long`", "lon")
dyf_rename_3 = RenameField.apply(dyf_rename_2, "`properties.Area`", "area")

dyf_rename_3.toDF().show()

dyf_small = Filter.apply(frame=dyf_rename_3, f=lambda x: x['area'] < 4)
dyf_small.toDF().show()

glueContext.write_dynamic_frame.from_options(\
    frame = dyf_small,\
    connection_options={'path': 's3://base-setup-silverbucket40feb4b1-esjpmoevpj2q'},\
    connection_type='s3',\
    format='parquet')
```
