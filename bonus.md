# To get further use the documentation

- [AWS CDK](https://docs.aws.amazon.com/cdk/api/latest/docs/aws-construct-library.html)
- [AWS CLI](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/index.html)
- [BOTO3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [CLOUDFORMATION](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-template-resource-type-ref.html)


## Add Athena code to Lambda
lambda...
```python
aws_lambda.Function(
    self, "FunctionId",
    runtime=aws_lambda.Runtime.PYTHON_3_7,
    handler="athena.main",
    code=aws_lambda.Code.fromBucket(bucket="", key="")
)
```
api gateway...
```python
rest_api = aws_apigateway.RestApi(self, "RestApi")
integration = aws_apigateway.LambdaIntegration(function)
method = 'GET'
rest_api.root.add_method(method, integration)
```

## Explore other tools
Check the free tier options:
https://aws.amazon.com/free/?all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc

## Local intellisense for creating your own awsglue spark data pipelines
The AWS Glue team has a public python repository on github with all the glue transforms for Spark. 

`git clone https://github.com/awslabs/aws-glue-libs`

After downloading this package there are multiple ways to add the `awsglue` folder of this repository to your working directory. For this masterclass I suggest the quick and unsustainable way. 

Option 1: copy the folder to the directory where you write your aws glue spark jobs as a subfolder. 

Option 2: use this downloaded repository as your new root folder. Create a Virtual Env and install `pyspark`.