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

## Add Simple email service
simple mail service...
https://docs.aws.amazon.com/ses/latest/DeveloperGuide/quick-start.html


event rule from s3...
https://docs.aws.amazon.com/codepipeline/latest/userguide/create-cloudtrail-S3-source-cfn.html

## Explore other tools
Check the free tier options:
https://aws.amazon.com/free/?all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc