# Base Setup
## Setting up access

```shell
aws configure --profile harvest

AWS Access Key ID : SLFDSDSLJ1LJ
AWS Secret Access Key : SASDKSDJSKJJHFDSJKFD//Kshdkfjh
Default region name : eu-west-1
Default output format: JSON

```

## Write the first code
create a cdk project
```shell
cdk init app --language python 
```
have a look
```shell
ls
```
open in your IDE and in the terminal of your IDE run
```shell
source .venv/bin/activate
```
or on windows the `source.bat` file by typing the name of the file in the terminal.


Add the following depencencies to your `requirements.txt` and do a pip install.
```requirements.txt
aws-cdk.aws-cloudwatch
aws-cdk.aws-cloudwatch-actions
aws-cdk.aws-ec2
aws-cdk.aws-ecs
aws-cdk.aws-kms
aws-cdk.aws-lambda
aws-cdk.aws-logs
aws-cdk.aws-iam
aws-cdk.aws-s3
aws-cdk.core
aws_cdk.aws_glue
aws_cdk.aws_athena
aws_cdk.aws_lakeformation
pyspark
awsglue-local
```
Create a package with the following code in your `__init__.py`
```python
from aws_cdk import core

from environment import get_environment


class BaseSetupStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id, env=get_environment())
```

then to get account number run in the command line:

```shell
aws sts get-caller-identity --profile harvest > account.json
````
------ or on windows -------------
```powershell
aws sts get-caller-identity --profile harvest | File-Out --FilePath account.json
```

Then add in a new file in the project root named environment.py the following code.
```python
from aws_cdk import core

def get_environment()-> core.Environment:
    return core.Environment(region='eu-west-1', account='<<<account number>>>')
```

Add method to BaseSetupStack
```python
    def add_s3_buckets(self):

        self.raw_bucket: aws_s3.Bucket = aws_s3.Bucket(self, "RawBucket",
                      block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
                      removal_policy=RemovalPolicy.DESTROY,
                      access_control=aws_s3.BucketAccessControl.PRIVATE,
                      )
```
call method from constructor
```python

class BaseSetupStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id, env=get_environment())

        self.add_s3_buckets()
```
add base setup to application
```python
#!/usr/bin/env python3

from aws_cdk import core

from base_setup.base_setup_stack import BaseSetupStack

app = core.App()
base = BaseSetupStack(app, "base-setup")

app.synth()
```

synthesize stack
```shell
cdk synth --app "python3 app.py" --profile harvest
```
deploy stack
```shell
cdk deploy base-setup --profile harvest
```
