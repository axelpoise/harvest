# Base Setup
## Setting up access

```shell
aws configure --profile data

AWS Access Key ID : SLFDSDSLJ1LJ
AWS Secret Access Key : SASDKSDJSKJJHFDSJKFD//Kshdkfjh
Default region name : eu-west-1
Default output format: json
```

## Write the first code
copy a cdk project base to get started
```shell
git clone https://github.com/axelpoise/harvest_template.git <<<your_project_name>>>
cd <<<your_project_name>>>
```
install a virtual env in the root of the new project
```shell
python -m venv .venv
```
open the project in your IDE and in the terminal of your IDE run
```shell
source .venv/bin/activate
```
or on windows the `source.bat` file by typing the name of the file in the terminal.


Add the following depencencies to your `requirements.txt` and do a pip install.
```requirements.txt
aws-cdk-lib==2.121.1
constructs>=10.0.0,<11.0.0
aws_cdk.aws_glue_alpha==2.121.1a0
pyspark
boto3
```

Then to get account number run in the command line:

```shell
aws sts get-caller-identity --profile data > account.json
````
------ or on windows -------------
```powershell
aws sts get-caller-identity --profile data | Out-File -Filepath account.json
```
To distinguish your project on the amazon cloud change the qualifier in the `cdk.json` file.
```json
{ 
  ...,
  "@aws-cdk/core:bootstrapQualifier": "<<<qualifier>>>"
}
```

Then add in a new file in the project root named environment.py the following code.
```python
import aws_cdk as cdk

def get_environment() -> cdk.Environment:
    return cdk.Environment(region='eu-west-1', account='<<<account number>>>')
```

In the storage package at the following code in your `__init__.py`
```python
import aws_cdk as cdk

from environment import get_environment
from constructs import Construct

class StorageSetupStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, name: str) -> None:
        super().__init__(scope, construct_id, env=get_environment())
```

Add method to StorageSetupStack
```python
def add_s3_buckets(self, name: str):
    self.bronze_bucket: cdk.aws_s3.Bucket = cdk.aws_s3.Bucket(self, f"BronzeBucket{name}",
                      block_public_access=cdk.aws_s3.BlockPublicAccess.BLOCK_ALL,
                      removal_policy=cdk.RemovalPolicy.DESTROY,
                      access_control=cdk.aws_s3.BucketAccessControl.PRIVATE,
                      )
```

call method from constructor
```python

class StorageSetupStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, name: str) -> None:
        super().__init__(scope, construct_id, env=get_environment())

        self.add_s3_buckets(name)
```

add base setup to application
```python
#!/usr/bin/env python3
import aws_cdk as cdk
from storage import StorageSetupStack

name = "<<<your name>>>"
app = cdk.App()

storage = StorageSetupStack(app, f"storage-setup-{name}", name)

app.synth()
```

synthesize stack
```shell
cdk synth 
```

bootstrap stack
```shell
cdk bootstrap --profile data --qualifier <<<qualifier>>> --toolkit-stack-name <<<qualifier-toolkit>>>
```

deploy stack
```shell
cdk deploy storage-setup-<<<name>>> --profile data
```
