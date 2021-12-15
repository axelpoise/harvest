# Lake formation

simulate policies
```shell
aws iam simulate-principal-policy  --policy-source-arn "arn:aws:iam::<acount-id>:role/<role-name>" --action-names "s3:PutObject" "s3:*" --profile harvest
```

## Create Roles
admin role
```python
    def create_aws_data_lake_admin(self, workflow_role: aws_iam.Role) -> aws_iam.Role:
        lake_admin_managed_policy = aws_iam.ManagedPolicy \
            .from_aws_managed_policy_name(managed_policy_name='AWSLakeFormationDataAdmin')
        glue_console_access_managed_policy = aws_iam.ManagedPolicy \
            .from_aws_managed_policy_name(managed_policy_name='AWSGlueConsoleFullAccess')
        cloud_watch_read_only_access_managed_policy = aws_iam.ManagedPolicy \
            .from_aws_managed_policy_name(managed_policy_name='CloudWatchLogsReadOnlyAccess')
        athena_full_access_managed_policy = aws_iam.ManagedPolicy \
            .from_aws_managed_policy_name(managed_policy_name='AmazonAthenaFullAccess')

        data_lake_admin_role = aws_iam.Role(self, "DataLakeAdminRole",
                                            managed_policies=[lake_admin_managed_policy,
                                                              glue_console_access_managed_policy,
                                                              cloud_watch_read_only_access_managed_policy,
                                                              athena_full_access_managed_policy],
                                            assumed_by=aws_iam.AccountPrincipal(account_id=self.account))
        data_lake_admin_role.add_to_policy(
            aws_iam.PolicyStatement(actions=["iam:CreateServiceLinkedRole"],
                                    effect=aws_iam.Effect.ALLOW,
                                    resources=["*"],
                                    conditions={
                                        "StringEquals": {
                                            "iam:AWSServiceName": "lakeformation.amazonaws.com"
                                        }
                                    }))
        data_lake_admin_role.add_to_policy(
            aws_iam.PolicyStatement(actions=[
                "iam:PutRolePolicy"
            ],
                effect=aws_iam.Effect.ALLOW,
                resources=[
                    f"arn:aws:iam::{self.account}:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess"])
        )
        data_lake_admin_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "iam:PassRole"
                ],
                effect=aws_iam.Effect.ALLOW,
                resources=[
                    f"arn:aws:iam::{self.account}:role/{workflow_role.role_name}"
                ],
                sid="PassRolePermissions"
            )
        )
        return data_lake_admin_role
```
engineer role
```python
    def create_datalake_engineer(self, workflow_role: aws_iam.Role) -> aws_iam.Role:
        datalake_engineer_role = aws_iam.Role(self, "DataLakeEngineerRole",
                                              assumed_by=aws_iam.AccountPrincipal(self.account),
                                              description="role for datalake engineer",
                                              managed_policies=[
                                                  aws_iam.ManagedPolicy.
                                              from_aws_managed_policy_name(
                                                      managed_policy_name='AWSGlueConsoleFullAccess'
                                                  )
                                              ])

        datalake_engineer_role.add_to_policy(aws_iam.PolicyStatement(
            actions=[
                "cloudformation:*",
                "lakeformation:GetDataAccess",
                "lakeformation:GrantPermissions",
                "lakeformation:RevokePermissions",
                "lakeformation:BatchGrantPermissions",
                "lakeformation:BatchRevokePermissions",
                "lakeformation:ListPermissions",
                "iam:CreateRole",
                "iam:CreatePolicy",
                "iam:AttachRolePolicy"
            ],
            effect=aws_iam.Effect.ALLOW,
            resources=['*']
        ))
        datalake_engineer_role.add_to_policy(aws_iam.PolicyStatement(
            actions=[
                "iam:PassRole"
            ],
            effect=aws_iam.Effect.ALLOW,
            resources=[f'arn:aws:iam::{self.account}:role/{workflow_role.role_name}']
        ))
        return datalake_engineer_role
```
analyst role
```python
    def create_datalake_analyst(self, log_bucket: aws_s3.Bucket) -> aws_iam.Role:
        datalake_analyst_role = aws_iam.Role(self, "DataLakeAnalystRole",
                                             assumed_by=aws_iam.AccountPrincipal(account_id=self.account),
                                             description="role for analysts",
                                             managed_policies=[
                                                 aws_iam.ManagedPolicy
                                             .from_aws_managed_policy_name(managed_policy_name='AmazonAthenaFullAccess')
                                             ])

        datalake_analyst_role.add_to_policy(aws_iam.PolicyStatement(
            actions=[
                "lakeformation:GetDataAccess",
                "glue:GetTable",
                "glue:GetTables",
                "glue:SearchTables",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetPartitions"
            ],
            effect=aws_iam.Effect.ALLOW,
            resources=['*']
        ))

        datalake_analyst_role.add_to_policy(aws_iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            effect=aws_iam.Effect.ALLOW,
            resources=[f"{log_bucket.bucket_arn}/athena-logs",
                       f"{log_bucket.bucket_arn}/athena-logs/*"]
        ))

        return datalake_analyst_role

```
workflow role
```python
    def create_workflow_role(self) -> aws_iam.Role:
        workflow_role = aws_iam.Role(self, "WorkFlowRole",
                                     assumed_by=aws_iam.AccountPrincipal(account_id=self.account),
                                     managed_policies=[
                                         aws_iam.ManagedPolicy
                                     .from_aws_managed_policy_name(
                                             managed_policy_name='service-role/AWSGlueServiceRole')
                                     ])
        workflow_role.add_to_policy(aws_iam.PolicyStatement(
            actions=[
                "lakeformation:GetDataAccess",
                "lakeformation:GrantPermissions"
            ],
            effect=aws_iam.Effect.ALLOW,
            resources=['*'],
            sid='Lakeformation'))

        workflow_role.add_to_policy(aws_iam.PolicyStatement(
            actions=[
                "iam:PassRole"
            ],
            effect=aws_iam.Effect.ALLOW,
            resources=[f"arn:aws:iam::{self.account}:role/{workflow_role.role_name}"]))

        workflow_role.add_to_policy(aws_iam.PolicyStatement(
            actions=["s3:GetObject", "s3:ListBucket"],
            effect=aws_iam.Effect.ALLOW,
            resources=["arn:aws:s3:::*"]
        ))


        return workflow_role
```
synth stack
```shell
cdk synth --app 'python3 app.py' --profile harvest
```
deploy stack
```shell
cdk deploy lakeformation-setup --profile harvest
```
## Test access for roles
get roles
```shell
aws iam get-roles --profile harvest
```
test policy for analyst
```shell
aws iam simulate-principal-policy  --policy-source-arn "arn:aws:iam::<acount-id>:role/<role-name>" --action-names "athena:*" --profile harvest
```
## Setup lake formation
get lake formation settings
```shell
aws lakeformation get-data-lake-settings --profile harvest
```
create data lake settings resource
```python
aws_lakeformation.CfnDataLakeSettings(self, "DataLake",
                                      admins=[
                                          aws_lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                                          data_lake_principal_identifier=data_admin_role.role_arn),
                                          aws_lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                                              data_lake_principal_identifier=aws_iam.User
                                                  .from_user_name(self, "HarvestUser", 
                                                 user_name="harvest").user_arn
                                          )
                                      ])
```
synth stack
```shell
cdk synth --app 'python3 app.py' --profile harvest
```
deploy stack
```shell
cdk deploy lakeformation-setup --profile harvest
```
get settings
```shell
aws lakeformation get-data-lake-settings --profile harvest
```
put data lake settings for future databases
```shell
aws lakeformation put-data-lake-settings --data-lake-settings '{ "DataLakeAdmins": [ { "DataLakePrincipalIdentifier": "arn:aws:iam::<account>:role/<lake formation admin role>"}, <harvest user principal>], "CreateDatabaseDefaultPermissions": [ { "Principal": { "DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS" }, "Permissions": [ ] } ], "CreateTableDefaultPermissions": [ { "Principal": { "DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS" }, "Permissions": [ ] } ], "TrustedResourceOwners": [] }' --profile harvest
```
## Lake formation permissions
lake formation permissions resource
```python
aws_lakeformation.CfnPermissions(self, f"Permissions{id_}",
                  data_lake_principal=  
                 aws_lakeformation.CfnPermissions.DataLakePrincipalProperty(
                                     data_lake_principal_identifier=principal_arn),
                                 resource=resource,
                                 permissions=permissions,
                                 permissions_with_grant_option=grantable_permissions)
```
lake formation permissions
```python
class LakeFormationPermissions:
    ALTER = "ALTER"
    CREATE_DATABASE = "CREATE_DATABASE"
    CREATE_TABLE = 'CREATE_TABLE'
    DATA_LOCATION_ACCESS = "DATA_LOCATION_ACCESS"
    DELETE = 'DELETE'
    DESCRIBE = 'DESCRIBE'
    DROP = 'DROP'
    INSERT = 'INSERT'
    SELECT = 'SELECT'
    SUPER = 'ALL'
```

### Create permissions for the analyst in the LakeformationPermissionsAdministrationStack
In the permissions administration stack add the resource names.
```python
database_arn = f"arn:aws:glue:f{self.region}:{self.account}:database/" + "harvestdb"
table_arn = f"arn:aws:glue:{self.region}:{self.account}:table/" + "harvestdb" + "/" + "base_setup_rawbucket0c3ee094_14eptovdemao"

analyst = f"arn:aws:iam::{self.account}:role/lakeformation-setup-DataLakeAnalystRoleName"
```
create table permissions resource.
```python
database = aws_glue.Database.from_database_arn(self, f"TableDB{id_}", database_arn=database_arn)
table = aws_glue.Table.from_table_arn(self, f"Table{id_}", table_arn=table_arn)
resource = aws_lakeformation.CfnPermissions.ResourceProperty(
    table_resource=aws_lakeformation.CfnPermissions
        .TableResourceProperty(catalog_id=database.catalog_id,
                               database_name=database.database_name,
                               name=table.table_name)
)
```
create permissions for analyst with table permissions resource.
Where principal arn is analyst arn and permissions is a list of permissions.
```python
aws_lakeformation.CfnPermissions(self, f"{prefix}Permissions{id_}",
                  data_lake_principal=  
                 aws_lakeformation.CfnPermissions.DataLakePrincipalProperty(
                                     data_lake_principal_identifier=principal_arn),
                                 resource=resource,
                                 permissions=permissions,
                                 permissions_with_grant_option=grantable_permissions)
```

Create a database permissions resource.
```python
database = aws_glue.Database.from_database_arn(self, f"Database{id_}", database_arn=database_arn)
resource = aws_lakeformation.CfnPermissions.ResourceProperty(
    database_resource=aws_lakeformation.CfnPermissions
        .DatabaseResourceProperty(catalog_id=database.catalog_id, name=database.database_name)
)

```
And the corresponding permissions object.
Be aware that the Permissions ID needs to be different, this can be done with `prefix` and suffix `id_`.
```python
aws_lakeformation.CfnPermissions(self, f"{prefix}Permissions{id_}",
                  data_lake_principal=  
                 aws_lakeformation.CfnPermissions.DataLakePrincipalProperty(
                                     data_lake_principal_identifier=principal_arn),
                                 resource=resource,
                                 permissions=permissions,
                                 permissions_with_grant_option=grantable_permissions)
```
synth stack
```shell
cdk synth --app 'python3 app.py' --profile harvest
```
deploy stack
```shell
cdk deploy lakeformation-administration --profile harvest
```

### Check analyst datalake permissions
Assume analyst role.
```shell
aws sts assume-role --role-arn "arn:aws:iam::885617958043:role/<role-name>" --session-name analyst --profile harvest
```
Configure analyst profile.
```shell
aws configure --profile assumed-analyst
```
And add the session token.
```shell
aws configure set aws_session_token <token> --profile assumed-analyst
```
Get glue tables with analyst.
```shell
aws glue get-tables --database-name 'harvestdb' --profile assumed-analyst
```

## Register resources with lake formation
register s3 bucket
```python
aws_lakeformation.CfnResource(self, "RawBucketLakeformationRegistration",
                              resource_arn=bucket.bucket_arn,
                              use_service_linked_role=True,
                              role_arn=data_admin_role.role_arn)
```
synth stack
```shell
cdk synth --app 'python3 app.py' --profile harvest
```
deploy stack
```shell
cdk deploy lakeformation-setup --profile harvest
```