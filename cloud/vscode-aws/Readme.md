# AWS Examples

## Prerequisites

You need to install the devcontainers extension: https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers


## Configure aws cli

You can configure the aws cli to use the awsacademy Sandbox Environment running the follow commands:

```bash
aws configure set aws_access_key_id <my_aws_access_key_id> --profile default
aws configure set aws_secret_access_key <my_aws_secret_access_key> --profile default
aws configure set aws_session_token <my_aws_session_token> --profile default
```

## Basic commands

The AWS CLI is a powerful tool for managing your AWS services from the command line. The commands follow a consistent structure: `aws <service> <command> [parameters]`.

Here are some of the main and most common commands, organized by service.

---

### **General and Configuration Commands**

* `aws configure`: The first command you'll use. It allows you to set up your access credentials, default region, and output format.
* `aws help`: Displays a list of all available services. If you use it with a service, like `aws s3 help`, it will show you a list of all the commands for that service.
* `aws --version`: Shows the current version of the AWS CLI.

### **Amazon S3 (Storage)**

S3 commands are very intuitive and resemble file system commands.

* `aws s3 ls`: Lists all the buckets in your account.
* `aws s3 ls s3://your-example-bucket`: Lists the objects (files) inside a specific bucket.
* `aws s3 mb s3://your-unique-bucket-name`: Creates a new bucket (`mb` = make bucket).
* `aws s3 cp your-file.txt s3://your-example-bucket/`: Copies a local file to an S3 bucket.
* `aws s3 sync . s3://your-example-bucket`: Synchronizes a complete local directory with an S3 bucket.

### **Amazon EC2 (Compute)**

For managing your virtual machines.

* `aws ec2 describe-instances`: Shows detailed information about all your EC2 instances.
* `aws ec2 start-instances --instance-ids i-0a1b2c3d4e5f6g7h8`: Starts one or more instances using their IDs.
* `aws ec2 stop-instances --instance-ids i-0a1b2c3d4e5f6g7h8`: Stops one or more instances.
* `aws ec2 run-instances --image-id ami-0123456789abcdef0 --count 1 --instance-type t2.micro`: Launches a new EC2 instance.

### **AWS IAM (Identity and Access Management)**

For managing users, groups, and policies.

* `aws iam list-users`: Displays a list of all IAM users in your account.
* `aws iam create-user --user-name NewDeveloper`: Creates a new IAM user.
* `aws iam list-attached-user-policies --user-name MyUser`: Lists the policies attached to a user.

### **AWS Lambda (Serverless)**

For managing your serverless functions.

* `aws lambda list-functions`: Lists all Lambda functions in your account.
* `aws lambda invoke --function-name MyLambdaFunction --payload '{"key":"value"}' response.txt`: Invokes a Lambda function and saves the response to a file.
* `aws lambda delete-function --function-name MyLambdaFunction`: Deletes a Lambda function.

To see all available commands and options for a service, remember to use the `help` command. For example, `aws s3api help` will give you the complete list of S3 API commands.

# Module Structure

# AWSCloudFormation folder
Contains CloudFormation templates samples:
-  BootstrapTutorialStack.template: example template from tutorial https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/deploying.applications.html

- LAMP_Single_Instance_With_RDS.template: a lamp with RDS database. 
Other templates in https://aws.amazon.com/es/cloudformation/templates/aws-cloudformation-templates-us-west-1/

# aws_s3_create_file_inmemory.py file
A simple python app to create a in-memory file in a S3 bucket.
