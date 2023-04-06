# PORTAL-TRANSPARENCIA-VIAGENS-NORMALIZATION-AND-SAVE-IN-RDS

The objective of this project is to normalize the "PORTAL TRANSPARENCIA VIAGENS" that are in S3, save this data in a new folder in S3, then structure the data and save it in RDS (Postgresql).

### Requirements

* Python 3.8.0
* Node.js 16.16.0
* Configure [AWS credentials](https://serverless.com/framework/docs/providers/aws/guide/credentials/)
* Make sure you have Docker running

### Installation

In order to deploy the script in the AWS please run the following commands:

```sh
# install whell
pip install wheel
# create wheel file
python setup.py bdist_wheel
# install serverless
npm install -g serverless
# install the dependencies that are in the file package.json
npm install
# deploy the project in the development environment on AWS
sls deploy --stage dev
```