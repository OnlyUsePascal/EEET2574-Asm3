# EEET2574 - Assignment 3 - Group Project

Team Members

- Dat Pham      s3927188

- Huan Nguyen   3927467

- Nhan Truong   s3929215

- Long Nguyen   s3904632

Project Structure

```
.
├── etl-glue/
├── producer-air/
├── producer-traffic/
├── producer-weather/
├── training/
├── docker-compose.yml
├── README.md
└── training/
    ├── DataInspection.ipynb
    └── train.csv
```

- `etl-glue`: contains glucozo

- `producer-*`: module for API ingestion and uploading to S3 storage via Firehose

- `training`: ???

# How to run Project

## Producers

```bash
mv ~/Downloads/labsuser.pem ~/.ssh/labsuser.pem

chmod 0400 ~/.ssh/labsuser.pem

# ec2 access config
EC2_CRED="~/.ssh/labsuser.pem"
EC2_USER="ec2-user"
EC2_DNS="ec2-107-23-22-182.compute-1.amazonaws.com"
EC2_PATH="/home/${EC2_USER}/projects/test1"

# upload to ec2 instance
scp -i ${EC2_CRED} -r ./* ${EC2_USER}@${EC2_DNS}:${EC2_PATH}

# access ec2 remotely
ssh -i ${EC2_CRED} "${EC2_USER}@${EC2_DNS}"


```

- change AWS credentials, firehose, api key config

- upload to EC2

- run & profit

## ETL 
- change AWS credentials, S3 source & sink, Mongodb sink 

- setup crawlers. 

- upload scripts to AWS Glue

- run etl for each topic, then finally `combine-etl`



## Training