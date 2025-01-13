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


# Training Instructions for Pollution Levels Classification Pipeline

## Environment Setup
1. **Tools & Libraries Required:**
   - AWS SageMaker Studio
   - Jupyter Lab in SageMaker
   - SparkMLlib for data processing and model training
   - Python and required dependencies (ensure `inference.py` dependencies are installed)

## Directory Structure
Ensure the following files are located in the `training/` directory:
- `model_training.ipynb`
- `deploy.ipynb`
- `inference.py`
- `invoke.ipynb`

## Step-by-Step Process

### 1. Upload Notebooks to SageMaker
   - Open AWS SageMaker Studio.
   - Navigate to the Jupyter Lab interface.
   - Upload all the files from the `training/` directory to your SageMaker workspace.

### 2. Model Training & Export
   - Open and run the `model_training.ipynb` notebook:
     - **Step 1:** Perform Exploratory Data Analysis (EDA) to understand the dataset.
     - **Step 2:** Train the classification model using SparkMLlib.
     - **Step 3:** Export the trained model and upload it to an S3 bucket for deployment.

### 3. Model Deployment
   - Run the `deploy.ipynb` notebook to:
     - **Create an Endpoint:** Configure and deploy the trained model endpoint using SageMaker.
     - **Deploy Model:** Ensure successful endpoint creation by monitoring deployment status.

### 4. Configure Inference Dependencies
   - Ensure all dependencies listed in `inference.py` are correctly installed in the SageMaker environment.

### 5. Model Invocation for Prediction
   - Open and run the `invoke.ipynb` notebook to:
     - **Send Requests to the Endpoint:** Use sample input data for testing.
     - **Receive Predictions:** Confirm the endpoint’s response for air pollution classification tasks.

## Additional Notes
- Monitor SageMaker logs during each step to catch potential errors.
- Verify S3 bucket permissions to ensure smooth upload and access to the model.
- Keep track of the endpoint name for invoking predictions effectively.
