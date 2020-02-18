#!/bin/bash
export SERVICE_ACCOUNT="ai4ops-service-account@gd-gcp-techlead-experiments.iam.gserviceaccount.com"

gcloud dataproc clusters create ai4ops \
    --service-account=${SERVICE_ACCOUNT} \
    --bucket=ai4ops \
    --scopes=default,storage-rw,compute-rw,cloud-platform \
    --image-version=1.4 \
    --zone=us-central1-a \
    --master-boot-disk-size=200GB \
    --worker-boot-disk-size=200GB \
    --properties=spark:spark.executor.cores=3,spark:spark.executor.instances=3,spark:spark.executor.memory=2048m \
    --metadata 'CONDA_PACKAGES=scipy=1.1.0 tensorflow=1.12.0' \
    --metadata 'PIP_PACKAGES=matplotlib==3.1.0 pandas==0.24.2 scipy==1.1.0 google-cloud-bigquery==1.10.0 google-cloud-storage==1.14.0 pandas-gbq==0.9.0 pyarrow==0.14.0 fastparquet==0.1.6 cryptography==2.2.2 scikit-learn==0.21.1 google-api-python-client==1.7.8 keras==2.2.4 mysql-connector-python==8.0.16' \
    --num-workers=2 \
    --initialization-actions \
    gs://ai4ops/dataproc-initialization-actions/conda-install.sh,gs://ai4ops/dataproc-initialization-actions/pip-install.sh