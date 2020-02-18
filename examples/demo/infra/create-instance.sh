export ID="01"
export SERVICE_ACCOUNT="ai4ops-service-account@gd-gcp-techlead-experiments.iam.gserviceaccount.com"
export BASE_NAME="ai4ops-jupyter-${ID}"

gcloud compute instances create "${BASE_NAME}" \
    --zone=us-central1-a \
    --machine-type=n1-standard-1 \
    --maintenance-policy=MIGRATE \
    --service-account="${SERVICE_ACCOUNT}" \
    --scopes=cloud-platform,storage-full \
    --min-cpu-platform=Automatic \
    --tags=jupyter \
    --image="coreos-stable-2191-5-0-v20190904" \
    --image-project="coreos-cloud" \
    --boot-disk-size=40GB \
    --boot-disk-type=pd-ssd \
    --boot-disk-device-name="${BASE_NAME}-boot" \
    --disk=name="${BASE_NAME}-data-0,device-name=jupyter-data,mode=rw,boot=no" \
    --no-address