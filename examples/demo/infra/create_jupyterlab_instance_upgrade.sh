#!/bin/bash

export ID="02"
export SERVICE_ACCOUNT="kos-ai4ops@kohls-kos-cicd.iam.gserviceaccount.com"
export SUBNET="projects/kohls-kos-xpn-cicd/regions/us-central1/subnetworks/kos-cicd-05-central1-prv01"
export BASE_NAME="ai4ops-jupyter-ds-${ID}"


gcloud compute instances create "${BASE_NAME}" \
    --zone=us-central1-a \
    --machine-type=n1-standard-1 \
    --subnet="${SUBNET}" \
    --maintenance-policy=MIGRATE \
    --service-account="${SERVICE_ACCOUNT}" \
    --scopes=cloud-platform,storage-full \
    --min-cpu-platform=Automatic \
    --tags=jupyter \
    --image="com-kohls-kship-mw-base-os-docker-vm-3-18-0" \
    --image-project="kohls-kos-cicd" \
    --boot-disk-size=40GB \
    --boot-disk-type=pd-ssd \
    --boot-disk-device-name="${BASE_NAME}-boot" \
    --disk=name="${BASE_NAME}-data-0,device-name=jupyter-data,mode=rw,boot=no" \
    --no-address

gcloud compute instances add-metadata "${BASE_NAME}" \
    --metadata=patching-type=2,patch-window-week=1,patch-window-day=4,patch-window-hour=23 \
    --zone=us-central1-a

gcloud compute instances add-labels "${BASE_NAME}" \
    --labels=environment-type=dev,environment-name=ai4ops,technology=jupyter \
    --zone=us-central1-a

#Install
#ssh to vm
sudo mkdir -p /mnt/disks/jupyter-data
sudo mount -o discard,defaults /dev/sdb /mnt/disks/jupyter-data
sudo chmod a+w /mnt/disks/jupyter-data
sudo cp /etc/fstab /etc/fstab.backup
echo UUID=`sudo blkid -s UUID -o value /dev/sdb` /mnt/disks/jupyter-data ext4 discard,defaults,nofail 0 2 | sudo tee -a /etc/fstab
cat /etc/fstab

sudo su
#export https_proxy="http://${1}:${2}@10.4.7.10:3128"
docker login -u oauth2accesstoken -p "$(gcloud auth print-access-token)" https://gcr.io
ID=$(id -u)

docker run \
  -d \
  --restart always \
  --network host \
  --name=ai4ops-jupyter \
  --user $ID -v /mnt/disks/jupyter-data/data:/home/jovyan/work/data \
  -v $HOME/.config/gcloud:/home/jovyan/.config/gcloud \
  gcr.io/kohls-kos-cicd/ai4ops_jupyterlab_image:2.1565263866  /opt/conda/bin/jupyter lab --allow-root --port 8080

#ssh to vm
sudo su
#Generate password
#date +%s | sha256sum | base64 | head -c 32 ; echo
docker exec -it ai4ops-jupyter bash
#Set password
jupyter notebook password
exit
docker restart ai4ops-jupyter
