export PROJECT_ID=""
export PROJECT_NUMBER=""
export REGION=""
export CLUSTER_NAME=$PROJECT_ID-dp-cluster-e
export GCS_BUCKET_NAME=$PROJECT_ID-services
export IMAGE_NAME=reddit-data-cluster

gcloud config set project $PROJECT_ID

# enable apis
gcloud services enable storage-component.googleapis.com 
gcloud services enable compute.googleapis.com  
gcloud services enable metastore.googleapis.com
gcloud services enable servicenetworking.googleapis.com 
gcloud services enable iam.googleapis.com 
gcloud services enable dataproc.googleapis.com
gcloud services enable cloudbilling.googleapis.com

# create bucket and upload scripts
gsutil mb gs://$GCS_BUCKET_NAME
gsutil cp scripts/small_file_generator.py gs://$GCS_BUCKET_NAME/scripts/
gsutil cp scripts/customize.sh gs://$GCS_BUCKET_NAME/scripts/
gsutil cp scripts/initialize.sh gs://$GCS_BUCKET_NAME/scripts/

# Allow external IP access
echo "{
  \"constraint\": \"constraints/compute.vmExternalIpAccess\",
	\"listPolicy\": {
	    \"allValues\": \"ALLOW\"
	  }
}" > external_ip_policy.json

gcloud resource-manager org-policies set-policy external_ip_policy.json --project=$PROJECT_ID

# edit the template.yaml
sed -i "s|%%PROJECT_ID%%|$PROJECT_ID|g" templates/pyspark-workflow-template.yaml
sed -i "s|%%GCS_BUCKET_NAME%%|$GCS_BUCKET_NAME|g" templates/pyspark-workflow-template.yaml
sed -i "s|%%CLUSTER_NAME%%|$CLUSTER_NAME|g" templates/pyspark-workflow-template.yaml
sed -i "s|%%REGION%%|$REGION|g" templates/pyspark-workflow-template.yaml
sed -i "s|%%IMAGE_NAME%%|$IMAGE_NAME|g" templates/pyspark-workflow-template.yaml


# clone and execute the custom image generator

git clone https://github.com/GoogleCloudDataproc/custom-images.git

cd custom-images

python generate_custom_image.py \
  --image-name $IMAGE_NAME \
  --dataproc-version 2.0.47-debian10 \
  --customization-script ../scripts/customize.sh \
  --zone $REGION-a \
  --gcs-bucket gs://$GCS_BUCKET_NAME \
  --disk-size 50 \
  --no-smoke-test

  # create the workflow

gcloud dataproc workflow-templates instantiate-from-file \
  --file templates/pyspark-workflow-template.yaml \
  --region $REGION