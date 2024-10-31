LOCATION=us-central1        
ENVIRONMENT_NAME=airline-composer
# Get list of DAG IDs - updated to handle table format output
DAG_IDS=$(gcloud composer environments run ${ENVIRONMENT_NAME} --location ${LOCATION} dags list | tail -n +4 | awk '{print $1}')

# Trigger each DAG
for dag_id in ${DAG_IDS}; do
    echo "Triggering DAG: ${dag_id}"
    gcloud composer environments run ${ENVIRONMENT_NAME} --location ${LOCATION} dags trigger -- ${dag_id}
done
