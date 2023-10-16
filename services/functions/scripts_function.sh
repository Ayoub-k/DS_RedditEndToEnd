gcloud functions deploy trigger_workflow \
  --runtime python310 \
  --trigger-http \
  --allow-unauthenticated \
  --source=$PROJECT_PATH""services/functions/trigger_workflow/


