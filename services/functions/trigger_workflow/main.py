"""trigger extract using pub/sub"""

import functions_framework
from google.cloud import pubsub_v1
import os

project_id = 'mygcp-402006'
topic_name = 'invoke-extract'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

def run_workflow(request):
    data = "Run workflow"
    future = publisher.publish(topic_path, data.encode())
    return 'Workflow execution triggered.'

# Entry point for the Cloud Function
@functions_framework.http
def trigger_workflow(request):
    # Run the workflow
    run_workflow(request)
    print("Test")
    return 'Workflow triggered.'