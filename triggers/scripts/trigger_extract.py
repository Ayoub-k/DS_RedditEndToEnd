from google.cloud import pubsub_v1
from paths import Paths
import os

project_id = 'mygcp-402006'
subscription_name = 'get-trigger'
# Path to your script or command to execute the workflow
workflow_script_path = f'{Paths.get_project_root_path_from_env()}/triggers/scripts/workflow.sh'

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def callback(message):
    print(f"Received message: {message.data}. Executing the workflow...")
    # Execute your workflow script
    os.system(f"sh {workflow_script_path} env etl/reddit/extract.py")
    message.ack()

subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

# Keep the script running to continuously listen for messages
while True:
    pass
