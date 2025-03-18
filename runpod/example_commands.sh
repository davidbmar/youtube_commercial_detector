#!/bin/bash
# Example RunPod Manager usage scenarios

# Make the scripts executable
chmod +x runpod_cli.py

# 1. Check available GPU types (useful to find the correct GPU ID)
./runpod_cli.py list-gpu-types

# 2. Find a specific GPU by name (e.g., RTX 3070)
./runpod_cli.py find-gpu "3070"

# 3. Create a pod with the Whisper image on an RTX 3070
./runpod_cli.py create-pod \
  --name "whisper-pod" \
  --image "davidbmar/whisper-runpod:latest" \
  --gpu-type "3070" \
  --env "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
  --env "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
  --env "AWS_DEFAULT_REGION=us-east-1"

# 4. List all pods to get the pod ID
./runpod_cli.py list-pods

# 5. Get detailed info about a specific pod
./runpod_cli.py get-pod "pod_id_from_previous_command"

# 6. Stop a pod (pause it)
./runpod_cli.py stop-pod "pod_id_here"

# 7. Start a previously stopped pod
./runpod_cli.py start-pod "pod_id_here"

# 8. Terminate a pod (delete it completely)
./runpod_cli.py terminate-pod "pod_id_here"

# 9. Create a pod with a different GPU using JSON output
./runpod_cli.py create-pod \
  --name "training-pod" \
  --image "tensorflow/tensorflow:latest-gpu" \
  --gpu-type "A100" \
  --json

# 10. Use environment variables for API key
export RUNPOD_API_KEY="your_api_key_here"
./runpod_cli.py list-pods

# 11. Create a pod with multiple environment variables
./runpod_cli.py create-pod \
  --name "multi-env-pod" \
  --image "pytorch/pytorch:latest" \
  --gpu-type "3070" \
  --env "BATCH_SIZE=64" \
  --env "LEARNING_RATE=0.001" \
  --env "EPOCHS=10"
