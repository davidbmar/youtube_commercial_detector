#!/bin/bash
# Example RunPod Manager usage scenarios with proper secrets handling
# Make the scripts executable
chmod +x runpod_cli.py

# Make sure you've created these secrets in RunPod beforehand:
# - aws_access_key_id
# - aws_secret_access_key
# - aws_region

# 1. Check available GPU types (useful to find the correct GPU ID)
./runpod_cli.py list-gpu-types

# 2. Find a specific GPU by name (e.g., RTX 3070)
./runpod_cli.py find-gpu "3070"

# 3. Create a pod with the Whisper image using AWS secrets (using full GPU ID from find-gpu)
./runpod_cli.py create-pod \
  --name "whisper-pod" \
  --image "davidbmar/whisper-runpod:latest" \
  --gpu-type "NVIDIA GeForce RTX 3080" \  
  --use-aws-secrets

# 4. Alternative way to create a pod with individual secret specification
#./runpod_cli.py create-pod \
#  --name "whisper-pod-alt" \
#  --image "davidbmar/whisper-runpod:latest" \
#  --gpu-type "3080" \  
#  --secret "aws_access_key_id" \
#  --secret "aws_secret_access_key" \
#  --secret "aws_region" \
#  --env "AWS_ACCESS_KEY_ID={{ RUNPOD_SECRET_aws_access_key_id }}" \
#  --env "AWS_SECRET_ACCESS_KEY={{ RUNPOD_SECRET_aws_secret_access_key }}" \
#  --env "AWS_DEFAULT_REGION={{ RUNPOD_SECRET_aws_region }}"
#  The backslash is intrepreted wrong so just use it all one line like this:

./runpod_cli.py create-pod --name "whisper-pod-alt" --image "davidbmar/whisper-runpod:latest" --gpu-type "3080" --secret "aws_access_key_id" --secret "aws_secret_access_key" --secret "aws_region" --env "AWS_ACCESS_KEY_ID={{ RUNPOD_SECRET_aws_access_key_id }}" --env "AWS_SECRET_ACCESS_KEY={{ RUNPOD_SECRET_aws_secret_access_key }}" --env "AWS_DEFAULT_REGION={{ RUNPOD_SECRET_aws_region }}"


# 5. List all pods to get the pod ID
./runpod_cli.py list-pods

# 6. Get detailed info about a specific pod
./runpod_cli.py get-pod "pod_id_from_previous_command"

# 7. Stop a pod (pause it)
./runpod_cli.py stop-pod "pod_id_here"

# 8. Start a previously stopped pod
./runpod_cli.py start-pod "pod_id_here"

# 9. Terminate a pod (delete it completely)
./runpod_cli.py terminate-pod "pod_id_here"

# 10. Create a pod with both secrets and regular environment variables
./runpod_cli.py create-pod \
  --name "multi-env-pod" \
  --image "pytorch/pytorch:latest" \
  --gpu-type "3070" \
  --use-aws-secrets \
  --env "BATCH_SIZE=64" \
  --env "LEARNING_RATE=0.001" \
  --env "EPOCHS=10"

# 11. Create a pod with JSON output
./runpod_cli.py create-pod \
  --name "training-pod" \
  --image "tensorflow/tensorflow:latest-gpu" \
  --gpu-type "A100" \
  --use-aws-secrets \
  --json
