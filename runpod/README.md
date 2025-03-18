# RunPod Manager

A comprehensive toolkit for managing RunPod resources programmatically. This package provides a core library, command-line interface, and AWS Lambda integration for RunPod management operations.

## Features

- List, create, start, stop, and terminate RunPod instances
- List available GPU types and find GPUs by name pattern
- Detailed pod information retrieval
- JSON output support for scripting
- AWS Lambda integration ready

## Components

1. **runpod_manager.py** - Core library with all RunPod management functionality
2. **runpod_cli.py** - Command-line interface for RunPod operations
3. **lambda_function.py** - AWS Lambda handler for serverless RunPod management

## Setup

### Prerequisites

- Python 3.6+
- RunPod account and API key

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/runpod-manager.git
   cd runpod-manager
   ```

2. Install the required packages:
   ```bash
   pip install runpod python-dotenv
   ```

3. Create a `.env` file with your RunPod API key:
   ```
   RUNPOD_API_KEY=your_api_key_here
   ```

## Usage

### Command Line Interface

The CLI provides easy access to all RunPod management functions:

#### List all pods:
```bash
./runpod_cli.py list-pods
```

#### Get pod details:
```bash
./runpod_cli.py get-pod pod_id_here
```

#### Create a new pod:
```bash
./runpod_cli.py create-pod \
  --name "my-pod" \
  --image "your-image:latest" \
  --gpu-type "RTX 3070" \
  --cloud-type "COMMUNITY" \
  --env "AWS_ACCESS_KEY_ID=your_key" \
  --env "AWS_SECRET_ACCESS_KEY=your_secret"
```

#### Stop a pod:
```bash
./runpod_cli.py stop-pod pod_id_here
```

#### Start a pod:
```bash
./runpod_cli.py start-pod pod_id_here
```

#### Terminate a pod:
```bash
./runpod_cli.py terminate-pod pod_id_here
```

#### List GPU types:
```bash
./runpod_cli.py list-gpu-types
```

#### Find GPU by name:
```bash
./runpod_cli.py find-gpu "3070"
```

### Using in Python Code

```python
from runpod_manager import RunPodManager

# Initialize the manager
manager = RunPodManager(api_key="your_api_key_here")

# List all pods
pods = manager.list_pods()

# Create a pod
new_pod = manager.create_pod(
    name="my-pod",
    image="your-image:latest",
    gpu_type_id="NVIDIA GeForce RTX 3070",
    cloud_type="COMMUNITY",
    env_vars={"KEY": "VALUE"}
)

# Get pod details
pod_details = manager.get_pod(new_pod["id"])

# Stop a pod
manager.stop_pod(new_pod["id"])

# Terminate a pod
manager.terminate_pod(new_pod["id"])
```

### AWS Lambda Integration

1. Package the code for Lambda:
   ```bash
   zip -r runpod_lambda.zip runpod_manager.py lambda_function.py
   ```

2. Upload to AWS Lambda and set the handler to `lambda_function.lambda_handler`

3. Set the environment variable `RUNPOD_API_KEY` in your Lambda configuration

4. Call the Lambda function with a payload like:
   ```json
   {
     "action": "create-pod",
     "params": {
       "name": "lambda-pod",
       "image": "your-image:latest",
       "gpu_type_id": "RTX 3070",
       "cloud_type": "COMMUNITY",
       "env_vars": {
         "KEY1": "VALUE1",
         "KEY2": "VALUE2"
       }
     }
   }
   ```

## Best Practices for Lambda

1. **Security**: Store the RunPod API key in AWS Secrets Manager for production use
2. **Error Handling**: The Lambda handler includes comprehensive error handling
3. **Logging**: All operations are logged for troubleshooting
4. **Timeout**: Set an appropriate timeout for your Lambda function (30+ seconds recommended)

## Common Use Cases

1. **Scheduled Pod Management**: Use AWS EventBridge to schedule pod creation/termination
2. **Cost Optimization**: Automatically shut down idle pods
3. **Auto-Scaling**: Dynamically create pods based on workload
4. **Web Integration**: Connect to a web frontend via API Gateway

## Tips

- Use the `find-gpu` command to locate the exact GPU ID for pod creation
- For scripts, use the `--json` flag to get machine-readable output
- When creating a Lambda deployment package, include all dependencies

## Troubleshooting

- **API Key Issues**: Ensure your RunPod API key is valid and has the appropriate permissions
- **GPU Availability**: If pod creation fails, check GPU availability in the RunPod marketplace
- **Networking**: For custom images, ensure required ports are exposed

## License

MIT
