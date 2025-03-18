"""
AWS Lambda handler for RunPod management
"""

import os
import json
import logging
from runpod_manager import RunPodManager

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda handler for RunPod management
    
    Expected event structure:
    {
        "action": "create-pod|list-pods|get-pod|stop-pod|start-pod|terminate-pod|list-gpu-types|find-gpu",
        "params": {
            // Action-specific parameters
        }
    }
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    try:
        # Extract action and parameters
        action = event.get('action')
        params = event.get('params', {})
        
        if not action:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameter: action'})
            }
        
        # Initialize RunPod manager
        api_key = params.get('api_key') or os.environ.get('RUNPOD_API_KEY')
        if not api_key:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'RunPod API key not provided'})
            }
        
        manager = RunPodManager(api_key=api_key)
        
        # Process action
        if action == 'list-pods':
            result = manager.list_pods()
            
        elif action == 'get-pod':
            pod_id = params.get('pod_id')
            if not pod_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing required parameter: pod_id'})
                }
            result = manager.get_pod(pod_id)
            
        elif action == 'create-pod':
            required_params = ['name', 'image', 'gpu_type_id']
            for param in required_params:
                if param not in params:
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'error': f'Missing required parameter: {param}'})
                    }
            
            # If gpu_type is a name pattern rather than ID, try to resolve it
            gpu_type_id = params['gpu_type_id']
            if not any(c.isdigit() for c in gpu_type_id):
                gpu = manager.find_gpu_by_name(gpu_type_id)
                if gpu:
                    gpu_type_id = gpu['id']
                    logger.info(f"Resolved GPU type '{params['gpu_type_id']}' to ID '{gpu_type_id}'")
                else:
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'error': f"No GPU found matching '{params['gpu_type_id']}'"})
                    }
            
            result = manager.create_pod(
                name=params['name'],
                image=params['image'],
                gpu_type_id=gpu_type_id,
                cloud_type=params.get('cloud_type', 'COMMUNITY'),
                env_vars=params.get('env_vars')
            )
            
        elif action == 'stop-pod':
            pod_id = params.get('pod_id')
            if not pod_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing required parameter: pod_id'})
                }
            result = manager.stop_pod(pod_id)
            
        elif action == 'start-pod':
            pod_id = params.get('pod_id')
            if not pod_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing required parameter: pod_id'})
                }
            result = manager.start_pod(pod_id)
            
        elif action == 'terminate-pod':
            pod_id = params.get('pod_id')
            if not pod_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing required parameter: pod_id'})
                }
            result = manager.terminate_pod(pod_id)
            
        elif action == 'list-gpu-types':
            result = manager.list_gpu_types()
            
        elif action == 'find-gpu':
            pattern = params.get('pattern')
            if not pattern:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing required parameter: pattern'})
                }
            result = manager.find_gpu_by_name(pattern)
            if not result:
                return {
                    'statusCode': 404,
                    'body': json.dumps({'error': f"No GPU found matching '{pattern}'"})
                }
                
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f'Unknown action: {action}'})
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps(result, default=str)
        }
        
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
