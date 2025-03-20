#!/usr/bin/env python3
"""
RunPod CLI - Command line interface for RunPod management
"""

import os
import sys
import json
import argparse
from dotenv import load_dotenv
from runpod_manager import RunPodManager, serialize_for_json

def pretty_print(data):
    """Print data in a formatted JSON structure"""
    print(json.dumps(data, indent=2, default=serialize_for_json))

def main():
    """Main CLI entry point"""
    # Load environment variables from .env file
    load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="RunPod Management CLI")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Command to execute")
    
    # Global arguments
    parser.add_argument("--api-key", help="RunPod API key (defaults to RUNPOD_API_KEY env var)")
    parser.add_argument("--json", action="store_true", help="Output in JSON format")
    
    # List pods command
    subparsers.add_parser("list-pods", help="List all pods")
    
    # Get pod command
    get_pod_parser = subparsers.add_parser("get-pod", help="Get pod details")
    get_pod_parser.add_argument("pod_id", help="ID of the pod")
    
    # Create pod command
    create_pod_parser = subparsers.add_parser("create-pod", help="Create a new pod")
    create_pod_parser.add_argument("--name", required=True, help="Name for the pod")
    create_pod_parser.add_argument("--image", required=True, help="Docker image to use")
    create_pod_parser.add_argument("--gpu-type", required=True, help="GPU type (can be ID or search pattern)")
    create_pod_parser.add_argument("--cloud-type", default="COMMUNITY", choices=["COMMUNITY", "SECURE"], 
                                  help="Cloud type (COMMUNITY or SECURE)")
    create_pod_parser.add_argument("--env", action="append", help="Environment variables in KEY=VALUE format")
    create_pod_parser.add_argument("--secret", action="append", help="Secret names to use (will be used to create env vars)")
    create_pod_parser.add_argument("--use-aws-secrets", action="store_true", 
                                 help="Use AWS credentials from secrets (aws_access_key_id, aws_secret_access_key, aws_region)")
    
    # Start pod command
    start_pod_parser = subparsers.add_parser("start-pod", help="Start a stopped pod")
    start_pod_parser.add_argument("pod_id", help="ID of the pod to start")
    
    # Stop pod command
    stop_pod_parser = subparsers.add_parser("stop-pod", help="Stop a running pod")
    stop_pod_parser.add_argument("pod_id", help="ID of the pod to stop")
    
    # Terminate pod command
    terminate_pod_parser = subparsers.add_parser("terminate-pod", help="Terminate (delete) a pod")
    terminate_pod_parser.add_argument("pod_id", help="ID of the pod to terminate")
    
    # List GPU types command
    list_gpu_parser = subparsers.add_parser("list-gpu-types", help="List available GPU types")
    
    # Find GPU command
    find_gpu_parser = subparsers.add_parser("find-gpu", help="Find GPU by name pattern")
    find_gpu_parser.add_argument("pattern", help="Name pattern to search for")
    
    # Parse arguments
    args = parser.parse_args()
    
    try:
        # Initialize RunPod manager
        manager = RunPodManager(api_key=args.api_key)
        
        # Process commands
        if args.command == "list-pods":
            result = manager.list_pods()
            if not args.json:
                print(f"Found {len(result)} pods:")
                for pod in result:
                    status = pod.get('desiredStatus', 'unknown')
                    print(f"  {pod['id']} - {pod.get('name', 'unnamed')} ({status})")
            else:
                pretty_print(result)
                
        elif args.command == "get-pod":
            result = manager.get_pod(args.pod_id)
            if not args.json:
                print(f"Pod details for {args.pod_id}:")
                print(f"  Name: {result.get('name', 'unnamed')}")
                print(f"  Status: {result.get('desiredStatus', 'unknown')}")
                print(f"  Image: {result.get('imageName', 'unknown')}")
                if result.get('gpus'):
                    print(f"  GPUs: {', '.join([g.get('name', 'unknown') for g in result.get('gpus', [])])}")
                print(f"  Cost: ${result.get('costPerHr', 0)} per hour")
            else:
                pretty_print(result)
                
        elif args.command == "create-pod":
            # Process environment variables
            env_vars = {}
            if args.env:
                for env_var in args.env:
                    key, value = env_var.split('=', 1)
                    env_vars[key] = value
            
            # Process secrets
            secrets = []
            if args.secret:
                secrets.extend(args.secret)
                
            # Add AWS secrets if requested
            if args.use_aws_secrets:
                secrets.extend(['aws_access_key_id', 'aws_secret_access_key', 'aws_region'])
                # Map these to the standard AWS environment variable names
                env_vars['AWS_ACCESS_KEY_ID'] = "{{ RUNPOD_SECRET_aws_access_key_id }}"
                env_vars['AWS_SECRET_ACCESS_KEY'] = "{{ RUNPOD_SECRET_aws_secret_access_key }}"
                env_vars['AWS_DEFAULT_REGION'] = "{{ RUNPOD_SECRET_aws_region }}"
            
            # Check if we need to look up the GPU type ID
            gpu_type_id = args.gpu_type
            # Always try to find the GPU by name unless it's clearly already an ID
            if not gpu_type_id.startswith("gpu-"):  # Actual IDs often start with "gpu-"
                # Try to find the GPU by name
                gpu = manager.find_gpu_by_name(args.gpu_type)
                if gpu:
                    gpu_type_id = gpu['id']
                    if not args.json:
                        print(f"Found GPU: {gpu.get('displayName', gpu['id'])}")
                        print(f"Using GPU ID: {gpu_type_id}")
                else:
                    print(f"Error: No GPU found matching '{args.gpu_type}'")
                    print("Available GPUs:")
                    for gpu in manager.list_gpu_types():
                        print(f"  {gpu.get('displayName', 'unnamed')} (ID: {gpu['id']})")
                    return 1
            
            # Create the pod
            result = manager.create_pod(
                name=args.name,
                image=args.image,
                gpu_type_id=gpu_type_id,
                cloud_type=args.cloud_type,
                env_vars=env_vars,
                secrets=secrets
            )
            
            if not args.json:
                print(f"Pod created successfully:")
                print(f"  ID: {result['id']}")
                print(f"  Status: {result.get('desiredStatus', 'unknown')}")
            else:
                pretty_print(result)
                
        elif args.command == "start-pod":
            result = manager.start_pod(args.pod_id)
            if not args.json:
                print(f"Pod {args.pod_id} start initiated:")
                print(f"  Status: {result.get('desiredStatus', 'unknown')}")
            else:
                pretty_print(result)
                
        elif args.command == "stop-pod":
            result = manager.stop_pod(args.pod_id)
            if not args.json:
                print(f"Pod {args.pod_id} stop initiated:")
                print(f"  Status: {result.get('desiredStatus', 'unknown')}")
            else:
                pretty_print(result)
                
        elif args.command == "terminate-pod":
            result = manager.terminate_pod(args.pod_id)
            if not args.json:
                print(f"Pod {args.pod_id} termination initiated!")
            else:
                pretty_print(result)
                
        elif args.command == "list-gpu-types":
            result = manager.list_gpu_types()
            if not args.json:
                print(f"Available GPU types:")
                for gpu in result:
                    memory = f"{gpu.get('memoryInGb', '?')} GB" if 'memoryInGb' in gpu else ''
                    print(f"  {gpu.get('displayName', gpu['id'])} {memory}")
                    print(f"    ID: {gpu['id']}")
            else:
                pretty_print(result)
                
        elif args.command == "find-gpu":
            result = manager.find_gpu_by_name(args.pattern)
            if result:
                if not args.json:
                    print(f"Found GPU matching '{args.pattern}':")
                    print(f"  Name: {result.get('displayName', result['id'])}")
                    print(f"  ID: {result['id']}")
                    if 'memoryInGb' in result:
                        print(f"  Memory: {result['memoryInGb']} GB")
                else:
                    pretty_print(result)
            else:
                print(f"No GPU found matching '{args.pattern}'")
                return 1
        
        return 0
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
