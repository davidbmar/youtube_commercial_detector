#!/usr/bin/env python3
"""
RunPod Manager - Core functionality for managing RunPod resources
"""

import os
import json
import logging
import runpod
from typing import Dict, List, Any, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger("runpod-manager")

class RunPodManager:
    """Core class for managing RunPod resources"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the RunPod Manager
        
        Args:
            api_key: RunPod API key (optional, will use env var if not provided)
        """
        self.api_key = api_key or os.environ.get('RUNPOD_API_KEY')
        if not self.api_key:
            raise ValueError("RunPod API key not provided and RUNPOD_API_KEY environment variable not set")
        
        # Set the API key for the runpod library
        runpod.api_key = self.api_key
        logger.debug("RunPod API initialized")
    
    def list_pods(self) -> List[Dict[str, Any]]:
        """
        List all available pods
        
        Returns:
            List of pod information dictionaries
        """
        logger.info("Listing all pods")
        try:
            return runpod.get_pods()
        except Exception as e:
            logger.error(f"Error listing pods: {str(e)}")
            raise
    
    def get_pod(self, pod_id: str) -> Dict[str, Any]:
        """
        Get information about a specific pod
        
        Args:
            pod_id: ID of the pod to retrieve
            
        Returns:
            Pod information dictionary
        """
        logger.info(f"Getting information for pod {pod_id}")
        try:
            return runpod.get_pod(pod_id)
        except Exception as e:
            logger.error(f"Error getting pod {pod_id}: {str(e)}")
            raise
    
    def create_pod(self, 
                 name: str, 
                 image: str, 
                 gpu_type_id: str,
                 cloud_type: str = "COMMUNITY",
                 env_vars: Optional[Dict[str, str]] = None,
                 secrets: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Create a new pod
        
        Args:
            name: Name for the pod
            image: Docker image to use
            gpu_type_id: GPU type ID (get from list_gpu_types())
            cloud_type: Cloud type (COMMUNITY or SECURE)
            env_vars: Environment variables to set in the container
            secrets: List of secret names to use with the pod
            
        Returns:
            New pod information
        """
        logger.info(f"Creating pod '{name}' with image '{image}' on GPU '{gpu_type_id}'")
        
        # Set up parameters for pod creation
        pod_params = {
            "name": name,
            "image_name": image,
            "gpu_type_id": gpu_type_id,
        }
        
        # Add cloud type if specified
        if cloud_type:
            pod_params["cloud_type"] = cloud_type
        
        # Process environment variables
        final_env_vars = {}
        if env_vars:
            final_env_vars.update(env_vars)
            
        # Process secrets and add them to environment variables
        if secrets:
            for secret_name in secrets:
                # Use RunPod's secret syntax
                final_env_vars[secret_name] = f"{{{{ RUNPOD_SECRET_{secret_name} }}}}"
                
        # Add environment variables if we have any
        if final_env_vars:
            pod_params["env"] = final_env_vars
        
        try:
            return runpod.create_pod(**pod_params)
        except Exception as e:
            logger.error(f"Error creating pod: {str(e)}")
            raise
    
    def stop_pod(self, pod_id: str) -> Dict[str, Any]:
        """
        Stop a running pod
        
        Args:
            pod_id: ID of the pod to stop
            
        Returns:
            Response with pod status
        """
        logger.info(f"Stopping pod {pod_id}")
        try:
            return runpod.stop_pod(pod_id)
        except Exception as e:
            logger.error(f"Error stopping pod {pod_id}: {str(e)}")
            raise
    
    def start_pod(self, pod_id: str) -> Dict[str, Any]:
        """
        Start a stopped pod
        
        Args:
            pod_id: ID of the pod to start
            
        Returns:
            Response with pod status
        """
        logger.info(f"Starting pod {pod_id}")
        try:
            return runpod.start_pod(pod_id)
        except Exception as e:
            logger.error(f"Error starting pod {pod_id}: {str(e)}")
            raise
    
    def terminate_pod(self, pod_id: str) -> Dict[str, Any]:
        """
        Terminate (delete) a pod
        
        Args:
            pod_id: ID of the pod to terminate
            
        Returns:
            Response with termination status
        """
        logger.info(f"Terminating pod {pod_id}")
        try:
            return runpod.terminate_pod(pod_id)
        except Exception as e:
            logger.error(f"Error terminating pod {pod_id}: {str(e)}")
            raise
    
    def list_gpu_types(self) -> List[Dict[str, Any]]:
        """
        List all available GPU types
        
        Returns:
            List of available GPU types
        """
        logger.info("Listing available GPU types")
        try:
            # Try the modern API first
            try:
                return runpod.get_gpu_types()
            except AttributeError:
                # Fall back to older API
                return runpod.get_gpus()
        except Exception as e:
            logger.error(f"Error listing GPU types: {str(e)}")
            raise
    
    def list_secrets(self) -> List[str]:
        """
        List available secrets (if supported by RunPod API)
        
        Returns:
            List of secret names
        """
        logger.info("Listing available secrets")
        try:
            return runpod.get_secrets()
        except (AttributeError, NotImplementedError):
            logger.warning("Listing secrets not supported by RunPod API")
            return []
        except Exception as e:
            logger.error(f"Error listing secrets: {str(e)}")
            raise

    def find_gpu_by_name(self, name_pattern: str) -> Optional[Dict[str, Any]]:
        """
        Find a GPU by a substring in its name
        
        Args:
            name_pattern: Substring to search for in GPU names (case insensitive)
            
        Returns:
            GPU information or None if not found
        """
        logger.info(f"Searching for GPU with pattern '{name_pattern}'")
        try:
            gpu_types = self.list_gpu_types()
            
            # First try exact match on ID
            for gpu in gpu_types:
                if gpu.get('id', '') == name_pattern:
                    logger.info(f"Found exact match on ID: {name_pattern}")
                    return gpu
            
            # Then try exact match on displayName
            for gpu in gpu_types:
                display_name = gpu.get('displayName', '')
                if display_name and display_name.upper() == name_pattern.upper():
                    logger.info(f"Found exact match on display name: {display_name}")
                    return gpu
            
            # Finally try substring match
            for gpu in gpu_types:
                # Try different fields where the name might appear
                gpu_name = gpu.get('displayName', '') or gpu.get('name', '') or str(gpu.get('id', ''))
                if name_pattern.upper() in gpu_name.upper():
                    logger.info(f"Found partial match: {gpu_name} contains {name_pattern}")
                    return gpu
            
            logger.warning(f"No GPU found matching pattern '{name_pattern}'")
            return None
        except Exception as e:
            logger.error(f"Error finding GPU: {str(e)}")
            raise

# Helper function for JSON serialization
def serialize_for_json(obj):
    """Helper to serialize objects for JSON"""
    if hasattr(obj, '__dict__'):
        return obj.__dict__
    return str(obj)
