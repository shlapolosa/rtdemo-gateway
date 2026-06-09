"""
Shared Agent Factory for Microservices

Provides a unified factory pattern for creating agent microservices with FastAPI.
Eliminates code duplication across all agent implementations.
"""

import os
import logging
from typing import Dict, Any, List, Optional, Type
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AgentRequestModel(BaseModel):
    """Standard request model for all agents"""
    query: str = Field(..., description="The query or request text")
    parameters: Optional[Dict[str, Any]] = Field(default=None, description="Additional parameters")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Request metadata")


class AgentResponseModel(BaseModel):
    """Standard response model for all agents"""
    result: Any = Field(..., description="The agent's response result")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Response metadata")


def create_agent_app(agent_class: Type, service_name: str, endpoints: List[Dict[str, str]]) -> FastAPI:
    """
    Create a FastAPI application for an agent microservice
    
    Args:
        agent_class: The agent class to instantiate
        service_name: Name of the microservice
        endpoints: List of endpoint definitions with path, task_type, and description
    
    Returns:
        FastAPI application instance
    """
    
    # Global agent instance
    agent: Optional[Any] = None
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Manage application lifespan"""
        nonlocal agent
        
        # Startup
        logger.info(f"Starting {service_name} service...")
        
        try:
            agent = agent_class()
            await agent.initialize()
            logger.info(f"{agent_class.__name__} initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize agent: {e}")
            raise
        
        yield
        
        # Shutdown
        logger.info(f"Shutting down {service_name} service...")
        if agent:
            await agent.cleanup()
    
    # Initialize FastAPI app
    app = FastAPI(
        title=f"{service_name.replace('-', ' ').title()} Service",
        description=f"Microservice for {service_name}",
        version="1.0.0",
        lifespan=lifespan
    )
    
    async def get_agent():
        """Dependency to get the agent instance"""
        if agent is None:
            raise HTTPException(status_code=503, detail="Agent not initialized")
        return agent
    
    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        return {"status": "healthy", "service": service_name}
    
    # Create dynamic endpoints based on provided configuration
    for endpoint_config in endpoints:
        path = endpoint_config["path"]
        task_type = endpoint_config["task_type"]
        description = endpoint_config.get("description", f"Execute {task_type} task")
        
        # Create the endpoint handler
        def create_handler(task_type_val: str, description_val: str):
            async def handler(
                request: AgentRequestModel,
                agent_instance = Depends(get_agent)
            ) -> AgentResponseModel:
                try:
                    # Import AgentTask from the agent module
                    from dataclasses import dataclass, field
                    from typing import Dict, Any, Optional
                    
                    @dataclass
                    class AgentTask:
                        task_id: str
                        task_type: str
                        payload: Dict[str, Any]
                        metadata: Optional[Dict[str, Any]] = field(default_factory=dict)
                    
                    # Create task payload
                    task_payload = {
                        "query": request.query,
                        "parameters": request.parameters or {}
                    }
                    
                    # Merge parameters if they exist
                    if request.parameters:
                        task_payload.update(request.parameters)
                    
                    # Handle requirements parameter specifically
                    if request.parameters and "requirements" in request.parameters:
                        task_payload["requirements"] = request.parameters["requirements"]
                    elif hasattr(request, 'query') and request.query:
                        # Try to parse query as requirements if it looks like structured data
                        try:
                            import json
                            parsed_query = json.loads(request.query)
                            if isinstance(parsed_query, list):
                                task_payload["requirements"] = parsed_query
                        except (json.JSONDecodeError, TypeError):
                            # Not JSON, treat as regular query
                            pass
                    
                    # Create task for the agent
                    task = AgentTask(
                        task_id=f"{task_type_val}-{hash(str(task_payload))}",
                        task_type=task_type_val,
                        payload=task_payload,
                        metadata=request.metadata or {}
                    )
                    
                    # Process with agent
                    response = await agent_instance.process_task(task)
                    
                    if response.success:
                        return AgentResponseModel(
                            result=response.result,
                            metadata={
                                "agent_type": service_name.split('-')[0] if '-' in service_name else service_name,
                                "implementation": "anthropic" if "anthropic" in service_name else "deterministic",
                                "task_type": task_type_val,
                                "processing_time": response.metadata.get("processing_time", 0)
                            }
                        )
                    else:
                        raise HTTPException(
                            status_code=500,
                            detail=f"Agent processing failed: {response.error}"
                        )
                
                except Exception as e:
                    logger.error(f"Error in {task_type_val}: {e}")
                    raise HTTPException(status_code=500, detail=str(e))
            
            return handler
        
        # Add the endpoint to the app
        handler_func = create_handler(task_type, description)
        handler_func.__name__ = f"{task_type}_handler"
        
        app.post(
            path,
            response_model=AgentResponseModel,
            summary=description,
            description=description
        )(handler_func)
    
    return app