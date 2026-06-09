"""
Base FastAPI application for microservice agents
"""

import os
import logging
from typing import Dict, Any, List, Optional, Type
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field
import httpx

from .models import AgentRequestModel, AgentResponseModel
from .base_microservice_agent import BaseMicroserviceAgent, AgentTask

logger = logging.getLogger(__name__)


def create_agent_app(
    agent_class: Type[BaseMicroserviceAgent],
    service_name: str,
    description: str,
    endpoints: List[Dict[str, str]]
) -> FastAPI:
    """
    Create a FastAPI app for an agent microservice
    
    Args:
        agent_class: The agent class to instantiate
        service_name: Name of the service (e.g., "business-analyst-anthropic")
        description: Service description
        endpoints: List of endpoint configs with 'path', 'task_type', 'method'
    """
    
    # Global agent instance
    agent: Optional[BaseMicroserviceAgent] = None
    
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
        title=f"{agent_class.__name__} Service",
        description=description,
        version="1.0.0",
        lifespan=lifespan
    )
    
    async def get_agent() -> BaseMicroserviceAgent:
        """Dependency to get the agent instance"""
        if agent is None:
            raise HTTPException(status_code=503, detail="Agent not initialized")
        return agent
    
    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        return {"status": "healthy", "service": service_name}
    
    # Add dynamic endpoints
    for endpoint_config in endpoints:
        path = endpoint_config["path"]
        task_type = endpoint_config["task_type"]
        method = endpoint_config.get("method", "POST")
        
        def create_endpoint_handler(task_type_param: str):
            async def endpoint_handler(
                request: AgentRequestModel,
                agent_instance: BaseMicroserviceAgent = Depends(get_agent)
            ) -> AgentResponseModel:
                try:
                    # Create task for the agent
                    task_payload = {
                        "query": request.query,
                        "parameters": request.parameters or {},
                        "context": request.context or {}
                    }
                    
                    # Process with agent
                    task = AgentTask(
                        task_id=f"{task_type_param}-{hash(str(task_payload))}",
                        task_type=task_type_param,
                        payload=task_payload
                    )
                    
                    response = await agent_instance.process_task(task)
                    
                    if response.success:
                        return AgentResponseModel(
                            result=response.result,
                            metadata={
                                "agent_type": agent_instance.agent_type,
                                "implementation": service_name.split("-")[-1],
                                "task_type": task_type_param,
                                "processing_time": response.metadata.get("processing_time", 0)
                            }
                        )
                    else:
                        raise HTTPException(
                            status_code=500,
                            detail=f"Agent processing failed: {response.error}"
                        )
                
                except Exception as e:
                    logger.error(f"Error in {task_type_param}: {e}")
                    raise HTTPException(status_code=500, detail=str(e))
            
            return endpoint_handler
        
        # Add endpoint to app
        handler = create_endpoint_handler(task_type)
        app.add_api_route(
            path,
            handler,
            methods=[method],
            response_model=AgentResponseModel,
            name=task_type.replace("_", "-")
        )
    
    # Main entry point for development
    @app.get("/")
    async def root():
        return {"service": service_name, "status": "running"}
    
    return app