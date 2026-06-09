"""
FastAPI utilities for agent microservices
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse

from .base_agent import BaseAgent
from .config import AgentConfig, get_agent_config
from .models import AgentRequestModel, AgentResponseModel, AgentTask, HealthResponse, ErrorResponse


logger = logging.getLogger(__name__)


def create_agent_app(
    agent_class,
    title: Optional[str] = None,
    description: Optional[str] = None,
    version: str = "1.0.0"
) -> FastAPI:
    """
    Create a FastAPI application for an agent microservice
    
    Args:
        agent_class: The agent class to instantiate
        title: Optional title for the API
        description: Optional description for the API
        version: API version
        
    Returns:
        Configured FastAPI application
    """
    
    # Global agent instance
    agent: Optional[BaseAgent] = None
    config: Optional[AgentConfig] = None
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Manage application lifespan"""
        nonlocal agent, config
        
        # Startup
        logger.info("Starting agent microservice...")
        
        try:
            config = get_agent_config()
            agent = agent_class(config)
            await agent.initialize()
            logger.info(f"Agent {agent.name} initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize agent: {e}")
            raise
        
        yield
        
        # Shutdown
        logger.info("Shutting down agent microservice...")
        if agent:
            await agent.cleanup()
    
    # Create FastAPI app
    app = FastAPI(
        title=title or f"{config.service_name if config else 'Agent'} Service",
        description=description or "Agent microservice for architecture automation",
        version=version,
        lifespan=lifespan
    )
    
    async def get_agent() -> BaseAgent:
        """Dependency to get the agent instance"""
        if agent is None:
            raise HTTPException(status_code=503, detail="Agent not initialized")
        return agent
    
    async def get_config() -> AgentConfig:
        """Dependency to get the configuration"""
        if config is None:
            raise HTTPException(status_code=503, detail="Configuration not available")
        return config
    
    @app.exception_handler(Exception)
    async def global_exception_handler(request, exc):
        """Global exception handler"""
        logger.error(f"Unhandled exception: {exc}")
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error="Internal server error",
                detail=str(exc),
                status_code=500,
                timestamp=datetime.now().isoformat()
            ).dict()
        )
    
    @app.get("/health", response_model=HealthResponse)
    async def health_check(agent_instance: BaseAgent = Depends(get_agent)):
        """Health check endpoint"""
        health_data = await agent_instance.health_check()
        return HealthResponse(**health_data)
    
    @app.get("/capabilities")
    async def get_capabilities(agent_instance: BaseAgent = Depends(get_agent)):
        """Get agent capabilities"""
        return {
            "capabilities": [cap.value for cap in agent_instance.capabilities],
            "supported_task_types": agent_instance.get_supported_task_types(),
            "agent_type": agent_instance.config.agent_type.value,
            "implementation": agent_instance.config.implementation_type.value
        }
    
    def add_task_endpoint(endpoint_path: str, task_type: str):
        """
        Add a task processing endpoint
        
        Args:
            endpoint_path: The URL path for the endpoint
            task_type: The task type to process
        """
        @app.post(endpoint_path, response_model=AgentResponseModel)
        async def process_task_endpoint(
            request: AgentRequestModel,
            agent_instance: BaseAgent = Depends(get_agent)
        ) -> AgentResponseModel:
            try:
                # Create task
                task = AgentTask(
                    task_type=task_type,
                    payload={
                        "query": request.query,
                        **(request.parameters or {})
                    },
                    priority=request.priority or 1
                )
                
                # Process with agent
                response = await agent_instance.process_task(task)
                
                if response.success:
                    return AgentResponseModel(
                        result=response.result,
                        metadata={
                            **(response.metadata or {}),
                            "processing_time": response.processing_time
                        }
                    )
                else:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Agent processing failed: {response.error}"
                    )
            
            except Exception as e:
                logger.error(f"Error in {endpoint_path}: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        # Update function name for OpenAPI
        process_task_endpoint.__name__ = f"process_{task_type.replace('-', '_')}"
        
        return process_task_endpoint
    
    # Store the add_task_endpoint function on the app for use by agent implementations
    app.add_task_endpoint = add_task_endpoint
    
    return app


def setup_cors(app: FastAPI):
    """Setup CORS for the FastAPI app"""
    from fastapi.middleware.cors import CORSMiddleware
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )