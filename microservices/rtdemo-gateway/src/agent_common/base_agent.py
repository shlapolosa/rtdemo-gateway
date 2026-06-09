"""
Base agent class for all microservices
"""

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, List, Optional

from .models import AgentType, AgentCapability, AgentTask, AgentResponse, ImplementationType
from .config import AgentConfig


logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """
    Abstract base class for all agent microservices
    
    Provides common functionality for task processing, health checks,
    and lifecycle management.
    """
    
    def __init__(self, config: AgentConfig):
        self.config = config
        self.agent_id = str(uuid.uuid4())
        self.name = self._get_agent_name()
        self.description = self._get_agent_description()
        self.capabilities = self._get_agent_capabilities()
        self.is_initialized = False
        self.is_healthy = True
        
        logger.info(f"Agent {self.name} ({self.agent_id}) created")
    
    @abstractmethod
    def _get_agent_name(self) -> str:
        """Get the human-readable name of the agent"""
        pass
    
    @abstractmethod 
    def _get_agent_description(self) -> str:
        """Get the description of the agent"""
        pass
    
    @abstractmethod
    def _get_agent_capabilities(self) -> List[AgentCapability]:
        """Get the list of capabilities this agent supports"""
        pass
    
    @abstractmethod
    async def _process_task_internal(self, task: AgentTask) -> Any:
        """Process task specific to the agent implementation"""
        pass
    
    async def initialize(self):
        """Initialize the agent and its dependencies"""
        try:
            await self._initialize_dependencies()
            self.is_initialized = True
            self.is_healthy = True
            logger.info(f"Agent {self.name} ({self.agent_id}) initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize agent {self.name}: {e}")
            self.is_healthy = False
            raise
    
    async def _initialize_dependencies(self):
        """Initialize agent-specific dependencies. Override in subclasses."""
        pass
    
    async def cleanup(self):
        """Cleanup agent resources"""
        try:
            await self._cleanup_dependencies()
            self.is_initialized = False
            logger.info(f"Agent {self.name} ({self.agent_id}) cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup for agent {self.name}: {e}")
    
    async def _cleanup_dependencies(self):
        """Cleanup agent-specific dependencies. Override in subclasses."""
        pass
    
    async def process_task(self, task: AgentTask) -> AgentResponse:
        """
        Process a task and return the response
        
        Args:
            task: The task to process
            
        Returns:
            AgentResponse with result or error
        """
        if not self.is_initialized:
            return AgentResponse(
                success=False,
                error="Agent not initialized",
                metadata={"task_id": task.task_id}
            )
        
        if not self.is_healthy:
            return AgentResponse(
                success=False,
                error="Agent not healthy",
                metadata={"task_id": task.task_id}
            )
        
        start_time = datetime.now()
        
        try:
            # Validate task
            if not self._validate_task(task):
                return AgentResponse(
                    success=False,
                    error=f"Invalid task type: {task.task_type}",
                    metadata={"task_id": task.task_id}
                )
            
            # Process the task
            result = await asyncio.wait_for(
                self._process_task_internal(task),
                timeout=self.config.task_timeout
            )
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return AgentResponse(
                success=True,
                result=result,
                processing_time=processing_time,
                metadata={
                    "task_id": task.task_id,
                    "agent_id": self.agent_id,
                    "agent_type": self.config.agent_type.value,
                    "implementation": self.config.implementation_type.value
                }
            )
            
        except asyncio.TimeoutError:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Task {task.task_id} timed out after {processing_time}s")
            return AgentResponse(
                success=False,
                error=f"Task timed out after {self.config.task_timeout}s",
                processing_time=processing_time,
                metadata={"task_id": task.task_id}
            )
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error processing task {task.task_id}: {e}")
            return AgentResponse(
                success=False,
                error=str(e),
                processing_time=processing_time,
                metadata={"task_id": task.task_id}
            )
    
    def _validate_task(self, task: AgentTask) -> bool:
        """
        Validate if the task can be processed by this agent
        Override in subclasses for specific validation logic
        """
        return task.task_type is not None
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check and return status
        
        Returns:
            Dictionary with health status information
        """
        try:
            # Perform any agent-specific health checks
            await self._perform_health_checks()
            
            return {
                "status": "healthy" if self.is_healthy else "unhealthy",
                "service": self.config.service_name,
                "agent_type": self.config.agent_type.value,
                "implementation": self.config.implementation_type.value,
                "agent_id": self.agent_id,
                "initialized": self.is_initialized,
                "capabilities": [cap.value for cap in self.capabilities],
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Health check failed for agent {self.name}: {e}")
            self.is_healthy = False
            return {
                "status": "unhealthy",
                "service": self.config.service_name,
                "agent_type": self.config.agent_type.value,
                "implementation": self.config.implementation_type.value,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def _perform_health_checks(self):
        """
        Perform agent-specific health checks
        Override in subclasses for custom health check logic
        """
        pass
    
    def get_supported_task_types(self) -> List[str]:
        """Get list of supported task types for this agent"""
        # Default implementation - override in subclasses
        return []
    
    def __str__(self) -> str:
        return f"{self.name} ({self.config.agent_type.value}-{self.config.implementation_type.value})"
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(id={self.agent_id}, type={self.config.agent_type.value})>"


class DeterministicAgent(BaseAgent):
    """
    Base class for deterministic (rule-based) agents
    """
    
    def __init__(self, config: AgentConfig):
        if config.implementation_type != ImplementationType.DETERMINISTIC:
            raise ValueError("DeterministicAgent requires DETERMINISTIC implementation type")
        super().__init__(config)


class LLMAgent(BaseAgent):
    """
    Base class for LLM-powered agents (Anthropic, OpenAI, etc.)
    """
    
    def __init__(self, config: AgentConfig):
        if config.implementation_type == ImplementationType.DETERMINISTIC:
            raise ValueError("LLMAgent cannot use DETERMINISTIC implementation type")
        super().__init__(config)
        self.api_key = self._get_api_key()
    
    def _get_api_key(self) -> Optional[str]:
        """Get the appropriate API key based on implementation type"""
        if self.config.implementation_type == ImplementationType.ANTHROPIC:
            return self.config.anthropic_api_key
        elif self.config.implementation_type == ImplementationType.OPENAI:
            return self.config.openai_api_key
        return None
    
    async def _initialize_dependencies(self):
        """Initialize LLM client"""
        if not self.api_key:
            logger.warning(f"No API key provided for {self.config.implementation_type.value} agent")
            # Don't raise error - allow agent to work in test mode
        await super()._initialize_dependencies()
    
    async def _perform_health_checks(self):
        """Check LLM service connectivity"""
        await super()._perform_health_checks()
        # Add LLM-specific health checks here
        if not self.api_key:
            logger.warning("No API key available for LLM service")