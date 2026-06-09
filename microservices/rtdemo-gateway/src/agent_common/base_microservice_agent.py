"""
Base Microservice Agent with common functionality
"""

import asyncio
import logging
import uuid
from typing import Dict, List, Any, Optional, Type
from datetime import datetime
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


@dataclass
class AgentTask:
    """Simple task representation"""
    task_id: str
    task_type: str
    payload: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = field(default_factory=dict)


@dataclass
class AgentResponse:
    """Simple response representation"""
    success: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = field(default_factory=dict)


class BaseProcessor(ABC):
    """Base processor class for all agents"""
    
    def __init__(self):
        self.knowledge_base = self._initialize_knowledge_base()
        self.templates = self._initialize_templates()
    
    @abstractmethod
    def _initialize_knowledge_base(self) -> Dict[str, Any]:
        """Initialize domain-specific knowledge base"""
        pass
    
    @abstractmethod
    def _initialize_templates(self) -> Dict[str, str]:
        """Initialize output templates"""
        pass


class BaseMicroserviceAgent(ABC):
    """
    Base class for all microservice agents with common functionality
    """
    
    def __init__(self, agent_type: str, agent_name: str, description: str):
        self.agent_id = str(uuid.uuid4())
        self.agent_type = agent_type
        self.name = agent_name
        self.description = description
        
        # Initialize processor
        self.processor = self._create_processor()
        
        logger.info(f"{self.name} Agent {self.agent_id} initialized")
    
    @abstractmethod
    def _create_processor(self) -> BaseProcessor:
        """Create the agent-specific processor"""
        pass
    
    @abstractmethod
    def _get_supported_task_types(self) -> List[str]:
        """Get list of supported task types for this agent"""
        pass
    
    async def initialize(self):
        """Initialize agent and dependencies"""
        logger.info(f"{self.name} Agent {self.agent_id} fully initialized")
    
    async def cleanup(self):
        """Cleanup agent resources"""
        logger.info(f"{self.name} Agent {self.agent_id} cleanup completed")
    
    async def process_task(self, task: AgentTask) -> AgentResponse:
        """Process task - common logic for all agents"""
        try:
            task_type = task.task_type
            supported_types = self._get_supported_task_types()
            
            if task_type not in supported_types:
                raise ValueError(f"Unknown task type: {task_type}. Supported: {supported_types}")
            
            # Call agent-specific method
            method_name = f"_{task_type}"
            if hasattr(self, method_name):
                result = await getattr(self, method_name)(task.payload)
            else:
                raise ValueError(f"Method {method_name} not implemented")
            
            return AgentResponse(
                success=True,
                result=result,
                metadata={"task_id": task.task_id, "processing_time": 0.1}
            )
        
        except Exception as e:
            logger.error(f"Error processing task {task.task_id}: {e}")
            return AgentResponse(
                success=False,
                error=str(e),
                metadata={"task_id": task.task_id}
            )