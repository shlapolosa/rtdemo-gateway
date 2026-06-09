"""
Agent Common Library

Shared base classes and utilities for all agent microservices.
Now includes real-time streaming capabilities with WebSocket, Kafka, and MQTT support.
"""

# Core agent functionality
from .base_agent import BaseAgent, AgentType, AgentCapability
from .base_microservice_agent import BaseMicroserviceAgent
from .models import (
    AgentTask, AgentResponse, AgentRequestModel, AgentResponseModel,
    # Real-time models
    RealtimeEvent, EventType, StreamingDataType, WebSocketMessage,
    ConnectionStatus, RealtimeConnectionInfo, AgentRealtimeStatus,
    KafkaMessageModel, MQTTMessageModel, HealthDataModel
)
from .config import AgentConfig, get_agent_config

# Real-time functionality
from .realtime_agent import RealtimeAgent
from .secret_loader import PlatformSecretLoader, load_realtime_platform_secrets, configure_agent_from_secrets
from .websocket_manager import WebSocketConnectionManager, websocket_manager

# FastAPI factories
from .shared_agent_factory import create_agent_app
from .realtime_fastapi import create_realtime_agent_app

__version__ = "1.1.0"
__all__ = [
    # Core functionality
    "BaseAgent",
    "BaseMicroserviceAgent", 
    "AgentType", 
    "AgentCapability",
    "AgentTask",
    "AgentResponse", 
    "AgentRequestModel",
    "AgentResponseModel",
    "AgentConfig",
    "get_agent_config",
    "create_agent_app",
    
    # Real-time functionality
    "RealtimeAgent",
    "RealtimeEvent",
    "EventType",
    "StreamingDataType",
    "WebSocketMessage",
    "ConnectionStatus",
    "RealtimeConnectionInfo", 
    "AgentRealtimeStatus",
    "KafkaMessageModel",
    "MQTTMessageModel",
    "HealthDataModel",
    
    # Real-time utilities
    "PlatformSecretLoader",
    "load_realtime_platform_secrets",
    "configure_agent_from_secrets",
    "WebSocketConnectionManager",
    "websocket_manager",
    "create_realtime_agent_app"
]