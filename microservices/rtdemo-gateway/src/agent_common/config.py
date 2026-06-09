"""
Configuration management for agent microservices
"""

import os
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from .models import AgentType, ImplementationType


@dataclass
class AgentConfig:
    """Configuration for agent microservices"""
    agent_type: AgentType
    implementation_type: ImplementationType
    service_name: str
    log_level: str = "INFO"
    port: int = 8080
    host: str = "0.0.0.0"
    
    # External service configurations
    anthropic_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    redis_host: Optional[str] = None
    redis_port: int = 6379
    
    # Performance configurations
    max_concurrent_tasks: int = 10
    task_timeout: int = 300  # seconds
    
    # Real-time platform configurations
    realtime_platform: Optional[str] = None
    websocket_enabled: bool = False
    
    # Streaming configurations
    kafka_bootstrap_servers: Optional[str] = None
    kafka_schema_registry_url: Optional[str] = None
    streaming_topics: List[str] = field(default_factory=list)
    streaming_consumer_group: Optional[str] = None
    
    # MQTT configurations  
    mqtt_host: Optional[str] = None
    mqtt_port: int = 1883
    mqtt_user: Optional[str] = None
    mqtt_password: Optional[str] = None
    mqtt_topics: List[str] = field(default_factory=list)
    
    # Database configurations (from realtime platform)
    db_host: Optional[str] = None
    db_port: int = 5432
    db_name: Optional[str] = None
    db_user: Optional[str] = None
    db_password: Optional[str] = None
    
    # Analytics configurations
    metabase_url: Optional[str] = None
    metabase_user: Optional[str] = None
    metabase_password: Optional[str] = None
    
    # Stream processing configurations
    lenses_url: Optional[str] = None
    lenses_user: Optional[str] = None
    lenses_password: Optional[str] = None
    
    # Custom configurations
    custom_config: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Configure logging after initialization"""
        logging.basicConfig(
            level=getattr(logging, self.log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )


def get_agent_config() -> AgentConfig:
    """Get agent configuration from environment variables"""
    
    # Required environment variables
    agent_type_str = os.getenv("AGENT_TYPE")
    if not agent_type_str:
        raise ValueError("AGENT_TYPE environment variable is required")
    
    implementation_type_str = os.getenv("IMPLEMENTATION_TYPE")
    if not implementation_type_str:
        raise ValueError("IMPLEMENTATION_TYPE environment variable is required")
    
    # Parse agent type
    try:
        agent_type = AgentType(agent_type_str)
    except ValueError:
        raise ValueError(f"Invalid AGENT_TYPE: {agent_type_str}")
    
    # Parse implementation type
    try:
        implementation_type = ImplementationType(implementation_type_str)
    except ValueError:
        raise ValueError(f"Invalid IMPLEMENTATION_TYPE: {implementation_type_str}")
    
    # Generate service name
    service_name = f"{agent_type.value}-{implementation_type.value}"
    
    return AgentConfig(
        agent_type=agent_type,
        implementation_type=implementation_type,
        service_name=service_name,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        port=int(os.getenv("PORT", "8080")),
        host=os.getenv("HOST", "0.0.0.0"),
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        redis_host=os.getenv("REDIS_HOST"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        max_concurrent_tasks=int(os.getenv("MAX_CONCURRENT_TASKS", "10")),
        task_timeout=int(os.getenv("TASK_TIMEOUT", "300")),
        
        # Real-time platform configurations
        realtime_platform=os.getenv("REALTIME_PLATFORM"),
        websocket_enabled=os.getenv("WEBSOCKET_ENABLED", "false").lower() == "true",
        
        # Streaming configurations
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        kafka_schema_registry_url=os.getenv("KAFKA_SCHEMA_REGISTRY_URL"),
        streaming_topics=os.getenv("STREAMING_TOPICS", "").split(",") if os.getenv("STREAMING_TOPICS") else [],
        streaming_consumer_group=os.getenv("STREAMING_CONSUMER_GROUP"),
        
        # MQTT configurations
        mqtt_host=os.getenv("MQTT_HOST"),
        mqtt_port=int(os.getenv("MQTT_PORT", "1883")),
        mqtt_user=os.getenv("MQTT_USER"),
        mqtt_password=os.getenv("MQTT_PASSWORD"),
        mqtt_topics=os.getenv("MQTT_TOPICS", "").split(",") if os.getenv("MQTT_TOPICS") else [],
        
        # Database configurations (from realtime platform)
        db_host=os.getenv("DB_HOST"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        
        # Analytics configurations
        metabase_url=os.getenv("METABASE_URL"),
        metabase_user=os.getenv("METABASE_USER"),
        metabase_password=os.getenv("METABASE_PASSWORD"),
        
        # Stream processing configurations
        lenses_url=os.getenv("LENSES_URL"),
        lenses_user=os.getenv("LENSES_USER"),
        lenses_password=os.getenv("LENSES_PASSWORD")
    )