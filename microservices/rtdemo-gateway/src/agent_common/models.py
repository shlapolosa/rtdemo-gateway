"""
Shared data models for agent microservices
"""

import uuid
from enum import Enum
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
from pydantic import BaseModel, Field


class AgentType(Enum):
    """Types of agents in the system"""
    BUSINESS_ANALYST = "business-analyst"
    BUSINESS_ARCHITECT = "business-architect"
    APPLICATION_ARCHITECT = "application-architect"
    INFRASTRUCTURE_ARCHITECT = "infrastructure-architect"
    SOLUTION_ARCHITECT = "solution-architect"
    DEVELOPER = "developer"
    PROJECT_MANAGER = "project-manager"
    ACCOUNTANT = "accountant"
    ORCHESTRATOR = "orchestrator"


class AgentCapability(Enum):
    """Agent capabilities"""
    REQUIREMENT_ANALYSIS = "requirement-analysis"
    BUSINESS_ARCHITECTURE = "business-architecture"
    APPLICATION_ARCHITECTURE = "application-architecture"
    INFRASTRUCTURE_ARCHITECTURE = "infrastructure-architecture"
    SOLUTION_ARCHITECTURE = "solution-architecture"
    CODE_GENERATION = "code-generation"
    PROJECT_MANAGEMENT = "project-management"
    COST_ANALYSIS = "cost-analysis"
    DOCUMENTATION = "documentation"
    VALIDATION = "validation"
    ORCHESTRATION = "orchestration"


class ImplementationType(Enum):
    """Implementation types for agents"""
    DETERMINISTIC = "deterministic"
    ANTHROPIC = "anthropic"
    OPENAI = "openai"


@dataclass
class AgentTask:
    """Task for agent processing"""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    metadata: Optional[Dict[str, Any]] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    priority: int = 1  # 1=low, 2=medium, 3=high


@dataclass
class AgentResponse:
    """Response from agent processing"""
    success: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = field(default_factory=dict)
    processing_time: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)


class AgentRequestModel(BaseModel):
    """Base request model for agent operations"""
    query: str = Field(..., description="The query to process")
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional parameters for the request"
    )
    context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional context for the request"
    )
    task_type: Optional[str] = Field(
        default=None,
        description="Specific task type to execute"
    )
    priority: Optional[int] = Field(
        default=1,
        description="Task priority (1=low, 2=medium, 3=high)"
    )


class AgentResponseModel(BaseModel):
    """Base response model for agent operations"""
    result: Any = Field(..., description="The result of the operation")
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional metadata about the operation"
    )
    processing_time: Optional[float] = Field(
        default=None,
        description="Time taken to process the request in seconds"
    )


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    service: str
    agent_type: str
    implementation: str
    timestamp: str
    capabilities: List[str] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    """Error response model"""
    error: str
    detail: Optional[str] = None
    status_code: int
    timestamp: str


# ==============================================================================
# Real-time Data Models
# ==============================================================================

class EventType(Enum):
    """Types of real-time events"""
    AGENT_TASK_STARTED = "agent_task_started"
    AGENT_TASK_COMPLETED = "agent_task_completed"
    AGENT_TASK_FAILED = "agent_task_failed"
    DATA_PROCESSED = "data_processed"
    ALERT_TRIGGERED = "alert_triggered"
    STATUS_UPDATE = "status_update"
    SYSTEM_EVENT = "system_event"
    CUSTOM_EVENT = "custom_event"


class StreamingDataType(Enum):
    """Types of streaming data"""
    HEALTH_DATA = "health_data"
    DEVICE_DATA = "device_data"
    SENSOR_DATA = "sensor_data"
    AGENT_COMMUNICATION = "agent_communication"
    SYSTEM_METRICS = "system_metrics"
    USER_INTERACTION = "user_interaction"
    BUSINESS_EVENT = "business_event"


@dataclass
class RealtimeEvent:
    """Real-time event data structure"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType = EventType.CUSTOM_EVENT
    source_service: str = ""
    source_agent: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    correlation_id: Optional[str] = None
    stream_id: Optional[str] = None
    priority: int = 1  # 1=low, 2=medium, 3=high


class RealtimeEventModel(BaseModel):
    """Pydantic model for real-time events"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    source_service: str
    source_agent: Optional[str] = None
    data: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.now)
    correlation_id: Optional[str] = None
    stream_id: Optional[str] = None
    priority: int = Field(default=1, ge=1, le=3)


class StreamingDataModel(BaseModel):
    """Base model for streaming data"""
    data_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    data_type: StreamingDataType
    source: str
    payload: Any
    timestamp: datetime = Field(default_factory=datetime.now)
    schema_version: str = "1.0"
    metadata: Optional[Dict[str, Any]] = None


class WebSocketMessage(BaseModel):
    """Model for WebSocket messages"""
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    message_type: str
    payload: Any
    timestamp: datetime = Field(default_factory=datetime.now)
    correlation_id: Optional[str] = None
    response_to: Optional[str] = None


class StreamingResponseModel(BaseModel):
    """Response model for streaming endpoints"""
    stream_id: str
    event: RealtimeEventModel
    metadata: Optional[Dict[str, Any]] = None
    next_offset: Optional[str] = None


class KafkaMessageModel(BaseModel):
    """Model for Kafka messages"""
    topic: str
    key: Optional[str] = None
    value: Any
    headers: Optional[Dict[str, str]] = None
    timestamp: datetime = Field(default_factory=datetime.now)
    partition: Optional[int] = None
    offset: Optional[int] = None


class MQTTMessageModel(BaseModel):
    """Model for MQTT messages"""
    topic: str
    payload: Any
    qos: int = Field(default=1, ge=0, le=2)
    retain: bool = False
    timestamp: datetime = Field(default_factory=datetime.now)
    message_id: Optional[str] = None


class ConnectionStatus(Enum):
    """Connection status for real-time services"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


class RealtimeConnectionInfo(BaseModel):
    """Information about real-time connections"""
    service_name: str
    connection_type: str  # kafka, mqtt, websocket, redis
    status: ConnectionStatus
    endpoint: str
    connected_at: Optional[datetime] = None
    last_activity: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0


class AgentRealtimeStatus(BaseModel):
    """Real-time status for an agent"""
    agent_type: AgentType
    implementation_type: ImplementationType
    service_name: str
    realtime_enabled: bool
    websocket_enabled: bool
    connections: List[RealtimeConnectionInfo] = Field(default_factory=list)
    active_streams: List[str] = Field(default_factory=list)
    message_count: int = 0
    error_count: int = 0
    last_activity: Optional[datetime] = None


class HealthDataModel(BaseModel):
    """Model for health-related streaming data"""
    device_id: str
    patient_id: Optional[str] = None
    reading_type: str  # blood_pressure, heart_rate, temperature, etc.
    value: Union[float, Dict[str, float]]  # Single value or multiple readings
    unit: str
    timestamp: datetime = Field(default_factory=datetime.now)
    location: Optional[Dict[str, float]] = None  # {"latitude": x, "longitude": y}
    device_metadata: Optional[Dict[str, Any]] = None