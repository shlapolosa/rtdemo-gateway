"""
Real-time enabled Agent base class with streaming capabilities
"""

import asyncio
import logging
import json
from typing import Dict, List, Any, Optional, Set, Callable
from datetime import datetime

from .base_microservice_agent import BaseMicroserviceAgent, AgentTask, AgentResponse
from .config import AgentConfig
from .models import (
    RealtimeEvent, EventType, ConnectionStatus, RealtimeConnectionInfo,
    AgentRealtimeStatus, KafkaMessageModel, MQTTMessageModel, WebSocketMessage
)

logger = logging.getLogger(__name__)


class RealtimeAgent(BaseMicroserviceAgent):
    """
    Real-time enabled agent with streaming capabilities.
    Extends BaseMicroserviceAgent with WebSocket, Kafka, and MQTT support.
    """
    
    def __init__(self, agent_type: str, agent_name: str, description: str, config: AgentConfig):
        super().__init__(agent_type, agent_name, description)
        self.config = config
        
        # Real-time connection clients
        self.kafka_producer = None
        self.kafka_consumer = None
        self.mqtt_client = None
        self.redis_client = None
        
        # WebSocket connections
        self.websocket_connections: Set = set()
        
        # Connection status tracking
        self.connections: Dict[str, RealtimeConnectionInfo] = {}
        
        # Event handlers
        self.event_handlers: Dict[EventType, List[Callable]] = {}
        self.message_handlers: Dict[str, Callable] = {}  # topic -> handler
        
        # Statistics
        self.message_count = 0
        self.error_count = 0
        self.last_activity = None
        
        logger.info(f"RealtimeAgent {self.name} initialized with realtime capabilities")
    
    async def initialize(self):
        """Initialize agent and real-time connections"""
        await super().initialize()
        
        if self.config.realtime_platform:
            await self._initialize_realtime_connections()
            logger.info(f"RealtimeAgent {self.name} real-time connections initialized")
    
    async def cleanup(self):
        """Cleanup agent and real-time connections"""
        await self._cleanup_realtime_connections()
        await super().cleanup()
        logger.info(f"RealtimeAgent {self.name} cleanup completed")
    
    async def _initialize_realtime_connections(self):
        """Initialize real-time connections based on configuration"""
        try:
            # Initialize Kafka connections
            if self.config.kafka_bootstrap_servers:
                await self._initialize_kafka()
            
            # Initialize MQTT connections
            if self.config.mqtt_host:
                await self._initialize_mqtt()
            
            # Initialize Redis connections
            if self.config.redis_host:
                await self._initialize_redis()
                
        except Exception as e:
            logger.error(f"Failed to initialize real-time connections: {e}")
            self.error_count += 1
            raise
    
    async def _initialize_kafka(self):
        """Initialize Kafka producer and consumer"""
        try:
            # Import here to avoid dependency issues if not using Kafka
            from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
            
            # Initialize producer
            self.kafka_producer = AIOKafkaProducer(
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self.kafka_producer.start()
            
            # Initialize consumer if topics specified
            if self.config.streaming_topics:
                self.kafka_consumer = AIOKafkaConsumer(
                    *self.config.streaming_topics,
                    bootstrap_servers=self.config.kafka_bootstrap_servers,
                    group_id=self.config.streaming_consumer_group or f"{self.name}-group",
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                await self.kafka_consumer.start()
                
                # Start consuming in background
                asyncio.create_task(self._kafka_consumer_loop())
            
            # Track connection
            self.connections["kafka"] = RealtimeConnectionInfo(
                service_name="kafka",
                connection_type="kafka",
                status=ConnectionStatus.CONNECTED,
                endpoint=self.config.kafka_bootstrap_servers,
                connected_at=datetime.now()
            )
            
            logger.info(f"Kafka connections initialized for {self.name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka: {e}")
            self.connections["kafka"] = RealtimeConnectionInfo(
                service_name="kafka",
                connection_type="kafka", 
                status=ConnectionStatus.ERROR,
                endpoint=self.config.kafka_bootstrap_servers or "unknown",
                error_message=str(e)
            )
            raise
    
    async def _initialize_mqtt(self):
        """Initialize MQTT client"""
        try:
            # Import here to avoid dependency issues if not using MQTT
            import asyncio_mqtt
            
            self.mqtt_client = asyncio_mqtt.Client(
                hostname=self.config.mqtt_host,
                port=self.config.mqtt_port,
                username=self.config.mqtt_user,
                password=self.config.mqtt_password
            )
            
            await self.mqtt_client.__aenter__()
            
            # Subscribe to topics if specified
            if self.config.mqtt_topics:
                for topic in self.config.mqtt_topics:
                    await self.mqtt_client.subscribe(topic)
                
                # Start listening in background
                asyncio.create_task(self._mqtt_listener_loop())
            
            # Track connection
            self.connections["mqtt"] = RealtimeConnectionInfo(
                service_name="mqtt",
                connection_type="mqtt",
                status=ConnectionStatus.CONNECTED,
                endpoint=f"{self.config.mqtt_host}:{self.config.mqtt_port}",
                connected_at=datetime.now()
            )
            
            logger.info(f"MQTT connection initialized for {self.name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize MQTT: {e}")
            self.connections["mqtt"] = RealtimeConnectionInfo(
                service_name="mqtt",
                connection_type="mqtt",
                status=ConnectionStatus.ERROR,
                endpoint=f"{self.config.mqtt_host}:{self.config.mqtt_port}",
                error_message=str(e)
            )
            raise
    
    async def _initialize_redis(self):
        """Initialize Redis client"""
        try:
            # Import here to avoid dependency issues if not using Redis
            import aioredis
            
            self.redis_client = await aioredis.from_url(
                f"redis://{self.config.redis_host}:{self.config.redis_port}"
            )
            
            # Test connection
            await self.redis_client.ping()
            
            # Track connection
            self.connections["redis"] = RealtimeConnectionInfo(
                service_name="redis",
                connection_type="redis",
                status=ConnectionStatus.CONNECTED,
                endpoint=f"{self.config.redis_host}:{self.config.redis_port}",
                connected_at=datetime.now()
            )
            
            logger.info(f"Redis connection initialized for {self.name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Redis: {e}")
            self.connections["redis"] = RealtimeConnectionInfo(
                service_name="redis",
                connection_type="redis",
                status=ConnectionStatus.ERROR,
                endpoint=f"{self.config.redis_host}:{self.config.redis_port}",
                error_message=str(e)
            )
            raise
    
    async def _cleanup_realtime_connections(self):
        """Cleanup all real-time connections"""
        try:
            # Cleanup Kafka
            if self.kafka_producer:
                await self.kafka_producer.stop()
            if self.kafka_consumer:
                await self.kafka_consumer.stop()
            
            # Cleanup MQTT
            if self.mqtt_client:
                await self.mqtt_client.__aexit__(None, None, None)
            
            # Cleanup Redis
            if self.redis_client:
                await self.redis_client.close()
            
            # Close WebSocket connections
            for websocket in self.websocket_connections.copy():
                try:
                    await websocket.close()
                except:
                    pass
            
            self.websocket_connections.clear()
            
            logger.info(f"All real-time connections cleaned up for {self.name}")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    async def _kafka_consumer_loop(self):
        """Background loop for consuming Kafka messages"""
        try:
            async for message in self.kafka_consumer:
                try:
                    await self._handle_kafka_message(message)
                    self.message_count += 1
                    self.last_activity = datetime.now()
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
                    self.error_count += 1
        except Exception as e:
            logger.error(f"Kafka consumer loop error: {e}")
            self.error_count += 1
    
    async def _mqtt_listener_loop(self):
        """Background loop for listening to MQTT messages"""
        try:
            async for message in self.mqtt_client.messages:
                try:
                    await self._handle_mqtt_message(message)
                    self.message_count += 1
                    self.last_activity = datetime.now()
                except Exception as e:
                    logger.error(f"Error processing MQTT message: {e}")
                    self.error_count += 1
        except Exception as e:
            logger.error(f"MQTT listener loop error: {e}")
            self.error_count += 1
    
    async def _handle_kafka_message(self, message):
        """Handle incoming Kafka message"""
        topic = message.topic
        if topic in self.message_handlers:
            await self.message_handlers[topic](message.value)
        else:
            # Default handling - emit as event
            event = RealtimeEvent(
                event_type=EventType.DATA_PROCESSED,
                source_service=self.name,
                source_agent=self.agent_type,
                data={
                    "topic": topic,
                    "message": message.value,
                    "offset": message.offset,
                    "partition": message.partition
                }
            )
            await self._emit_event(event)
    
    async def _handle_mqtt_message(self, message):
        """Handle incoming MQTT message"""
        topic = message.topic.value
        payload = message.payload.decode('utf-8')
        
        if topic in self.message_handlers:
            await self.message_handlers[topic](payload)
        else:
            # Default handling - emit as event
            event = RealtimeEvent(
                event_type=EventType.DATA_PROCESSED,
                source_service=self.name,
                source_agent=self.agent_type,
                data={
                    "topic": topic,
                    "payload": payload
                }
            )
            await self._emit_event(event)
    
    async def _emit_event(self, event: RealtimeEvent):
        """Emit real-time event to all registered handlers"""
        # Call registered event handlers
        if event.event_type in self.event_handlers:
            for handler in self.event_handlers[event.event_type]:
                try:
                    await handler(event)
                except Exception as e:
                    logger.error(f"Error in event handler: {e}")
        
        # Broadcast to WebSocket connections
        if self.websocket_connections:
            message = WebSocketMessage(
                message_type="event",
                payload=event.__dict__,
                correlation_id=event.correlation_id
            )
            await self._broadcast_websocket(message)
    
    async def _broadcast_websocket(self, message: WebSocketMessage):
        """Broadcast message to all WebSocket connections"""
        if not self.websocket_connections:
            return
        
        message_data = message.dict()
        disconnected = set()
        
        for websocket in self.websocket_connections:
            try:
                await websocket.send_text(json.dumps(message_data))
            except Exception as e:
                logger.warning(f"Failed to send WebSocket message: {e}")
                disconnected.add(websocket)
        
        # Remove disconnected connections
        self.websocket_connections -= disconnected
    
    # Public API methods
    
    def register_event_handler(self, event_type: EventType, handler: Callable):
        """Register handler for specific event type"""
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        self.event_handlers[event_type].append(handler)
    
    def register_message_handler(self, topic: str, handler: Callable):
        """Register handler for specific topic"""
        self.message_handlers[topic] = handler
    
    async def send_kafka_message(self, topic: str, message: Any, key: Optional[str] = None):
        """Send message to Kafka topic"""
        if not self.kafka_producer:
            raise RuntimeError("Kafka producer not initialized")
        
        await self.kafka_producer.send_and_wait(topic, message, key=key)
        logger.debug(f"Sent Kafka message to {topic}")
    
    async def send_mqtt_message(self, topic: str, payload: Any, qos: int = 1):
        """Send message to MQTT topic"""
        if not self.mqtt_client:
            raise RuntimeError("MQTT client not initialized")
        
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        
        await self.mqtt_client.publish(topic, payload, qos=qos)
        logger.debug(f"Sent MQTT message to {topic}")
    
    async def add_websocket_connection(self, websocket):
        """Add WebSocket connection"""
        self.websocket_connections.add(websocket)
        logger.debug(f"Added WebSocket connection. Total: {len(self.websocket_connections)}")
    
    async def remove_websocket_connection(self, websocket):
        """Remove WebSocket connection"""
        self.websocket_connections.discard(websocket)
        logger.debug(f"Removed WebSocket connection. Total: {len(self.websocket_connections)}")
    
    def get_realtime_status(self) -> AgentRealtimeStatus:
        """Get current real-time status"""
        return AgentRealtimeStatus(
            agent_type=self.agent_type,
            implementation_type=getattr(self.config, 'implementation_type', 'unknown'),
            service_name=self.name,
            realtime_enabled=bool(self.config.realtime_platform),
            websocket_enabled=self.config.websocket_enabled,
            connections=list(self.connections.values()),
            active_streams=self.config.streaming_topics,
            message_count=self.message_count,
            error_count=self.error_count,
            last_activity=self.last_activity
        )
    
    async def process_realtime_task(self, task: AgentTask) -> AgentResponse:
        """Process task and emit real-time events"""
        # Emit task started event
        start_event = RealtimeEvent(
            event_type=EventType.AGENT_TASK_STARTED,
            source_service=self.name,
            source_agent=self.agent_type,
            data={"task_id": task.task_id, "task_type": task.task_type},
            correlation_id=task.task_id
        )
        await self._emit_event(start_event)
        
        try:
            # Process task using parent method
            response = await self.process_task(task)
            
            # Emit completion event
            completion_event = RealtimeEvent(
                event_type=EventType.AGENT_TASK_COMPLETED if response.success else EventType.AGENT_TASK_FAILED,
                source_service=self.name,
                source_agent=self.agent_type,
                data={
                    "task_id": task.task_id,
                    "success": response.success,
                    "result": response.result if response.success else None,
                    "error": response.error if not response.success else None
                },
                correlation_id=task.task_id
            )
            await self._emit_event(completion_event)
            
            return response
            
        except Exception as e:
            # Emit failure event
            failure_event = RealtimeEvent(
                event_type=EventType.AGENT_TASK_FAILED,
                source_service=self.name,
                source_agent=self.agent_type,
                data={"task_id": task.task_id, "error": str(e)},
                correlation_id=task.task_id
            )
            await self._emit_event(failure_event)
            raise