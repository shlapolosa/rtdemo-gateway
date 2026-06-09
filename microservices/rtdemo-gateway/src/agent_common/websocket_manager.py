"""
WebSocket connection manager and utilities for real-time communication
"""

import json
import logging
import asyncio
from typing import Set, Dict, Any, Optional, List, Callable
from datetime import datetime
from fastapi import WebSocket, WebSocketDisconnect

from .models import WebSocketMessage, RealtimeEvent, ConnectionStatus

logger = logging.getLogger(__name__)


class WebSocketConnectionManager:
    """
    Manages WebSocket connections for real-time communication
    """
    
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.connection_metadata: Dict[WebSocket, Dict[str, Any]] = {}
        self.topic_subscriptions: Dict[str, Set[WebSocket]] = {}
        self.message_handlers: Dict[str, Callable] = {}
        self.connection_count = 0
    
    async def connect(self, websocket: WebSocket, client_id: Optional[str] = None, 
                     metadata: Optional[Dict[str, Any]] = None):
        """
        Accept and register a new WebSocket connection
        
        Args:
            websocket: WebSocket connection instance
            client_id: Optional client identifier
            metadata: Optional connection metadata
        """
        try:
            await websocket.accept()
            self.active_connections.add(websocket)
            
            # Store connection metadata
            self.connection_metadata[websocket] = {
                "client_id": client_id or f"client_{len(self.active_connections)}",
                "connected_at": datetime.now(),
                "last_activity": datetime.now(),
                "subscriptions": set(),
                "message_count": 0,
                **(metadata or {})
            }
            
            self.connection_count += 1
            
            logger.info(f"WebSocket connection established. Client: {client_id}, Total: {len(self.active_connections)}")
            
            # Send welcome message
            welcome_message = WebSocketMessage(
                message_type="connection_established",
                payload={
                    "client_id": self.connection_metadata[websocket]["client_id"],
                    "server_time": datetime.now().isoformat(),
                    "connection_id": self.connection_count
                }
            )
            await self._send_to_connection(websocket, welcome_message)
            
        except Exception as e:
            logger.error(f"Failed to establish WebSocket connection: {e}")
            if websocket in self.active_connections:
                await self.disconnect(websocket)
            raise
    
    async def disconnect(self, websocket: WebSocket, code: int = 1000, reason: str = "Normal closure"):
        """
        Disconnect and cleanup a WebSocket connection
        
        Args:
            websocket: WebSocket connection to disconnect
            code: WebSocket close code
            reason: Reason for disconnection
        """
        try:
            # Remove from topic subscriptions
            for topic, subscribers in self.topic_subscriptions.items():
                subscribers.discard(websocket)
            
            # Remove connection metadata
            metadata = self.connection_metadata.pop(websocket, {})
            client_id = metadata.get("client_id", "unknown")
            
            # Remove from active connections
            self.active_connections.discard(websocket)
            
            # Close connection if still open
            if websocket.client_state.name != "DISCONNECTED":
                await websocket.close(code=code, reason=reason)
            
            logger.info(f"WebSocket disconnected. Client: {client_id}, Reason: {reason}, Total: {len(self.active_connections)}")
            
        except Exception as e:
            logger.error(f"Error during WebSocket disconnect: {e}")
    
    async def broadcast(self, message: WebSocketMessage, exclude: Optional[Set[WebSocket]] = None):
        """
        Broadcast message to all active connections
        
        Args:
            message: Message to broadcast
            exclude: Set of connections to exclude from broadcast
        """
        if not self.active_connections:
            return
        
        exclude = exclude or set()
        targets = self.active_connections - exclude
        
        await self._send_to_multiple(targets, message)
        logger.debug(f"Broadcast message to {len(targets)} connections")
    
    async def send_to_topic(self, topic: str, message: WebSocketMessage):
        """
        Send message to all connections subscribed to a topic
        
        Args:
            topic: Topic name
            message: Message to send
        """
        subscribers = self.topic_subscriptions.get(topic, set())
        if subscribers:
            await self._send_to_multiple(subscribers, message)
            logger.debug(f"Sent message to {len(subscribers)} subscribers of topic '{topic}'")
    
    async def send_to_client(self, client_id: str, message: WebSocketMessage) -> bool:
        """
        Send message to a specific client
        
        Args:
            client_id: Client identifier
            message: Message to send
            
        Returns:
            True if message was sent successfully
        """
        for websocket, metadata in self.connection_metadata.items():
            if metadata.get("client_id") == client_id:
                success = await self._send_to_connection(websocket, message)
                return success
        
        logger.warning(f"Client not found: {client_id}")
        return False
    
    async def subscribe_to_topic(self, websocket: WebSocket, topic: str):
        """
        Subscribe a connection to a topic
        
        Args:
            websocket: WebSocket connection
            topic: Topic to subscribe to
        """
        if topic not in self.topic_subscriptions:
            self.topic_subscriptions[topic] = set()
        
        self.topic_subscriptions[topic].add(websocket)
        
        # Update connection metadata
        if websocket in self.connection_metadata:
            self.connection_metadata[websocket]["subscriptions"].add(topic)
        
        logger.debug(f"Client subscribed to topic '{topic}'")
        
        # Send subscription confirmation
        confirmation = WebSocketMessage(
            message_type="subscription_confirmed",
            payload={"topic": topic, "subscriber_count": len(self.topic_subscriptions[topic])}
        )
        await self._send_to_connection(websocket, confirmation)
    
    async def unsubscribe_from_topic(self, websocket: WebSocket, topic: str):
        """
        Unsubscribe a connection from a topic
        
        Args:
            websocket: WebSocket connection
            topic: Topic to unsubscribe from
        """
        if topic in self.topic_subscriptions:
            self.topic_subscriptions[topic].discard(websocket)
            
            # Clean up empty topics
            if not self.topic_subscriptions[topic]:
                del self.topic_subscriptions[topic]
        
        # Update connection metadata
        if websocket in self.connection_metadata:
            self.connection_metadata[websocket]["subscriptions"].discard(topic)
        
        logger.debug(f"Client unsubscribed from topic '{topic}'")
        
        # Send unsubscription confirmation
        confirmation = WebSocketMessage(
            message_type="unsubscription_confirmed",
            payload={"topic": topic}
        )
        await self._send_to_connection(websocket, confirmation)
    
    async def handle_message(self, websocket: WebSocket, message: str):
        """
        Handle incoming message from WebSocket connection
        
        Args:
            websocket: WebSocket connection
            message: Raw message string
        """
        try:
            # Parse message
            data = json.loads(message)
            message_type = data.get("message_type")
            payload = data.get("payload", {})
            
            # Update connection activity
            if websocket in self.connection_metadata:
                self.connection_metadata[websocket]["last_activity"] = datetime.now()
                self.connection_metadata[websocket]["message_count"] += 1
            
            # Handle built-in message types
            if message_type == "subscribe":
                topic = payload.get("topic")
                if topic:
                    await self.subscribe_to_topic(websocket, topic)
                return
            
            elif message_type == "unsubscribe":
                topic = payload.get("topic")
                if topic:
                    await self.unsubscribe_from_topic(websocket, topic)
                return
            
            elif message_type == "ping":
                pong_message = WebSocketMessage(
                    message_type="pong",
                    payload={"timestamp": datetime.now().isoformat()},
                    response_to=data.get("message_id")
                )
                await self._send_to_connection(websocket, pong_message)
                return
            
            # Handle custom message types
            if message_type in self.message_handlers:
                await self.message_handlers[message_type](websocket, payload)
            else:
                logger.warning(f"Unknown message type: {message_type}")
                
                # Send error response
                error_message = WebSocketMessage(
                    message_type="error",
                    payload={"error": f"Unknown message type: {message_type}"},
                    response_to=data.get("message_id")
                )
                await self._send_to_connection(websocket, error_message)
        
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON received from WebSocket")
            error_message = WebSocketMessage(
                message_type="error",
                payload={"error": "Invalid JSON format"}
            )
            await self._send_to_connection(websocket, error_message)
        
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")
            error_message = WebSocketMessage(
                message_type="error",
                payload={"error": str(e)}
            )
            await self._send_to_connection(websocket, error_message)
    
    def register_message_handler(self, message_type: str, handler: Callable):
        """
        Register handler for custom message types
        
        Args:
            message_type: Type of message to handle
            handler: Async function that takes (websocket, payload) arguments
        """
        self.message_handlers[message_type] = handler
        logger.debug(f"Registered handler for message type: {message_type}")
    
    async def broadcast_event(self, event: RealtimeEvent):
        """
        Broadcast a real-time event to all connections
        
        Args:
            event: Real-time event to broadcast
        """
        message = WebSocketMessage(
            message_type="realtime_event",
            payload=event.__dict__ if hasattr(event, '__dict__') else event.dict(),
            correlation_id=getattr(event, 'correlation_id', None)
        )
        await self.broadcast(message)
    
    async def send_event_to_topic(self, topic: str, event: RealtimeEvent):
        """
        Send a real-time event to topic subscribers
        
        Args:
            topic: Topic name
            event: Real-time event to send
        """
        message = WebSocketMessage(
            message_type="realtime_event",
            payload=event.__dict__ if hasattr(event, '__dict__') else event.dict(),
            correlation_id=getattr(event, 'correlation_id', None)
        )
        await self.send_to_topic(topic, message)
    
    async def _send_to_connection(self, websocket: WebSocket, message: WebSocketMessage) -> bool:
        """
        Send message to a single WebSocket connection
        
        Args:
            websocket: Target connection
            message: Message to send
            
        Returns:
            True if sent successfully
        """
        try:
            message_data = message.dict()
            await websocket.send_text(json.dumps(message_data))
            return True
        except WebSocketDisconnect:
            logger.info("WebSocket disconnected during send")
            await self.disconnect(websocket, reason="Disconnected during send")
            return False
        except Exception as e:
            logger.error(f"Failed to send WebSocket message: {e}")
            return False
    
    async def _send_to_multiple(self, connections: Set[WebSocket], message: WebSocketMessage):
        """
        Send message to multiple WebSocket connections
        
        Args:
            connections: Set of target connections
            message: Message to send
        """
        if not connections:
            return
        
        message_data = message.dict()
        message_json = json.dumps(message_data)
        
        # Send to all connections concurrently
        send_tasks = []
        for websocket in connections.copy():  # Copy to avoid modification during iteration
            task = asyncio.create_task(self._send_with_error_handling(websocket, message_json))
            send_tasks.append(task)
        
        # Wait for all sends to complete
        if send_tasks:
            await asyncio.gather(*send_tasks, return_exceptions=True)
    
    async def _send_with_error_handling(self, websocket: WebSocket, message_json: str):
        """
        Send message with proper error handling
        
        Args:
            websocket: Target connection
            message_json: JSON message string
        """
        try:
            await websocket.send_text(message_json)
        except WebSocketDisconnect:
            await self.disconnect(websocket, reason="Disconnected during broadcast")
        except Exception as e:
            logger.error(f"Failed to send message to WebSocket: {e}")
            await self.disconnect(websocket, reason=f"Send error: {str(e)}")
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """
        Get statistics about WebSocket connections
        
        Returns:
            Dictionary with connection statistics
        """
        total_connections = len(self.active_connections)
        total_subscriptions = sum(len(subscribers) for subscribers in self.topic_subscriptions.values())
        
        # Calculate total messages
        total_messages = sum(
            metadata.get("message_count", 0) 
            for metadata in self.connection_metadata.values()
        )
        
        return {
            "total_connections": total_connections,
            "total_topics": len(self.topic_subscriptions),
            "total_subscriptions": total_subscriptions,
            "total_messages": total_messages,
            "topics": {
                topic: len(subscribers) 
                for topic, subscribers in self.topic_subscriptions.items()
            },
            "connection_details": [
                {
                    "client_id": metadata.get("client_id"),
                    "connected_at": metadata.get("connected_at").isoformat() if metadata.get("connected_at") else None,
                    "message_count": metadata.get("message_count", 0),
                    "subscriptions": list(metadata.get("subscriptions", set()))
                }
                for metadata in self.connection_metadata.values()
            ]
        }
    
    async def cleanup_inactive_connections(self, timeout_seconds: int = 300):
        """
        Clean up inactive connections based on timeout
        
        Args:
            timeout_seconds: Timeout in seconds for inactive connections
        """
        current_time = datetime.now()
        inactive_connections = []
        
        for websocket, metadata in self.connection_metadata.items():
            last_activity = metadata.get("last_activity")
            if last_activity:
                inactive_duration = (current_time - last_activity).total_seconds()
                if inactive_duration > timeout_seconds:
                    inactive_connections.append(websocket)
        
        # Disconnect inactive connections
        for websocket in inactive_connections:
            await self.disconnect(websocket, reason="Inactive timeout")
        
        if inactive_connections:
            logger.info(f"Cleaned up {len(inactive_connections)} inactive connections")


# Global WebSocket manager instance
websocket_manager = WebSocketConnectionManager()