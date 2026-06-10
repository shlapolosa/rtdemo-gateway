"""
Enhanced FastAPI application factory with real-time capabilities
"""

import os
import json
import logging
import asyncio
from typing import Dict, Any, List, Optional, Type, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import httpx

from .models import (
    AgentRequestModel, AgentResponseModel, WebSocketMessage, RealtimeEvent,
    AgentRealtimeStatus, HealthDataModel, StreamingResponseModel
)
from .realtime_agent import RealtimeAgent
from .config import AgentConfig, get_agent_config
from .secret_loader import load_realtime_platform_secrets, configure_agent_from_secrets
from .websocket_manager import WebSocketConnectionManager

logger = logging.getLogger(__name__)


def create_realtime_agent_app(
    agent_class: Type[RealtimeAgent],
    service_name: str,
    description: str,
    endpoints: List[Dict[str, str]],
    websocket_endpoints: Optional[List[Dict[str, str]]] = None,
    streaming_endpoints: Optional[List[Dict[str, str]]] = None,
    config: Optional[AgentConfig] = None,
    verify_token: Optional[Callable[[Optional[str]], bool]] = None,
) -> FastAPI:
    """
    Create a FastAPI app for a real-time enabled agent microservice
    
    Args:
        agent_class: The real-time agent class to instantiate
        service_name: Name of the service (e.g., "business-analyst-anthropic")
        description: Service description
        endpoints: List of standard HTTP endpoint configs
        websocket_endpoints: List of WebSocket endpoint configs
        streaming_endpoints: List of server-sent events endpoint configs
        config: Optional pre-configured AgentConfig
    """
    
    # Global instances
    agent: Optional[RealtimeAgent] = None
    websocket_manager = WebSocketConnectionManager()
    agent_config = config

    # RT-1 (#156): WebSocket auth. APIM does not proxy /ws (Developer-SKU
    # POST-body bug + websocket gaps), so /ws is exposed via Istio and JWT is
    # verified in-service here. Default verifier: if JWT_ISSUER_URI is present
    # (from the bound <identity>-conn) require a non-empty bearer/?token=;
    # otherwise (no identity bound) /ws is open (backward compatible).
    # NOTE: this is a presence/issuer-config gate, not full signature
    # validation — see plan §8 "limits"; a real verify_token should be passed
    # by services needing cryptographic verification.
    _jwt_issuer = os.getenv("JWT_ISSUER_URI") or os.getenv("AUTH0_ISSUER")

    def _default_verify_token(token: Optional[str]) -> bool:
        if not _jwt_issuer:
            return True  # no identity bound -> open
        return bool(token)

    _verify = verify_token or _default_verify_token

    # RT-1 (#167): the WebSocket routes must be registered at app-BUILD time, but
    # the runtime agent_config (with websocket_enabled) is only loaded later in
    # lifespan — at build time `config` is usually None, so gating registration on
    # it left /ws unregistered (404). Decide from build-time signals instead: the
    # WEBSOCKET_ENABLED env (set by the realtime-service CD), an explicit
    # websocket_endpoints list, or a pre-supplied config.
    _websocket_enabled = (
        os.getenv("WEBSOCKET_ENABLED", "false").lower() == "true"
        or bool(websocket_endpoints)
        or (config is not None and getattr(config, "websocket_enabled", False))
    )

    def _extract_ws_token(ws: WebSocket) -> Optional[str]:
        auth = ws.headers.get("authorization") or ws.headers.get("Authorization")
        if auth and auth.lower().startswith("bearer "):
            return auth[7:].strip()
        return ws.query_params.get("token")
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Manage application lifespan with real-time setup"""
        nonlocal agent, agent_config
        
        # Startup
        logger.info(f"Starting real-time {service_name} service...")
        
        try:
            # Load configuration. A realtime stream gateway is not an AI agent,
            # so it doesn't require AGENT_TYPE / IMPLEMENTATION_TYPE to be set —
            # fall back to the generic orchestrator/deterministic identity.
            if not agent_config:
                agent_config = get_agent_config(
                    default_agent_type="orchestrator",
                    default_implementation_type="deterministic",
                )
            
            # Load platform secrets if realtime platform is specified
            if agent_config.realtime_platform:
                logger.info(f"Loading secrets for realtime platform: {agent_config.realtime_platform}")
                secrets = await load_realtime_platform_secrets(agent_config.realtime_platform)
                agent_config = configure_agent_from_secrets(agent_config, secrets)
                logger.info(f"Configuration updated with platform secrets")
            
            # Initialize agent with configuration
            agent = agent_class(
                agent_type=service_name.split("-")[0],
                agent_name=service_name,
                description=description,
                config=agent_config
            )
            
            await agent.initialize()
            logger.info(f"RealtimeAgent {agent_class.__name__} initialized successfully")
            
            # Register WebSocket event handlers
            _register_websocket_handlers(agent, websocket_manager)
            
            # Start background tasks
            if agent_config.websocket_enabled:
                asyncio.create_task(_websocket_cleanup_task(websocket_manager))
            
        except Exception as e:
            logger.error(f"Failed to initialize real-time agent: {e}")
            raise
        
        yield
        
        # Shutdown
        logger.info(f"Shutting down real-time {service_name} service...")
        if agent:
            await agent.cleanup()
    
    # Initialize FastAPI app
    app = FastAPI(
        title=f"{agent_class.__name__} Real-time Service",
        description=description + " (Real-time enabled)",
        version="1.0.0",
        lifespan=lifespan
    )
    
    async def get_agent() -> RealtimeAgent:
        """Dependency to get the agent instance"""
        if agent is None:
            raise HTTPException(status_code=503, detail="Agent not initialized")
        return agent
    
    async def get_websocket_manager() -> WebSocketConnectionManager:
        """Dependency to get the WebSocket manager"""
        return websocket_manager
    
    # =====================================================================
    # Standard HTTP Endpoints
    # =====================================================================
    
    @app.get("/health")
    async def health_check():
        """Enhanced health check with real-time status"""
        base_health = {"status": "healthy", "service": service_name}
        
        if agent:
            realtime_status = agent.get_realtime_status()
            base_health.update({
                "realtime_enabled": realtime_status.realtime_enabled,
                "websocket_enabled": realtime_status.websocket_enabled,
                "connections": len(realtime_status.connections),
                "message_count": realtime_status.message_count,
                "error_count": realtime_status.error_count
            })
        
        return base_health
    
    @app.get("/realtime/status", response_model=AgentRealtimeStatus)
    async def get_realtime_status(agent_instance: RealtimeAgent = Depends(get_agent)):
        """Get detailed real-time status"""
        return agent_instance.get_realtime_status()
    
    @app.get("/realtime/connections")
    async def get_connection_stats(manager: WebSocketConnectionManager = Depends(get_websocket_manager)):
        """Get WebSocket connection statistics"""
        return manager.get_connection_stats()
    
    # Add standard HTTP endpoints
    for endpoint_config in endpoints:
        path = endpoint_config["path"]
        task_type = endpoint_config["task_type"]
        method = endpoint_config.get("method", "POST")
        enable_realtime = endpoint_config.get("realtime", True)
        
        def create_endpoint_handler(task_type_param: str, realtime_enabled: bool):
            async def endpoint_handler(
                request: AgentRequestModel,
                agent_instance: RealtimeAgent = Depends(get_agent)
            ) -> AgentResponseModel:
                try:
                    # Create task for the agent
                    task_payload = {
                        "query": request.query,
                        "parameters": request.parameters or {},
                        "context": request.context or {}
                    }
                    
                    from .base_microservice_agent import AgentTask
                    task = AgentTask(
                        task_id=f"{task_type_param}-{abs(hash(str(task_payload)))}",
                        task_type=task_type_param,
                        payload=task_payload
                    )
                    
                    # Process with real-time events if enabled
                    if realtime_enabled and agent_instance.config.realtime_platform:
                        response = await agent_instance.process_realtime_task(task)
                    else:
                        response = await agent_instance.process_task(task)
                    
                    if response.success:
                        return AgentResponseModel(
                            result=response.result,
                            metadata={
                                "agent_type": agent_instance.agent_type,
                                "implementation": service_name.split("-")[-1],
                                "task_type": task_type_param,
                                "processing_time": response.metadata.get("processing_time", 0),
                                "realtime_enabled": realtime_enabled
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
        handler = create_endpoint_handler(task_type, enable_realtime)
        app.add_api_route(
            path,
            handler,
            methods=[method],
            response_model=AgentResponseModel,
            name=task_type.replace("_", "-")
        )
    
    # =====================================================================
    # WebSocket Endpoints
    # =====================================================================
    
    if _websocket_enabled:

        @app.websocket("/ws")
        async def websocket_endpoint(
            websocket: WebSocket,
            manager: WebSocketConnectionManager = Depends(get_websocket_manager)
        ):
            """Main WebSocket endpoint for real-time communication"""
            # RT-1 (#156): in-service JWT gate on the upgrade.
            if not _verify(_extract_ws_token(websocket)):
                await websocket.close(code=4401)  # 4401 ~ unauthorized
                return
            await manager.connect(websocket)
            
            try:
                while True:
                    # Receive message from client
                    data = await websocket.receive_text()
                    await manager.handle_message(websocket, data)
                    
            except WebSocketDisconnect:
                await manager.disconnect(websocket)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await manager.disconnect(websocket, reason=f"Error: {str(e)}")
        
        @app.websocket("/ws/events")
        async def events_websocket(
            websocket: WebSocket,
            manager: WebSocketConnectionManager = Depends(get_websocket_manager)
        ):
            """WebSocket endpoint specifically for real-time events"""
            # RT-1 (#156): in-service JWT gate on the upgrade.
            if not _verify(_extract_ws_token(websocket)):
                await websocket.close(code=4401)
                return
            await manager.connect(websocket, metadata={"endpoint_type": "events"})
            
            try:
                # Auto-subscribe to general events
                await manager.subscribe_to_topic(websocket, "events")
                
                while True:
                    data = await websocket.receive_text()
                    await manager.handle_message(websocket, data)
                    
            except WebSocketDisconnect:
                await manager.disconnect(websocket)
            except Exception as e:
                logger.error(f"Events WebSocket error: {e}")
                await manager.disconnect(websocket, reason=f"Error: {str(e)}")
        
        # Add custom WebSocket endpoints. Skip the reserved paths already
        # registered above (/ws, /ws/events) — those carry the in-service JWT
        # gate, whereas this generic loop does not; re-registering /ws here would
        # both double-register and bypass auth.
        _reserved_ws_paths = {"/ws", "/ws/events"}
        if websocket_endpoints:
            for ws_config in websocket_endpoints:
                path = ws_config["path"]
                if path in _reserved_ws_paths:
                    continue
                handler_name = ws_config.get("handler", "default")
                auto_subscribe = ws_config.get("auto_subscribe", [])
                
                def create_ws_handler(subscribe_topics: List[str]):
                    async def ws_handler(
                        websocket: WebSocket,
                        manager: WebSocketConnectionManager = Depends(get_websocket_manager)
                    ):
                        await manager.connect(websocket, metadata={"endpoint_type": handler_name})
                        
                        # Auto-subscribe to specified topics
                        for topic in subscribe_topics:
                            await manager.subscribe_to_topic(websocket, topic)
                        
                        try:
                            while True:
                                data = await websocket.receive_text()
                                await manager.handle_message(websocket, data)
                        except WebSocketDisconnect:
                            await manager.disconnect(websocket)
                        except Exception as e:
                            await manager.disconnect(websocket, reason=f"Error: {str(e)}")
                    
                    return ws_handler
                
                app.add_api_websocket_route(path, create_ws_handler(auto_subscribe))
    
    # =====================================================================
    # Server-Sent Events (SSE) Endpoints
    # =====================================================================
    
    @app.get("/stream/events")
    async def stream_events(agent_instance: RealtimeAgent = Depends(get_agent)):
        """Server-sent events stream for real-time events"""
        async def event_stream():
            try:
                # This would be connected to the agent's event system
                while True:
                    # For demo purposes, send periodic status updates
                    status = agent_instance.get_realtime_status()
                    data = {
                        "type": "status_update",
                        "data": status.dict(),
                        "timestamp": status.last_activity.isoformat() if status.last_activity else None
                    }
                    yield f"data: {json.dumps(data)}\n\n"
                    await asyncio.sleep(5)  # Update every 5 seconds
            except Exception as e:
                logger.error(f"SSE stream error: {e}")
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
        
        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Cache-Control"
            }
        )
    
    # Add custom streaming endpoints
    if streaming_endpoints:
        for stream_config in streaming_endpoints:
            path = stream_config["path"]
            topic = stream_config.get("topic", "default")
            
            def create_stream_handler(stream_topic: str):
                async def stream_handler():
                    async def topic_stream():
                        # This would connect to Kafka or other streaming source
                        counter = 0
                        while True:
                            data = {
                                "topic": stream_topic,
                                "counter": counter,
                                "timestamp": asyncio.get_event_loop().time()
                            }
                            yield f"data: {json.dumps(data)}\n\n"
                            counter += 1
                            await asyncio.sleep(1)
                    
                    return StreamingResponse(
                        topic_stream(),
                        media_type="text/event-stream",
                        headers={
                            "Cache-Control": "no-cache",
                            "Connection": "keep-alive"
                        }
                    )
                
                return stream_handler
            
            app.add_api_route(path, create_stream_handler(topic), methods=["GET"])
    
    # =====================================================================
    # Real-time API Endpoints
    # =====================================================================
    
    @app.post("/realtime/broadcast")
    async def broadcast_message(
        message: Dict[str, Any],
        manager: WebSocketConnectionManager = Depends(get_websocket_manager)
    ):
        """Broadcast message to all WebSocket connections"""
        ws_message = WebSocketMessage(
            message_type="broadcast",
            payload=message
        )
        await manager.broadcast(ws_message)
        return {"status": "broadcasted", "connections": len(manager.active_connections)}
    
    @app.post("/realtime/topic/{topic}")
    async def send_to_topic(
        topic: str,
        message: Dict[str, Any],
        manager: WebSocketConnectionManager = Depends(get_websocket_manager)
    ):
        """Send message to specific topic subscribers"""
        ws_message = WebSocketMessage(
            message_type="topic_message",
            payload={"topic": topic, "data": message}
        )
        await manager.send_to_topic(topic, ws_message)
        
        subscriber_count = len(manager.topic_subscriptions.get(topic, set()))
        return {"status": "sent", "topic": topic, "subscribers": subscriber_count}
    
    # Main entry point
    @app.get("/")
    async def root():
        base_info = {"service": service_name, "status": "running", "type": "realtime"}
        
        if agent:
            realtime_status = agent.get_realtime_status()
            base_info.update({
                "realtime_enabled": realtime_status.realtime_enabled,
                "websocket_enabled": realtime_status.websocket_enabled,
                "platform": agent.config.realtime_platform
            })
        
        return base_info
    
    return app


def _register_websocket_handlers(agent: RealtimeAgent, manager: WebSocketConnectionManager):
    """Register WebSocket message handlers for the agent"""
    
    async def handle_agent_request(websocket, payload):
        """Handle agent task requests via WebSocket"""
        try:
            query = payload.get("query")
            task_type = payload.get("task_type", "default")
            parameters = payload.get("parameters", {})
            
            if not query:
                error_msg = WebSocketMessage(
                    message_type="error",
                    payload={"error": "Query is required"}
                )
                await manager._send_to_connection(websocket, error_msg)
                return
            
            # Create and process task
            from .base_microservice_agent import AgentTask
            task = AgentTask(
                task_id=f"ws-{abs(hash(str(payload)))}",
                task_type=task_type,
                payload={"query": query, "parameters": parameters}
            )
            
            response = await agent.process_realtime_task(task)
            
            # Send response back to WebSocket
            response_msg = WebSocketMessage(
                message_type="agent_response",
                payload={
                    "success": response.success,
                    "result": response.result,
                    "error": response.error,
                    "task_id": task.task_id
                }
            )
            await manager._send_to_connection(websocket, response_msg)
            
        except Exception as e:
            logger.error(f"Error handling agent request via WebSocket: {e}")
            error_msg = WebSocketMessage(
                message_type="error",
                payload={"error": str(e)}
            )
            await manager._send_to_connection(websocket, error_msg)
    
    # Register the handler
    manager.register_message_handler("agent_request", handle_agent_request)


async def _websocket_cleanup_task(manager: WebSocketConnectionManager):
    """Background task to clean up inactive WebSocket connections"""
    while True:
        try:
            await asyncio.sleep(300)  # Run every 5 minutes
            await manager.cleanup_inactive_connections(timeout_seconds=600)  # 10 minute timeout
        except Exception as e:
            logger.error(f"Error in WebSocket cleanup task: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retry


# Convenience function for backwards compatibility
def create_agent_app(
    agent_class: Type,
    service_name: str,
    description: str,
    endpoints: List[Dict[str, str]],
    **kwargs
) -> FastAPI:
    """
    Backwards compatible agent app creator.
    Automatically uses realtime features if agent is a RealtimeAgent.
    """
    if issubclass(agent_class, RealtimeAgent):
        return create_realtime_agent_app(
            agent_class, service_name, description, endpoints, **kwargs
        )
    else:
        # Fall back to original implementation
        from .fastapi_base import create_agent_app as create_standard_app
        return create_standard_app(agent_class, service_name, description, endpoints)