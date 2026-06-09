"""RT-1 realtime-service entrypoint (generated). Websocket + Kafka over
agent_common.realtime_fastapi. Topics discovered from CONSUME_*/PRODUCE_*/TOPIC_*
env injected by the realtime-service CD + <realtime>-conn secret."""
import os
from agent_common.realtime_fastapi import create_realtime_agent_app
from agent_common.realtime_agent import GenericRealtimeAgent

SERVICE_NAME = os.getenv("WEBSERVICE_NAME", os.getenv("REALTIME_PLATFORM_NAME", "realtime-service"))

app = create_realtime_agent_app(
    agent_class=GenericRealtimeAgent,
    service_name=SERVICE_NAME,
    description="RT-1 realtime websocket+kafka service",
    endpoints=[],
    websocket_endpoints=[{"path": "/ws", "description": "realtime stream"}],
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
