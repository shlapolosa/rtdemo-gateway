"""
Enhanced platform secret loading utilities for real-time platform integration
with cross-component discovery and standardized naming conventions
"""

import os
import json
import logging
import time
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


class PlatformSecretLoader:
    """
    Loads secrets from Kubernetes mounted secrets or environment variables
    following the standardized naming pattern from realtime platform
    """
    
    def __init__(self, platform_name: Optional[str] = None):
        self.platform_name = platform_name
        self.secret_mount_path = "/var/secrets"  # Standard Kubernetes secret mount path
        self.webservice_name = os.getenv("WEBSERVICE_NAME")  # For cross-component discovery
    
    def generate_secret_name(self, component_name: str, service_name: str) -> str:
        """
        Generate standardized secret name using the {name}-{service}-secret pattern
        
        Args:
            component_name: Name of the component/platform
            service_name: Name of the service (kafka, mqtt, db, etc.)
            
        Returns:
            Standardized secret name
        """
        return f"{component_name}-{service_name}-secret"
    
    async def discover_realtime_platform_secrets(self, realtime_name: str) -> List[Dict[str, Any]]:
        """
        Cross-component secret discovery mechanism that allows webservices to
        discover secrets from realtime-platform components
        
        Args:
            realtime_name: Name of the realtime platform to discover secrets from
            
        Returns:
            List of discovered secrets with their metadata
        """
        discovered_secrets = []
        
        # Standard realtime platform service types
        service_types = ['kafka', 'mqtt', 'db', 'metabase', 'lenses']
        
        for service_type in service_types:
            secret_name = self.generate_secret_name(realtime_name, service_type)
            secret_data = await self._load_mounted_secret(secret_name)
            
            if secret_data:
                discovered_secrets.append({
                    'name': secret_name,
                    'service_type': service_type,
                    'platform': realtime_name,
                    'data': secret_data,
                    'labels': {
                        'app.kubernetes.io/part-of': 'realtime-platform',
                        'realtime.platform.example.org/name': realtime_name,
                        'app.kubernetes.io/managed-by': 'crossplane',
                        'app.kubernetes.io/discoverable': 'true'
                    }
                })
                logger.info(f"Discovered secret: {secret_name} for service: {service_type}")
        
        logger.info(f"Discovered {len(discovered_secrets)} secrets for realtime platform: {realtime_name}")
        return discovered_secrets
    
    async def load_platform_secrets(self, platform_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Load all secrets for a realtime platform
        
        Args:
            platform_name: Name of the realtime platform (overrides instance setting)
        
        Returns:
            Dictionary containing all connection details
        """
        platform = platform_name or self.platform_name
        if not platform:
            raise ValueError("Platform name must be provided")
        
        secrets = {}
        
        # Load each type of secret
        secrets.update(await self._load_kafka_secrets(platform))
        secrets.update(await self._load_mqtt_secrets(platform))
        secrets.update(await self._load_database_secrets(platform))
        secrets.update(await self._load_analytics_secrets(platform))
        secrets.update(await self._load_streaming_secrets(platform))
        
        logger.info(f"Loaded secrets for platform: {platform}")
        logger.debug(f"Available secret types: {list(secrets.keys())}")
        
        return secrets
    
    async def _load_kafka_secrets(self, platform_name: str) -> Dict[str, Any]:
        """Load Kafka connection secrets using standardized naming"""
        secret_name = self.generate_secret_name(platform_name, "kafka")
        
        # Try mounted secret first
        kafka_secrets = await self._load_mounted_secret(secret_name)
        if kafka_secrets:
            return {
                "kafka_bootstrap_servers": kafka_secrets.get("KAFKA_BOOTSTRAP_SERVERS"),
                "kafka_schema_registry_url": kafka_secrets.get("KAFKA_SCHEMA_REGISTRY_URL"),
                "kafka_username": kafka_secrets.get("KAFKA_USERNAME"),
                "kafka_password": kafka_secrets.get("KAFKA_PASSWORD"),
                "kafka_security_protocol": kafka_secrets.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
                "kafka_sasl_mechanism": kafka_secrets.get("KAFKA_SASL_MECHANISM")
            }
        
        # Fallback to environment variables
        return {
            "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "kafka_schema_registry_url": os.getenv("KAFKA_SCHEMA_REGISTRY_URL"),
            "kafka_username": os.getenv("KAFKA_USERNAME"),
            "kafka_password": os.getenv("KAFKA_PASSWORD"),
            "kafka_security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            "kafka_sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM")
        }
    
    async def _load_mqtt_secrets(self, platform_name: str) -> Dict[str, Any]:
        """Load MQTT connection secrets using standardized naming"""
        secret_name = self.generate_secret_name(platform_name, "mqtt")
        
        # Try mounted secret first
        mqtt_secrets = await self._load_mounted_secret(secret_name)
        if mqtt_secrets:
            return {
                "mqtt_host": mqtt_secrets.get("MQTT_HOST"),
                "mqtt_port": int(mqtt_secrets.get("MQTT_PORT", "1883")),
                "mqtt_user": mqtt_secrets.get("MQTT_USER"),
                "mqtt_password": mqtt_secrets.get("MQTT_PASSWORD"),
                "mqtt_protocol": mqtt_secrets.get("MQTT_PROTOCOL", "tcp"),
                "mqtt_client_id": mqtt_secrets.get("MQTT_CLIENT_ID")
            }
        
        # Fallback to environment variables
        return {
            "mqtt_host": os.getenv("MQTT_HOST"),
            "mqtt_port": int(os.getenv("MQTT_PORT", "1883")),
            "mqtt_user": os.getenv("MQTT_USER"),
            "mqtt_password": os.getenv("MQTT_PASSWORD"),
            "mqtt_protocol": os.getenv("MQTT_PROTOCOL", "tcp"),
            "mqtt_client_id": os.getenv("MQTT_CLIENT_ID")
        }
    
    async def _load_database_secrets(self, platform_name: str) -> Dict[str, Any]:
        """Load database connection secrets using standardized naming"""
        secret_name = self.generate_secret_name(platform_name, "db")
        
        # Try mounted secret first
        db_secrets = await self._load_mounted_secret(secret_name)
        if db_secrets:
            return {
                "db_host": db_secrets.get("DB_HOST"),
                "db_port": int(db_secrets.get("DB_PORT", "5432")),
                "db_name": db_secrets.get("DB_NAME"),
                "db_user": db_secrets.get("DB_USER"),
                "db_password": db_secrets.get("DB_PASSWORD"),
                "db_ssl_mode": db_secrets.get("DB_SSL_MODE", "prefer"),
                "db_connection_string": db_secrets.get("DB_CONNECTION_STRING")
            }
        
        # Fallback to environment variables
        return {
            "db_host": os.getenv("DB_HOST"),
            "db_port": int(os.getenv("DB_PORT", "5432")),
            "db_name": os.getenv("DB_NAME"),
            "db_user": os.getenv("DB_USER"),
            "db_password": os.getenv("DB_PASSWORD"),
            "db_ssl_mode": os.getenv("DB_SSL_MODE", "prefer"),
            "db_connection_string": os.getenv("DB_CONNECTION_STRING")
        }
    
    async def _load_analytics_secrets(self, platform_name: str) -> Dict[str, Any]:
        """Load analytics dashboard secrets (Metabase) using standardized naming"""
        secret_name = self.generate_secret_name(platform_name, "metabase")
        
        # Try mounted secret first
        analytics_secrets = await self._load_mounted_secret(secret_name)
        if analytics_secrets:
            return {
                "metabase_url": analytics_secrets.get("METABASE_URL"),
                "metabase_user": analytics_secrets.get("METABASE_USER"),
                "metabase_password": analytics_secrets.get("METABASE_PASSWORD"),
                "metabase_api_key": analytics_secrets.get("METABASE_API_KEY"),
                "metabase_database_id": analytics_secrets.get("METABASE_DATABASE_ID")
            }
        
        # Fallback to environment variables
        return {
            "metabase_url": os.getenv("METABASE_URL"),
            "metabase_user": os.getenv("METABASE_USER"),
            "metabase_password": os.getenv("METABASE_PASSWORD"),
            "metabase_api_key": os.getenv("METABASE_API_KEY"),
            "metabase_database_id": os.getenv("METABASE_DATABASE_ID")
        }
    
    async def _load_streaming_secrets(self, platform_name: str) -> Dict[str, Any]:
        """Load stream processing secrets (Lenses) using standardized naming"""
        secret_name = self.generate_secret_name(platform_name, "lenses")
        
        # Try mounted secret first
        streaming_secrets = await self._load_mounted_secret(secret_name)
        if streaming_secrets:
            return {
                "lenses_url": streaming_secrets.get("LENSES_URL"),
                "lenses_user": streaming_secrets.get("LENSES_USER"),
                "lenses_password": streaming_secrets.get("LENSES_PASSWORD"),
                "lenses_api_key": streaming_secrets.get("LENSES_API_KEY"),
                "lenses_ws_url": streaming_secrets.get("LENSES_WS_URL")
            }
        
        # Fallback to environment variables
        return {
            "lenses_url": os.getenv("LENSES_URL"),
            "lenses_user": os.getenv("LENSES_USER"),
            "lenses_password": os.getenv("LENSES_PASSWORD"),
            "lenses_api_key": os.getenv("LENSES_API_KEY"),
            "lenses_ws_url": os.getenv("LENSES_WS_URL")
        }
    
    async def _load_mounted_secret(self, secret_name: str) -> Optional[Dict[str, str]]:
        """
        Load secret from Kubernetes mounted volume
        
        Args:
            secret_name: Name of the secret to load
            
        Returns:
            Dictionary of secret keys and values, or None if not found
        """
        secret_path = Path(self.secret_mount_path) / secret_name
        
        if not secret_path.exists():
            logger.debug(f"Secret path not found: {secret_path}")
            return None
        
        try:
            secrets = {}
            
            # Kubernetes secrets are mounted as individual files
            for secret_file in secret_path.iterdir():
                if secret_file.is_file():
                    key = secret_file.name
                    value = secret_file.read_text().strip()
                    secrets[key] = value
            
            if secrets:
                logger.debug(f"Loaded mounted secret: {secret_name}")
                return secrets
            else:
                logger.warning(f"Empty secret found: {secret_name}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to load mounted secret {secret_name}: {e}")
            return None
    
    async def load_specific_secret(self, secret_name: str) -> Optional[Dict[str, str]]:
        """
        Load a specific secret by name
        
        Args:
            secret_name: Full name of the secret to load
            
        Returns:
            Dictionary of secret keys and values, or None if not found
        """
        return await self._load_mounted_secret(secret_name)
    
    async def inject_secrets_to_webservice(self, webservice_config: Dict[str, Any], secrets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Secret injection capability for webservice components
        
        Args:
            webservice_config: Webservice configuration dictionary
            secrets: List of discovered secrets to inject
            
        Returns:
            Updated webservice configuration with injected secrets
        """
        if not secrets:
            logger.info("No secrets to inject")
            return webservice_config
        
        # Initialize envFrom list if it doesn't exist
        if 'envFrom' not in webservice_config:
            webservice_config['envFrom'] = []
        
        # Add environment variables from each secret
        for secret in secrets:
            secret_ref = {
                'secretRef': {
                    'name': secret['name']
                }
            }
            
            # Add secret reference if not already present
            if secret_ref not in webservice_config['envFrom']:
                webservice_config['envFrom'].append(secret_ref)
                logger.info(f"Injected secret reference: {secret['name']} for service: {secret['service_type']}")
        
        # Add realtime platform discovery annotation
        if 'annotations' not in webservice_config:
            webservice_config['annotations'] = {}
        
        webservice_config['annotations']['realtime.platform.example.org/secrets-injected'] = 'true'
        webservice_config['annotations']['realtime.platform.example.org/injection-timestamp'] = str(int(time.time()))
        
        logger.info(f"Successfully injected {len(secrets)} secret references into webservice configuration")
        return webservice_config
    
    async def create_secret_environment_map(self, secrets: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Create environment variable mapping for all discovered secrets
        
        Args:
            secrets: List of discovered secrets
            
        Returns:
            Dictionary mapping environment variable names to their values
        """
        env_map = {}
        
        for secret in secrets:
            service_type = secret['service_type']
            data = secret['data']
            
            # Create service-specific environment variable prefixes
            prefix = service_type.upper()
            
            for key, value in data.items():
                env_var_name = f"{prefix}_{key}"
                env_map[env_var_name] = value
        
        logger.info(f"Created environment map with {len(env_map)} variables from {len(secrets)} secrets")
        return env_map
    
    def validate_required_secrets(self, secrets: Dict[str, Any], required_services: List[str]) -> bool:
        """
        Validate that all required secrets are present
        
        Args:
            secrets: Dictionary of loaded secrets
            required_services: List of required services ('kafka', 'mqtt', 'database', etc.)
            
        Returns:
            True if all required secrets are present
        """
        validation_map = {
            'kafka': ['kafka_bootstrap_servers'],
            'mqtt': ['mqtt_host', 'mqtt_port'],
            'database': ['db_host', 'db_name', 'db_user'],
            'analytics': ['metabase_url'],
            'streaming': ['lenses_url']
        }
        
        for service in required_services:
            if service not in validation_map:
                logger.warning(f"Unknown service for validation: {service}")
                continue
            
            required_fields = validation_map[service]
            for field in required_fields:
                if not secrets.get(field):
                    logger.error(f"Missing required secret for {service}: {field}")
                    return False
        
        logger.info(f"All required secrets present for services: {required_services}")
        return True
    
    def get_connection_string(self, secrets: Dict[str, Any], service_type: str) -> Optional[str]:
        """
        Generate connection string for a service
        
        Args:
            secrets: Dictionary of loaded secrets
            service_type: Type of service ('database', 'kafka', 'mqtt')
            
        Returns:
            Connection string or None if cannot be generated
        """
        if service_type == 'database':
            if all(secrets.get(k) for k in ['db_host', 'db_port', 'db_name', 'db_user', 'db_password']):
                return f"postgresql://{secrets['db_user']}:{secrets['db_password']}@{secrets['db_host']}:{secrets['db_port']}/{secrets['db_name']}"
        
        elif service_type == 'kafka':
            return secrets.get('kafka_bootstrap_servers')
        
        elif service_type == 'mqtt':
            if secrets.get('mqtt_host') and secrets.get('mqtt_port'):
                protocol = secrets.get('mqtt_protocol', 'tcp')
                return f"{protocol}://{secrets['mqtt_host']}:{secrets['mqtt_port']}"
        
        return None


# Convenience function for quick secret loading
async def load_realtime_platform_secrets(platform_name: str) -> Dict[str, Any]:
    """
    Convenience function to load all secrets for a realtime platform
    
    Args:
        platform_name: Name of the realtime platform
        
    Returns:
        Dictionary containing all connection details
    """
    loader = PlatformSecretLoader(platform_name)
    return await loader.load_platform_secrets()


# Auto-configuration helper
def configure_agent_from_secrets(config: 'AgentConfig', secrets: Dict[str, Any]) -> 'AgentConfig':
    """
    Auto-configure AgentConfig from loaded secrets
    
    Args:
        config: Existing AgentConfig instance
        secrets: Dictionary of loaded secrets
        
    Returns:
        Updated AgentConfig instance
    """
    # Update Kafka settings
    if secrets.get('kafka_bootstrap_servers'):
        config.kafka_bootstrap_servers = secrets['kafka_bootstrap_servers']
    if secrets.get('kafka_schema_registry_url'):
        config.kafka_schema_registry_url = secrets['kafka_schema_registry_url']
    
    # Update MQTT settings
    if secrets.get('mqtt_host'):
        config.mqtt_host = secrets['mqtt_host']
    if secrets.get('mqtt_port'):
        config.mqtt_port = secrets['mqtt_port']
    if secrets.get('mqtt_user'):
        config.mqtt_user = secrets['mqtt_user']
    if secrets.get('mqtt_password'):
        config.mqtt_password = secrets['mqtt_password']
    
    # Update database settings
    if secrets.get('db_host'):
        config.db_host = secrets['db_host']
    if secrets.get('db_port'):
        config.db_port = secrets['db_port']
    if secrets.get('db_name'):
        config.db_name = secrets['db_name']
    if secrets.get('db_user'):
        config.db_user = secrets['db_user']
    if secrets.get('db_password'):
        config.db_password = secrets['db_password']
    
    # Update analytics settings
    if secrets.get('metabase_url'):
        config.metabase_url = secrets['metabase_url']
    if secrets.get('metabase_user'):
        config.metabase_user = secrets['metabase_user']
    if secrets.get('metabase_password'):
        config.metabase_password = secrets['metabase_password']
    
    # Update streaming settings
    if secrets.get('lenses_url'):
        config.lenses_url = secrets['lenses_url']
    if secrets.get('lenses_user'):
        config.lenses_user = secrets['lenses_user']
    if secrets.get('lenses_password'):
        config.lenses_password = secrets['lenses_password']
    
    logger.info(f"AgentConfig updated with platform secrets")
    return config


# Enhanced convenience functions for webservice integration
async def load_and_inject_realtime_secrets(webservice_name: str, realtime_platform_name: str) -> Dict[str, Any]:
    """
    Convenience function to discover and inject realtime platform secrets into a webservice
    
    Args:
        webservice_name: Name of the webservice component
        realtime_platform_name: Name of the realtime platform to discover secrets from
        
    Returns:
        Dictionary containing injected configuration and discovered secrets
    """
    loader = PlatformSecretLoader()
    
    # Discover realtime platform secrets
    discovered_secrets = await loader.discover_realtime_platform_secrets(realtime_platform_name)
    
    if not discovered_secrets:
        logger.warning(f"No secrets discovered for realtime platform: {realtime_platform_name}")
        return {
            'webservice_config': {},
            'discovered_secrets': [],
            'injection_successful': False
        }
    
    # Create basic webservice configuration
    webservice_config = {
        'envFrom': [],
        'annotations': {
            'realtime.platform.example.org/integration': realtime_platform_name,
            'webservice.example.org/name': webservice_name
        }
    }
    
    # Inject secrets into webservice configuration
    updated_config = await loader.inject_secrets_to_webservice(webservice_config, discovered_secrets)
    
    return {
        'webservice_config': updated_config,
        'discovered_secrets': discovered_secrets,
        'injection_successful': True,
        'secret_count': len(discovered_secrets)
    }


async def validate_webservice_realtime_integration(webservice_name: str, realtime_platform_name: str) -> Dict[str, Any]:
    """
    Validate that a webservice can successfully integrate with a realtime platform
    
    Args:
        webservice_name: Name of the webservice component
        realtime_platform_name: Name of the realtime platform
        
    Returns:
        Validation results with status and recommendations
    """
    loader = PlatformSecretLoader()
    
    # Discover available secrets
    discovered_secrets = await loader.discover_realtime_platform_secrets(realtime_platform_name)
    
    validation_results = {
        'webservice': webservice_name,
        'realtime_platform': realtime_platform_name,
        'validation_timestamp': int(time.time()),
        'status': 'unknown',
        'available_services': [],
        'missing_services': [],
        'recommendations': []
    }
    
    # Check for essential realtime platform services
    essential_services = ['kafka', 'mqtt', 'db']
    discovered_service_types = [secret['service_type'] for secret in discovered_secrets]
    
    validation_results['available_services'] = discovered_service_types
    validation_results['missing_services'] = [service for service in essential_services if service not in discovered_service_types]
    
    # Determine validation status
    if len(discovered_secrets) == 0:
        validation_results['status'] = 'failed'
        validation_results['recommendations'].append(f"No secrets found for realtime platform '{realtime_platform_name}'. Ensure the platform is deployed and secrets are created.")
    elif len(validation_results['missing_services']) > 0:
        validation_results['status'] = 'partial'
        validation_results['recommendations'].append(f"Missing essential services: {', '.join(validation_results['missing_services'])}")
    else:
        validation_results['status'] = 'ready'
        validation_results['recommendations'].append("All essential services available for integration")
    
    # Add service-specific recommendations
    if 'kafka' in discovered_service_types:
        validation_results['recommendations'].append("Kafka available: Enable event streaming and message processing")
    if 'mqtt' in discovered_service_types:
        validation_results['recommendations'].append("MQTT available: Enable IoT device communication")
    if 'db' in discovered_service_types:
        validation_results['recommendations'].append("Database available: Enable persistent data storage")
    
    logger.info(f"Validation completed for {webservice_name} → {realtime_platform_name}: {validation_results['status']}")
    return validation_results


def generate_webservice_secret_configuration(realtime_platform_name: str) -> Dict[str, Any]:
    """
    Generate Kubernetes secret configuration for webservice integration
    
    Args:
        realtime_platform_name: Name of the realtime platform
        
    Returns:
        Kubernetes configuration for secret discovery and injection
    """
    return {
        'apiVersion': 'v1',
        'kind': 'Secret',
        'metadata': {
            'name': f'webservice-{realtime_platform_name}-integration',
            'labels': {
                'app.kubernetes.io/managed-by': 'webservice-secret-injector',
                'realtime.platform.example.org/integration': realtime_platform_name,
                'app.kubernetes.io/component': 'secret-integration'
            },
            'annotations': {
                'realtime.platform.example.org/source-platform': realtime_platform_name,
                'webservice.example.org/secret-type': 'cross-component-integration',
                'webservice.example.org/discovery-pattern': f'{realtime_platform_name}-*-secret'
            }
        },
        'type': 'Opaque',
        'data': {
            'REALTIME_PLATFORM_NAME': realtime_platform_name,
            'INTEGRATION_ENABLED': 'true',
            'SECRET_DISCOVERY_PATTERN': f'{realtime_platform_name}-*-secret'
        }
    }