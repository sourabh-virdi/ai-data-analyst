"""
Configuration Manager for AI Data Analyst

Handles loading and accessing configuration from YAML files with
environment variable substitution.
"""

import os
import re
import yaml
from typing import Any, Dict, Optional
from pathlib import Path


class ConfigManager:
    """Configuration manager with environment variable support"""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Substitute environment variables
        content = self._substitute_env_vars(content)
        
        return yaml.safe_load(content)
    
    def _substitute_env_vars(self, content: str) -> str:
        """Substitute environment variables in configuration content"""
        
        # Pattern to match ${VAR_NAME} or ${VAR_NAME:default_value}
        pattern = r'\$\{([^}]+)\}'
        
        def replace_var(match):
            var_expr = match.group(1)
            
            if ':' in var_expr:
                var_name, default_value = var_expr.split(':', 1)
                return os.getenv(var_name.strip(), default_value.strip())
            else:
                var_name = var_expr.strip()
                value = os.getenv(var_name)
                if value is None:
                    raise ValueError(f"Environment variable {var_name} not found and no default provided")
                return value
        
        return re.sub(pattern, replace_var, content)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value using dot notation (e.g., 'server.port')"""
        
        keys = key.split('.')
        value = self._config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """Get entire configuration section"""
        return self.get(section, {})
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value using dot notation"""
        
        keys = key.split('.')
        config = self._config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def reload(self) -> None:
        """Reload configuration from file"""
        self._config = self._load_config()
    
    @property
    def config(self) -> Dict[str, Any]:
        """Get the entire configuration dictionary"""
        return self._config.copy()
    
    def validate_required_keys(self, required_keys: list) -> None:
        """Validate that required configuration keys are present"""
        
        missing_keys = []
        
        for key in required_keys:
            if self.get(key) is None:
                missing_keys.append(key)
        
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")
    
    def get_database_config(self, db_name: str) -> Optional[Dict[str, Any]]:
        """Get database configuration for a specific database"""
        
        db_config = self.get(f"databases.{db_name}")
        
        if db_config and db_config.get("enabled", False):
            return db_config
        
        return None
    
    def get_enabled_databases(self) -> Dict[str, Dict[str, Any]]:
        """Get all enabled database configurations"""
        
        databases = self.get("databases", {})
        enabled_dbs = {}
        
        for db_name, db_config in databases.items():
            if db_config.get("enabled", False):
                enabled_dbs[db_name] = db_config
        
        return enabled_dbs 