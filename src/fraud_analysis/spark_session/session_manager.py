"""
Gerenciamento da sessão Spark.
"""

from pyspark.sql import SparkSession
from typing import Dict, Any, Optional


class SparkSessionManager:
    """Classe responsável pelo gerenciamento da sessão Spark."""
    
    def __init__(self, app_name: str, config: Dict[str, Any] = None):
        """
        Inicializa o gerenciador de sessão Spark.
        
        Args:
            app_name (str): Nome da aplicação Spark
            config (Dict[str, Any], optional): Configurações do Spark
        """
        self.app_name = app_name
        self.config = config or {}
        self._spark_session: Optional[SparkSession] = None
    
    def create_session(self) -> SparkSession:
        """
        Cria e configura a sessão Spark.
        
        Returns:
            SparkSession: Sessão Spark configurada
        """
        if self._spark_session is not None:
            return self._spark_session
        
        builder = SparkSession.builder.appName(self.app_name)
        
        # Aplica configurações personalizadas
        for key, value in self.config.items():
            builder = builder.config(key, value)
        
        self._spark_session = builder.getOrCreate()
        
        # Configura log level para reduzir verbosidade
        self._spark_session.sparkContext.setLogLevel("WARN")
        
        return self._spark_session
    
    def get_session(self) -> SparkSession:
        """
        Retorna a sessão Spark existente ou cria uma nova.
        
        Returns:
            SparkSession: Sessão Spark
        """
        if self._spark_session is None:
            return self.create_session()
        return self._spark_session
    
    def stop_session(self) -> None:
        """Para a sessão Spark."""
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None
    
    def __enter__(self):
        """Context manager entry."""
        return self.create_session()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_session()
