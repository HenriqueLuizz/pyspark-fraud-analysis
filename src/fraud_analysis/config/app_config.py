"""
Configurações centralizadas da aplicação de análise de fraude.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any


@dataclass
class AppConfig:
    """Classe de configuração centralizada da aplicação."""
    
    # Configurações de caminhos
    base_path: Path = Path.cwd()
    pagamentos_path: str = "data/dataset-json-pagamentos/data/pagamentos"
    pedidos_path: str = "data/datasets-csv-pedidos/data/pedidos"
    output_path: str = "data/output"
    
    # Configurações de arquivo
    target_year: int = 2025
    output_format: str = "parquet"
    
    # Configurações do Spark
    app_name: str = "FraudAnalysisApp"
    spark_config: Dict[str, Any] = None
    
    def __post_init__(self):
        """Inicialização pós-construção."""
        if self.spark_config is None:
            self.spark_config = {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            }
    
    def get_pagamentos_full_path(self) -> str:
        """
        Retorna o caminho completo para os dados de pagamentos.
        
        Returns:
            str: Caminho completo para dados de pagamentos
        """
        return str(self.base_path / self.pagamentos_path)
    
    def get_pedidos_full_path(self) -> str:
        """
        Retorna o caminho completo para os dados de pedidos.
        
        Returns:
            str: Caminho completo para dados de pedidos
        """
        return str(self.base_path / self.pedidos_path)
    
    def get_output_full_path(self) -> str:
        """
        Retorna o caminho completo para os dados de saída.
        
        Returns:
            str: Caminho completo para dados de saída
        """
        return str(self.base_path / self.output_path)
    
    def get_target_year_filter(self) -> str:
        """
        Retorna filtro para arquivos do ano alvo.
        
        Returns:
            str: Padrão de filtro para o ano alvo
        """
        return f"*{self.target_year}*"
