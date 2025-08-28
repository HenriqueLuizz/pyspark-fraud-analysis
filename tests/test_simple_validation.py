"""
Testes simples de validação das classes sem dependência do Spark.
"""

import pytest
import sys
from pathlib import Path

# Adiciona o diretório src ao path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.fraud_analysis.config.app_config import AppConfig
from src.fraud_analysis.schemas.data_schemas import DataSchemas


class TestAppConfig:
    """Testes para a classe AppConfig."""
    
    def test_app_config_initialization(self):
        """Testa a inicialização da configuração."""
        config = AppConfig()
        
        assert config.target_year == 2025
        assert config.app_name == "FraudAnalysisApp"
        assert config.output_format == "parquet"
        assert config.spark_config is not None
    
    def test_path_methods(self):
        """Testa os métodos de caminho."""
        config = AppConfig()
        
        pagamentos_path = config.get_pagamentos_full_path()
        pedidos_path = config.get_pedidos_full_path()
        output_path = config.get_output_full_path()
        
        assert "data/dataset-json-pagamentos/data/pagamentos" in pagamentos_path
        assert "data/datasets-csv-pedidos/data/pedidos" in pedidos_path
        assert "data/output" in output_path
    
    def test_year_filter(self):
        """Testa o filtro por ano."""
        config = AppConfig()
        year_filter = config.get_target_year_filter()
        
        assert year_filter == "*2025*"


class TestDataSchemas:
    """Testes para a classe DataSchemas."""
    
    def test_pagamentos_schema(self):
        """Testa o schema de pagamentos."""
        schema = DataSchemas.get_pagamentos_schema()
        
        field_names = [field.name for field in schema.fields]
        
        expected_fields = [
            "id_pedido", "forma_pagamento", "valor_pagamento", 
            "status", "data_processamento", "avaliacao_fraude"
        ]
        
        for field in expected_fields:
            assert field in field_names
    
    def test_pedidos_schema(self):
        """Testa o schema de pedidos."""
        schema = DataSchemas.get_pedidos_schema()
        
        field_names = [field.name for field in schema.fields]
        
        expected_fields = [
            "ID_PEDIDO", "PRODUTO", "VALOR_UNITARIO", 
            "QUANTIDADE", "DATA_CRIACAO", "UF", "ID_CLIENTE"
        ]
        
        for field in expected_fields:
            assert field in field_names
    
    def test_fraud_report_schema(self):
        """Testa o schema do relatório de fraude."""
        schema = DataSchemas.get_fraud_report_schema()
        
        field_names = [field.name for field in schema.fields]
        
        expected_fields = [
            "id_pedido", "uf", "forma_pagamento", 
            "valor_total_pedido", "data_pedido"
        ]
        
        for field in expected_fields:
            assert field in field_names
        
        assert len(field_names) == 5


def test_import_all_modules():
    """Testa se todos os módulos podem ser importados corretamente."""
    
    # Testa imports das classes principais
    from src.fraud_analysis.config.app_config import AppConfig
    from src.fraud_analysis.spark_session.session_manager import SparkSessionManager
    from src.fraud_analysis.schemas.data_schemas import DataSchemas
    from src.fraud_analysis.io.data_reader import DataReader
    from src.fraud_analysis.io.data_writer import DataWriter
    from src.fraud_analysis.business.fraud_analyzer import FraudAnalyzer
    from src.fraud_analysis.pipeline.fraud_pipeline import FraudAnalysisPipeline
    
    # Se chegou até aqui, todos os imports funcionaram
    assert True


def test_project_structure():
    """Testa se a estrutura do projeto está correta."""
    base_path = Path(__file__).parent.parent
    
    # Testa se diretórios principais existem
    assert (base_path / "src" / "fraud_analysis").exists()
    assert (base_path / "data").exists()
    assert (base_path / "tests").exists()
    
    # Testa se arquivos de configuração existem
    assert (base_path / "main.py").exists()
    assert (base_path / "requirements.txt").exists()
    assert (base_path / "pyproject.toml").exists()
    assert (base_path / "README.md").exists()
    assert (base_path / "MANIFEST.in").exists()
    
    # Testa se submodules existem
    assert (base_path / "data" / "dataset-json-pagamentos").exists()
    assert (base_path / "data" / "datasets-csv-pedidos").exists()
