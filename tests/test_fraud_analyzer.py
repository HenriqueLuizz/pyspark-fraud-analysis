"""
Testes unitários para a classe FraudAnalyzer.
"""

import pytest
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, TimestampType, IntegerType
from datetime import datetime

# Adiciona o diretório src ao path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.fraud_analysis.business.fraud_analyzer import FraudAnalyzer


class TestFraudAnalyzer:
    """Testes para a classe FraudAnalyzer."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Fixture para criar sessão Spark para testes."""
        spark = SparkSession.builder \
            .appName("FraudAnalyzerTest") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def fraud_analyzer(self):
        """Fixture para criar instância do FraudAnalyzer."""
        return FraudAnalyzer()
    
    @pytest.fixture
    def sample_payments_data(self, spark):
        """Fixture com dados de exemplo para pagamentos."""
        schema = StructType([
            StructField("id_pedido", StringType(), False),
            StructField("forma_pagamento", StringType(), False),
            StructField("valor_pagamento", DoubleType(), False),
            StructField("status", BooleanType(), False),
            StructField("data_processamento", TimestampType(), False),
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), False),
                StructField("score", DoubleType(), False)
            ]), False)
        ])
        
        data = [
            ("123", "PIX", 1000.0, False, datetime(2025, 1, 15), {"fraude": False, "score": 0.1}),  # Recusado legítimo
            ("456", "CARTAO_CREDITO", 2000.0, True, datetime(2025, 1, 16), {"fraude": False, "score": 0.2}),  # Aprovado
            ("789", "BOLETO", 1500.0, False, datetime(2025, 1, 17), {"fraude": True, "score": 0.9}),  # Recusado fraudulento
            ("101", "PIX", 800.0, False, datetime(2024, 12, 15), {"fraude": False, "score": 0.15}),  # Ano errado
        ]
        
        return spark.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_orders_data(self, spark):
        """Fixture com dados de exemplo para pedidos."""
        schema = StructType([
            StructField("ID_PEDIDO", StringType(), False),
            StructField("PRODUTO", StringType(), False),
            StructField("VALOR_UNITARIO", DoubleType(), False),
            StructField("QUANTIDADE", IntegerType(), False),
            StructField("DATA_CRIACAO", TimestampType(), False),
            StructField("UF", StringType(), False),
            StructField("ID_CLIENTE", IntegerType(), False)
        ])
        
        data = [
            ("123", "PRODUTO_A", 500.0, 2, datetime(2025, 1, 15), "SP", 1001),
            ("456", "PRODUTO_B", 1000.0, 2, datetime(2025, 1, 16), "RJ", 1002),
            ("789", "PRODUTO_C", 750.0, 2, datetime(2025, 1, 17), "MG", 1003),
            ("101", "PRODUTO_D", 400.0, 2, datetime(2024, 12, 15), "RS", 1004),
        ]
        
        return spark.createDataFrame(data, schema)
    
    def test_filter_legitimate_refused_payments(self, fraud_analyzer, sample_payments_data):
        """Testa o filtro de pagamentos recusados mas legítimos."""
        result = fraud_analyzer.filter_legitimate_refused_payments(sample_payments_data)
        
        # Deve retornar apenas 1 registro (id_pedido = "123")
        assert result.count() == 1
        
        # Verifica se é o registro correto
        row = result.collect()[0]
        assert row.id_pedido == "123"
        assert row.status == False
        assert row.avaliacao_fraude.fraude == False
    
    def test_filter_target_year(self, fraud_analyzer, sample_payments_data):
        """Testa o filtro por ano alvo."""
        result = fraud_analyzer.filter_target_year(
            sample_payments_data, "data_processamento", 2025
        )
        
        # Deve retornar 3 registros (excluindo o de 2024)
        assert result.count() == 3
        
        # Verifica se todos os registros são de 2025
        for row in result.collect():
            assert row.data_processamento.year == 2025
    
    def test_join_payment_and_order_data(self, fraud_analyzer, sample_payments_data, sample_orders_data):
        """Testa o join entre pagamentos e pedidos."""
        result = fraud_analyzer.join_payment_and_order_data(
            sample_payments_data, sample_orders_data
        )
        
        # Deve ter 4 registros (todos os IDs coincidem)
        assert result.count() == 4
        
        # Verifica se contém colunas de ambas as tabelas
        columns = result.columns
        assert "id_pedido" in columns
        assert "forma_pagamento" in columns
        assert "UF" in columns
        assert "PRODUTO" in columns
    
    def test_calculate_order_totals(self, fraud_analyzer, sample_orders_data):
        """Testa o cálculo de totais dos pedidos."""
        result = fraud_analyzer.calculate_order_totals(sample_orders_data)
        
        # Deve ter 4 registros (um por pedido)
        assert result.count() == 4
        
        # Verifica se contém a coluna valor_total_pedido
        assert "valor_total_pedido" in result.columns
        
        # Verifica se os cálculos estão corretos
        totals = {row.ID_PEDIDO: row.valor_total_pedido for row in result.collect()}
        assert totals["123"] == 1000.0  # 500 * 2
        assert totals["456"] == 2000.0  # 1000 * 2
        assert totals["789"] == 1500.0  # 750 * 2
        assert totals["101"] == 800.0   # 400 * 2
    
    def test_validate_data_quality_with_data(self, fraud_analyzer, sample_payments_data):
        """Testa validação de qualidade com dados válidos."""
        # Não deve levantar exceção
        fraud_analyzer.validate_data_quality(sample_payments_data, "test_dataset")
    
    def test_validate_data_quality_empty_data(self, fraud_analyzer, spark):
        """Testa validação de qualidade com dados vazios."""
        empty_df = spark.createDataFrame([], StructType([
            StructField("test_col", StringType(), True)
        ]))
        
        # Deve levantar ValueError
        with pytest.raises(ValueError, match="Dataset .* está vazio"):
            fraud_analyzer.validate_data_quality(empty_df, "empty_dataset")
    
    def test_create_fraud_report(self, fraud_analyzer, sample_payments_data, sample_orders_data):
        """Testa a criação do relatório de fraude."""
        # Primeiro calcula totais
        orders_with_totals = fraud_analyzer.calculate_order_totals(sample_orders_data)
        
        # Depois faz o join
        joined = fraud_analyzer.join_payment_and_order_data(
            sample_payments_data, orders_with_totals
        )
        
        # Cria o relatório
        result = fraud_analyzer.create_fraud_report(joined)
        
        # Verifica colunas esperadas
        expected_columns = ["id_pedido", "uf", "forma_pagamento", "valor_total_pedido", "data_pedido"]
        assert result.columns == expected_columns
        
        # Verifica se mantém o número de registros
        assert result.count() == 4
    
    def test_sort_report(self, fraud_analyzer, spark):
        """Testa a ordenação do relatório."""
        # Dados de teste para ordenação
        schema = StructType([
            StructField("id_pedido", StringType(), False),
            StructField("uf", StringType(), False),
            StructField("forma_pagamento", StringType(), False),
            StructField("valor_total_pedido", DoubleType(), False),
            StructField("data_pedido", TimestampType(), False)
        ])
        
        data = [
            ("1", "SP", "PIX", 1000.0, datetime(2025, 1, 20)),
            ("2", "RJ", "CARTAO_CREDITO", 2000.0, datetime(2025, 1, 15)),
            ("3", "SP", "BOLETO", 1500.0, datetime(2025, 1, 10)),
            ("4", "RJ", "PIX", 800.0, datetime(2025, 1, 25)),
        ]
        
        df = spark.createDataFrame(data, schema)
        result = fraud_analyzer.sort_report(df)
        
        # Coleta resultados ordenados
        sorted_data = result.collect()
        
        # Verifica ordenação: RJ deve vir antes de SP
        assert sorted_data[0].uf == "RJ"
        assert sorted_data[1].uf == "RJ"
        assert sorted_data[2].uf == "SP"
        assert sorted_data[3].uf == "SP"
        
        # Dentro do mesmo estado, verifica ordenação por forma_pagamento e data
        rj_records = [row for row in sorted_data if row.uf == "RJ"]
        assert rj_records[0].forma_pagamento == "CARTAO_CREDITO"
        assert rj_records[1].forma_pagamento == "PIX"
