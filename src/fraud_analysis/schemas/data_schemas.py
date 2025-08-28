"""
Schemas explícitos para os DataFrames do projeto de análise de fraude.
Todos os schemas são definidos explicitamente sem inferência.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, TimestampType, IntegerType
)


class DataSchemas:
    """Classe responsável por definir todos os schemas dos DataFrames."""
    
    @staticmethod
    def get_pagamentos_schema() -> StructType:
        """
        Schema para o DataFrame de pagamentos (JSON).
        
        Returns:
            StructType: Schema explícito para dados de pagamentos
        """
        return StructType([
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
    
    @staticmethod
    def get_pedidos_schema() -> StructType:
        """
        Schema para o DataFrame de pedidos (CSV).
        
        Returns:
            StructType: Schema explícito para dados de pedidos
        """
        return StructType([
            StructField("ID_PEDIDO", StringType(), False),
            StructField("PRODUTO", StringType(), False),
            StructField("VALOR_UNITARIO", DoubleType(), False),
            StructField("QUANTIDADE", IntegerType(), False),
            StructField("DATA_CRIACAO", TimestampType(), False),
            StructField("UF", StringType(), False),
            StructField("ID_CLIENTE", IntegerType(), False)
        ])
    
    @staticmethod
    def get_fraud_report_schema() -> StructType:
        """
        Schema para o DataFrame do relatório final de fraude.
        
        Returns:
            StructType: Schema explícito para o relatório de fraude
        """
        return StructType([
            StructField("id_pedido", StringType(), False),
            StructField("uf", StringType(), False),
            StructField("forma_pagamento", StringType(), False),
            StructField("valor_total_pedido", DoubleType(), False),
            StructField("data_pedido", TimestampType(), False)
        ])
