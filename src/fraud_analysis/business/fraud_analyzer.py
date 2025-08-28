"""
Lógica de negócios para análise de fraude.
"""

import logging
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, sum as spark_sum, desc
from typing import Tuple

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class FraudAnalyzer:
    """Classe responsável pela lógica de negócios de análise de fraude."""
    
    def __init__(self):
        """Inicializa o analisador de fraude."""
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def filter_legitimate_refused_payments(self, pagamentos_df: DataFrame) -> DataFrame:
        """
        Filtra pagamentos recusados mas classificados como legítimos.
        
        Args:
            pagamentos_df (DataFrame): DataFrame de pagamentos
            
        Returns:
            DataFrame: DataFrame filtrado
        """
        try:
            self.logger.info("Iniciando filtro de pagamentos recusados mas legítimos")
            
            # Filtro: status=false (recusado) e fraude=false (legítimo)
            filtered_df = pagamentos_df.filter(
                (col("status") == False) & 
                (col("avaliacao_fraude.fraude") == False)
            )
            
            count = filtered_df.count()
            self.logger.info(f"Encontrados {count} pagamentos recusados mas legítimos")
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Erro ao filtrar pagamentos: {str(e)}")
            raise
    
    def filter_target_year(self, df: DataFrame, date_column: str, 
                          target_year: int) -> DataFrame:
        """
        Filtra dados do ano alvo.
        
        Args:
            df (DataFrame): DataFrame a ser filtrado
            date_column (str): Nome da coluna de data
            target_year (int): Ano alvo
            
        Returns:
            DataFrame: DataFrame filtrado por ano
        """
        try:
            self.logger.info(f"Filtrando dados do ano {target_year}")
            
            filtered_df = df.filter(year(col(date_column)) == target_year)
            
            count = filtered_df.count()
            self.logger.info(f"Encontrados {count} registros para o ano {target_year}")
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Erro ao filtrar por ano: {str(e)}")
            raise
    
    def join_payment_and_order_data(self, pagamentos_df: DataFrame, 
                                   pedidos_df: DataFrame) -> DataFrame:
        """
        Faz join entre dados de pagamentos e pedidos.
        
        Args:
            pagamentos_df (DataFrame): DataFrame de pagamentos
            pedidos_df (DataFrame): DataFrame de pedidos
            
        Returns:
            DataFrame: DataFrame com dados unidos
        """
        try:
            self.logger.info("Iniciando join entre pagamentos e pedidos")
            
            # Usar alias para evitar ambiguidade nas colunas
            pagamentos_alias = pagamentos_df.alias("pag")
            pedidos_alias = pedidos_df.alias("ped")
            
            # Join usando ID_PEDIDO como chave
            joined_df = pagamentos_alias.join(
                pedidos_alias,
                pagamentos_alias.id_pedido == pedidos_alias.ID_PEDIDO,
                "inner"
            )
            
            count = joined_df.count()
            self.logger.info(f"Join resultou em {count} registros")
            
            return joined_df
            
        except Exception as e:
            self.logger.error(f"Erro no join de dados: {str(e)}")
            raise
    
    def calculate_order_totals(self, pedidos_df: DataFrame) -> DataFrame:
        """
        Calcula o valor total dos pedidos agrupando por ID_PEDIDO.
        
        Args:
            pedidos_df (DataFrame): DataFrame de pedidos
            
        Returns:
            DataFrame: DataFrame com valores totais por pedido
        """
        try:
            self.logger.info("Calculando valores totais dos pedidos")
            
            # Calcula valor total: VALOR_UNITARIO * QUANTIDADE
            pedidos_with_total = pedidos_df.withColumn(
                "valor_total_item",
                col("VALOR_UNITARIO") * col("QUANTIDADE")
            )
            
            # Agrupa por pedido e soma os valores
            totals_df = (pedidos_with_total
                        .groupBy("ID_PEDIDO", "UF", "DATA_CRIACAO")
                        .agg(spark_sum("valor_total_item").alias("valor_total_pedido")))
            
            count = totals_df.count()
            self.logger.info(f"Calculados totais para {count} pedidos únicos")
            
            return totals_df
            
        except Exception as e:
            self.logger.error(f"Erro ao calcular totais: {str(e)}")
            raise
    
    def create_fraud_report(self, joined_df: DataFrame) -> DataFrame:
        """
        Cria o relatório final de fraude com as colunas solicitadas.
        
        Args:
            joined_df (DataFrame): DataFrame com dados unidos
            
        Returns:
            DataFrame: Relatório final
        """
        try:
            self.logger.info("Criando relatório final de fraude")
            
            # Seleciona e renomeia colunas conforme especificado
            # Usar alias específicos para evitar ambiguidade
            report_df = joined_df.select(
                col("pag.id_pedido"),
                col("ped.UF").alias("uf"),
                col("pag.forma_pagamento"),
                col("ped.valor_total_pedido"),
                col("ped.DATA_CRIACAO").alias("data_pedido")
            )
            
            count = report_df.count()
            self.logger.info(f"Relatório criado com {count} registros")
            
            return report_df
            
        except Exception as e:
            self.logger.error(f"Erro ao criar relatório: {str(e)}")
            raise
    
    def sort_report(self, report_df: DataFrame) -> DataFrame:
        """
        Ordena o relatório conforme especificado: UF, forma_pagamento, data_pedido.
        
        Args:
            report_df (DataFrame): DataFrame do relatório
            
        Returns:
            DataFrame: Relatório ordenado
        """
        try:
            self.logger.info("Ordenando relatório final")
            
            sorted_df = report_df.orderBy("uf", "forma_pagamento", "data_pedido")
            
            self.logger.info("Relatório ordenado com sucesso")
            
            return sorted_df
            
        except Exception as e:
            self.logger.error(f"Erro ao ordenar relatório: {str(e)}")
            raise
    
    def validate_data_quality(self, df: DataFrame, dataset_name: str) -> None:
        """
        Valida a qualidade dos dados.
        
        Args:
            df (DataFrame): DataFrame a ser validado
            dataset_name (str): Nome do dataset para log
        """
        try:
            self.logger.info(f"Validando qualidade dos dados: {dataset_name}")
            
            total_count = df.count()
            if total_count == 0:
                raise ValueError(f"Dataset {dataset_name} está vazio")
            
            self.logger.info(f"Dataset {dataset_name} validado: {total_count} registros")
            
        except Exception as e:
            self.logger.error(f"Erro na validação de dados {dataset_name}: {str(e)}")
            raise
