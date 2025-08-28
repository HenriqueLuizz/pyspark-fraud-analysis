"""
Orquestração do pipeline de análise de fraude.
"""

import logging
from pyspark.sql import DataFrame
from typing import Dict, Any

from ..config.app_config import AppConfig
from ..spark_session.session_manager import SparkSessionManager
from ..schemas.data_schemas import DataSchemas
from ..io.data_reader import DataReader
from ..io.data_writer import DataWriter
from ..business.fraud_analyzer import FraudAnalyzer


class FraudAnalysisPipeline:
    """Classe responsável pela orquestração do pipeline de análise de fraude."""
    
    def __init__(self, config: AppConfig, session_manager: SparkSessionManager,
                 data_reader: DataReader, data_writer: DataWriter,
                 fraud_analyzer: FraudAnalyzer):
        """
        Inicializa o pipeline de análise de fraude.
        
        Args:
            config (AppConfig): Configurações da aplicação
            session_manager (SparkSessionManager): Gerenciador de sessão Spark
            data_reader (DataReader): Leitor de dados
            data_writer (DataWriter): Escritor de dados
            fraud_analyzer (FraudAnalyzer): Analisador de fraude
        """
        self.config = config
        self.session_manager = session_manager
        self.data_reader = data_reader
        self.data_writer = data_writer
        self.fraud_analyzer = fraud_analyzer
        self.schemas = DataSchemas()
        
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract_data(self) -> Dict[str, DataFrame]:
        """
        Extrai dados de pagamentos e pedidos.
        
        Returns:
            Dict[str, DataFrame]: Dicionário com DataFrames extraídos
        """
        try:
            self.logger.info("=== FASE DE EXTRAÇÃO ===")
            
            # Extrai dados de pagamentos
            self.logger.info("Extraindo dados de pagamentos...")
            pagamentos_df = self.data_reader.read_json_files(
                path=self.config.get_pagamentos_full_path(),
                schema=self.schemas.get_pagamentos_schema(),
                year_filter=str(self.config.target_year)
            )
            
            # Extrai dados de pedidos
            self.logger.info("Extraindo dados de pedidos...")
            pedidos_df = self.data_reader.read_csv_files(
                path=self.config.get_pedidos_full_path(),
                schema=self.schemas.get_pedidos_schema(),
                year_filter=str(self.config.target_year)
            )
            
            # Valida qualidade dos dados
            self.fraud_analyzer.validate_data_quality(pagamentos_df, "pagamentos")
            self.fraud_analyzer.validate_data_quality(pedidos_df, "pedidos")
            
            self.logger.info("Extração de dados concluída com sucesso")
            
            return {
                "pagamentos": pagamentos_df,
                "pedidos": pedidos_df
            }
            
        except Exception as e:
            self.logger.error(f"Erro na extração de dados: {str(e)}")
            raise
    
    def transform_data(self, dataframes: Dict[str, DataFrame]) -> DataFrame:
        """
        Transforma os dados conforme regras de negócio.
        
        Args:
            dataframes (Dict[str, DataFrame]): DataFrames extraídos
            
        Returns:
            DataFrame: DataFrame transformado para o relatório
        """
        try:
            self.logger.info("=== FASE DE TRANSFORMAÇÃO ===")
            
            pagamentos_df = dataframes["pagamentos"]
            pedidos_df = dataframes["pedidos"]
            
            # Filtra pagamentos do ano alvo
            self.logger.info("Filtrando pagamentos por ano...")
            pagamentos_filtered = self.fraud_analyzer.filter_target_year(
                pagamentos_df, "data_processamento", self.config.target_year
            )
            
            # Filtra pedidos do ano alvo
            self.logger.info("Filtrando pedidos por ano...")
            pedidos_filtered = self.fraud_analyzer.filter_target_year(
                pedidos_df, "DATA_CRIACAO", self.config.target_year
            )
            
            # Filtra pagamentos recusados mas legítimos
            self.logger.info("Aplicando filtro de fraude...")
            legitimate_refused = self.fraud_analyzer.filter_legitimate_refused_payments(
                pagamentos_filtered
            )
            
            # Calcula totais dos pedidos
            self.logger.info("Calculando totais dos pedidos...")
            pedidos_with_totals = self.fraud_analyzer.calculate_order_totals(
                pedidos_filtered
            )
            
            # Faz join entre pagamentos e pedidos
            self.logger.info("Unindo dados de pagamentos e pedidos...")
            joined_data = self.fraud_analyzer.join_payment_and_order_data(
                legitimate_refused, pedidos_with_totals
            )
            
            # Cria relatório final
            self.logger.info("Criando relatório final...")
            report_df = self.fraud_analyzer.create_fraud_report(joined_data)
            
            # Ordena conforme especificação
            self.logger.info("Ordenando relatório...")
            final_report = self.fraud_analyzer.sort_report(report_df)
            
            self.logger.info("Transformação de dados concluída com sucesso")
            
            return final_report
            
        except Exception as e:
            self.logger.error(f"Erro na transformação de dados: {str(e)}")
            raise
    
    def load_data(self, report_df: DataFrame) -> None:
        """
        Carrega o relatório final em formato Parquet.
        
        Args:
            report_df (DataFrame): DataFrame do relatório final
        """
        try:
            self.logger.info("=== FASE DE CARREGAMENTO ===")
            
            output_path = f"{self.config.get_output_full_path()}/fraud_report"
            
            # Remove diretório se existir
            self.data_writer.remove_directory(output_path)
            
            # Escreve em formato Parquet
            self.logger.info(f"Salvando relatório em: {output_path}")
            self.data_writer.write_parquet(report_df, output_path)
            
            # Log estatísticas finais
            total_records = report_df.count()
            self.logger.info(f"Relatório salvo com sucesso: {total_records} registros")
            
            # Mostra preview dos dados
            self.logger.info("Preview do relatório:")
            report_df.show(20, truncate=False)
            
        except Exception as e:
            self.logger.error(f"Erro no carregamento de dados: {str(e)}")
            raise
    
    def run_pipeline(self) -> None:
        """
        Executa o pipeline completo de análise de fraude.
        """
        try:
            self.logger.info("=== INICIANDO PIPELINE DE ANÁLISE DE FRAUDE ===")
            
            # 1. Extração
            dataframes = self.extract_data()
            
            # 2. Transformação
            report_df = self.transform_data(dataframes)
            
            # 3. Carregamento
            self.load_data(report_df)
            
            self.logger.info("=== PIPELINE CONCLUÍDO COM SUCESSO ===")
            
        except Exception as e:
            self.logger.error(f"Erro na execução do pipeline: {str(e)}")
            raise
        finally:
            # Para a sessão Spark
            self.session_manager.stop_session()
            self.logger.info("Sessão Spark finalizada")
