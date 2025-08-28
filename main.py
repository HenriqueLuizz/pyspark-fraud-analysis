"""
Main.py - Aggregation Root para o projeto de análise de fraude.
Responsável pela injeção de dependências e orquestração principal.
"""

import logging
import sys
from pathlib import Path

# Adiciona o diretório src ao path
sys.path.append(str(Path(__file__).parent / "src"))

from src.fraud_analysis.config.app_config import AppConfig
from src.fraud_analysis.spark_session.session_manager import SparkSessionManager
from src.fraud_analysis.io.data_reader import DataReader
from src.fraud_analysis.io.data_writer import DataWriter
from src.fraud_analysis.business.fraud_analyzer import FraudAnalyzer
from src.fraud_analysis.pipeline.fraud_pipeline import FraudAnalysisPipeline


def main():
    """
    Função principal - Aggregation Root com injeção de dependências.
    """
    try:
        # Configuração do logging principal
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        logger = logging.getLogger(__name__)
        
        logger.info("=== INICIANDO APLICAÇÃO DE ANÁLISE DE FRAUDE ===")
        
        # 1. Instanciação das dependências - Classes de configuração
        logger.info("Configurando aplicação...")
        config = AppConfig()
        
        # 2. Classes de gerenciamento de sessão spark
        logger.info("Criando gerenciador de sessão Spark...")
        session_manager = SparkSessionManager(
            app_name=config.app_name,
            config=config.spark_config
        )
        
        # 3. Criação da sessão Spark
        logger.info("Inicializando sessão Spark...")
        spark = session_manager.create_session()
        
        # 4. Classes de leitura e escrita de dados
        logger.info("Configurando componentes de I/O...")
        data_reader = DataReader(spark)
        data_writer = DataWriter()
        
        # 5. Classes de lógica de negócios
        logger.info("Inicializando analisador de fraude...")
        fraud_analyzer = FraudAnalyzer()
        
        # 6. Classes de orquestração do pipeline
        logger.info("Configurando pipeline...")
        pipeline = FraudAnalysisPipeline(
            config=config,
            session_manager=session_manager,
            data_reader=data_reader,
            data_writer=data_writer,
            fraud_analyzer=fraud_analyzer
        )
        
        # 7. Execução do pipeline
        logger.info("Executando pipeline de análise de fraude...")
        pipeline.run_pipeline()
        
        logger.info("=== APLICAÇÃO CONCLUÍDA COM SUCESSO ===")
        
    except Exception as e:
        logger.error(f"Erro na execução da aplicação: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
