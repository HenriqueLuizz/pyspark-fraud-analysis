"""
Classes de leitura de dados.
"""

import glob
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from typing import List


class DataReader:
    """Classe responsável pela leitura de dados."""
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o leitor de dados.
        
        Args:
            spark (SparkSession): Sessão Spark
        """
        self.spark = spark
    
    def read_json_files(self, path: str, schema: StructType, 
                       year_filter: str = None) -> DataFrame:
        """
        Lê arquivos JSON com schema explícito.
        
        Args:
            path (str): Caminho para os arquivos JSON
            schema (StructType): Schema explícito
            year_filter (str, optional): Filtro por ano
            
        Returns:
            DataFrame: DataFrame com dados JSON
        """
        if year_filter:
            pattern = f"{path}/*{year_filter}*.json.gz"
        else:
            pattern = f"{path}/*.json.gz"
        
        files = glob.glob(pattern)
        
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado em: {pattern}")
        
        return self.spark.read.schema(schema).json(files)
    
    def read_csv_files(self, path: str, schema: StructType,
                      year_filter: str = None, separator: str = ";") -> DataFrame:
        """
        Lê arquivos CSV com schema explícito.
        
        Args:
            path (str): Caminho para os arquivos CSV
            schema (StructType): Schema explícito
            year_filter (str, optional): Filtro por ano
            separator (str): Separador do CSV
            
        Returns:
            DataFrame: DataFrame com dados CSV
        """
        if year_filter:
            pattern = f"{path}/*{year_filter}*.csv.gz"
        else:
            pattern = f"{path}/*.csv.gz"
        
        files = glob.glob(pattern)
        
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado em: {pattern}")
        
        return (self.spark.read
                .schema(schema)
                .option("header", "true")
                .option("sep", separator)
                .csv(files))
    
    def get_available_files(self, path: str, extension: str = "*") -> List[str]:
        """
        Lista arquivos disponíveis em um caminho.
        
        Args:
            path (str): Caminho para buscar arquivos
            extension (str): Extensão dos arquivos
            
        Returns:
            List[str]: Lista de arquivos encontrados
        """
        pattern = f"{path}/*.{extension}"
        return glob.glob(pattern)
