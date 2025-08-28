"""
Classes de escrita de dados.
"""

import os
from pyspark.sql import DataFrame
from pathlib import Path


class DataWriter:
    """Classe responsável pela escrita de dados."""
    
    def __init__(self):
        """Inicializa o escritor de dados."""
        pass
    
    def write_parquet(self, df: DataFrame, output_path: str, 
                     mode: str = "overwrite") -> None:
        """
        Escreve DataFrame em formato Parquet.
        
        Args:
            df (DataFrame): DataFrame a ser escrito
            output_path (str): Caminho de saída
            mode (str): Modo de escrita (overwrite, append)
        """
        # Cria diretório se não existir
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        (df.coalesce(1)
         .write
         .mode(mode)
         .parquet(output_path))
    
    def write_csv(self, df: DataFrame, output_path: str, 
                 mode: str = "overwrite", header: bool = True,
                 separator: str = ",") -> None:
        """
        Escreve DataFrame em formato CSV.
        
        Args:
            df (DataFrame): DataFrame a ser escrito
            output_path (str): Caminho de saída
            mode (str): Modo de escrita
            header (bool): Incluir cabeçalho
            separator (str): Separador
        """
        # Cria diretório se não existir
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        (df.coalesce(1)
         .write
         .mode(mode)
         .option("header", header)
         .option("sep", separator)
         .csv(output_path))
    
    def ensure_directory_exists(self, path: str) -> None:
        """
        Garante que o diretório existe.
        
        Args:
            path (str): Caminho do diretório
        """
        Path(path).mkdir(parents=True, exist_ok=True)
    
    def remove_directory(self, path: str) -> None:
        """
        Remove diretório se existir.
        
        Args:
            path (str): Caminho do diretório
        """
        if os.path.exists(path):
            import shutil
            shutil.rmtree(path)
