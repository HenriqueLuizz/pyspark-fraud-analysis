# PySpark Fraud Analysis

## Descrição

Projeto de análise de fraude desenvolvido em PySpark seguindo princípios de orientação a objetos, injeção de dependências e boas práticas de engenharia de software. O objetivo é identificar pagamentos recusados que foram classificados como legítimos pela análise de fraude.

## Escopo de Negócio

A alta gestão da empresa deseja um relatório de pedidos de venda cujos pagamentos foram recusados (status=false) mas que na avaliação de fraude foram classificados como legítimos (fraude=false).

### Relatório Gerado

O relatório contém:
1. Identificador do pedido (id_pedido)
2. Estado (UF) onde o pedido foi feito
3. Forma de pagamento
4. Valor total do pedido
5. Data do pedido

**Filtros aplicados:**
- Apenas pedidos do ano de 2025
- Ordenação: estado (UF), forma de pagamento, data de criação do pedido
- Formato de saída: Parquet

## Arquitetura do Projeto

O projeto segue uma arquitetura em camadas com injeção de dependências:

```
src/fraud_analysis/
├── config/          # Configurações centralizadas
├── spark_session/   # Gerenciamento da sessão Spark
├── schemas/         # Schemas explícitos dos DataFrames
├── io/             # Classes de leitura e escrita de dados
├── business/       # Lógica de negócios
└── pipeline/       # Orquestração do pipeline
```

### Componentes Principais

- **AppConfig**: Configurações centralizadas da aplicação
- **SparkSessionManager**: Gerenciamento da sessão Spark
- **DataSchemas**: Schemas explícitos para todos os DataFrames
- **DataReader/DataWriter**: Classes de I/O para diferentes formatos
- **FraudAnalyzer**: Lógica de negócios para análise de fraude
- **FraudAnalysisPipeline**: Orquestração do pipeline ETL
- **main.py**: Aggregation Root com injeção de dependências

## Instalação

### Pré-requisitos

- Python 3.8+
- Java 8+ (para PySpark)

### Instalação das Dependências

```bash
pip install -r requirements.txt
```

### Instalação em Modo Desenvolvimento

```bash
pip install -e .
```

## Datasets

O projeto utiliza dois datasets como submodules:

- **Pagamentos**: `data/dataset-json-pagamentos/data/pagamentos/`
- **Pedidos**: `data/datasets-csv-pedidos/data/pedidos/`

### Inicialização dos Submodules

Se você clonou o repositório, inicialize os submodules:

```bash
git submodule init
git submodule update
```

## Uso

### Execução do Pipeline

```bash
python main.py
```

### Execução dos Testes

```bash
pytest tests/ -v
```

### Execução dos Testes com Cobertura

```bash
pytest tests/ --cov=src --cov-report=html
```

## Estrutura de Dados

### Schema de Pagamentos (JSON)
```python
{
    "id_pedido": "UUID",
    "forma_pagamento": "PIX|CARTAO_CREDITO|BOLETO",
    "valor_pagamento": "float",
    "status": "boolean",
    "data_processamento": "timestamp",
    "avaliacao_fraude": {
        "fraude": "boolean",
        "score": "float"
    }
}
```

### Schema de Pedidos (CSV)
```
ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE
```

## Saída

O relatório final é salvo em:
- **Localização**: `data/output/fraud_report/`
- **Formato**: Parquet
- **Particionamento**: Coalesce(1) para arquivo único

## Logging

O projeto implementa logging completo com:
- Configuração centralizada
- Logs de todas as etapas do pipeline
- Tratamento de erros com logs detalhados
- Formato: `%(asctime)s - %(levelname)s - %(message)s`

## Critérios de Qualidade Atendidos

✅ **Schemas explícitos**: Todos os DataFrames têm schemas definidos explicitamente  
✅ **Orientação a objetos**: Todos os componentes encapsulados em classes  
✅ **Injeção de Dependências**: main.py como Aggregation Root  
✅ **Configurações centralizadas**: Classe AppConfig  
✅ **Sessão Spark**: Classe SparkSessionManager  
✅ **Leitura e Escrita de Dados**: Classes DataReader e DataWriter  
✅ **Lógica de Negócio**: Classe FraudAnalyzer  
✅ **Orquestração do pipeline**: Classe FraudAnalysisPipeline  
✅ **Logging**: Implementado com tratamento de erros  
✅ **Tratamento de Erros**: try/catch com logging  
✅ **Empacotamento**: pyproject.toml, requirements.txt, MANIFEST.in  
✅ **Testes unitários**: pytest com cobertura  

## Desenvolvimento

### Estrutura de Testes

```
tests/
├── __init__.py
└── test_fraud_analyzer.py    # Testes da lógica de negócios
```

### Executar Formatação de Código

```bash
black src/ tests/
```

### Executar Análise de Código

```bash
flake8 src/ tests/
```

## Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo LICENSE para detalhes.

## Contato

Henrique Luiz - henrique@example.com

Link do Projeto: [https://github.com/seu-usuario/pyspark-fraud-analysis](https://github.com/seu-usuario/pyspark-fraud-analysis)