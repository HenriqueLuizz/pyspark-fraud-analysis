# Guia de Instalação

## Pré-requisitos

### Java
PySpark 4.0+ requer Java 17 ou superior. Verifique sua versão:

```bash
java -version
```

Se necessário, instale Java 17:

#### macOS (usando Homebrew)
```bash
brew install openjdk@17
echo 'export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
```

#### Ubuntu/Debian
```bash
sudo apt update
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

### Python
- Python 3.8 ou superior

## Instalação do Projeto

1. **Clone o repositório com submodules:**
```bash
git clone --recursive <url-do-repositorio>
cd pyspark-fraud-analysis
```

2. **Se já clonado, inicialize os submodules:**
```bash
git submodule init
git submodule update
```

3. **Instale as dependências:**
```bash
pip install -r requirements.txt
```

4. **Ou instale em modo desenvolvimento:**
```bash
pip install -e .
```

## Execução

### Pipeline Principal
```bash
python main.py
```

### Testes
```bash
# Testes básicos (sem Spark)
pytest tests/test_simple_validation.py -v

# Testes completos (requer Java 17+)
pytest tests/ -v
```

## Solução de Problemas

### Erro: "Java gateway process exited"
- Verifique se Java 17+ está instalado
- Configure JAVA_HOME corretamente
- Reinicie o terminal após configurar o Java

### Erro: "No module named pyspark"
```bash
pip install pyspark>=3.4.0
```

### Erro nos submodules
```bash
git submodule deinit --all
git submodule init
git submodule update
```

## Estrutura de Saída

Após execução bem-sucedida:
- **Relatório**: `data/output/fraud_report/` (formato Parquet)
- **Logs**: Saída do console com informações detalhadas do pipeline
