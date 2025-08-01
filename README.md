# 📊 Projeto ComexStat ETL + Dashboard Metabase

Este projeto realiza um pipeline completo de **ETL (Extração, Transformação e Carga)** a partir da API pública do [ComexStat](https://comexstat.mdic.gov.br/en/home), com visualização final em dashboards no Metabase.

---

## 🔗 Fonte de Dados

- **Origem**: [ComexStat - Ministério do Desenvolvimento, Indústria e Comércio Exterior (MDIC)](https://comexstat.mdic.gov.br/en/home)
- **Tipo**: API pública REST
- **Formato da resposta**: JSON

---

## ⚙️ Pipeline de Dados

### 1. 🔌 Extração da API

Os dados são coletados via scripts em Python localizados na pasta [`api/`](./api), utilizando chamadas programadas a endpoints REST da ComexStat.

- Bibliotecas: `requests`, `json`
- Parametrização dinâmica (ano, mês, tipo de operação, classificação)
- Exemplo de endpoint:
  ```
  https://comexstat.mdic.gov.br/api/consulta/municipio?tipo=EXP&ano=2024&mes=1
  ```

---

### 2. 🧪 Transformação em Parquet

Os dados brutos JSON são convertidos para o formato **Parquet**, com uso das bibliotecas `pandas` e `pyarrow`. Os arquivos são salvos na pasta [`stage/`](./stage), organizados por categoria:

- `blocos_economicos_cidade.parquet`
- `categorias_produtos.parquet`
- `classificacoes.parquet`
- `localidade_eua.parquet`
- `modos_transporte.parquet`
- `metricas.parquet`

---

### 3. 🗄️ Carga no PostgreSQL

Os arquivos `.parquet` são lidos e inseridos em tabelas no PostgreSQL por meio do script [`insercao_postgres.py`](./banco/insercao_postgres.py), com uso de `sqlalchemy` e `psycopg2`.

---

### 4. 📊 Visualização no Metabase

Com os dados organizados e armazenados no PostgreSQL, foi realizada a conexão com o **Metabase**, criando dashboards interativos com indicadores de comércio exterior.

📎 [🔗 Clique aqui para visualizar o dashboard exportado (PDF)](https://drive.google.com/file/d/1Z_h15d6QDDtUj629ELlwLvLL_QYEHgAr/view?usp=sharing)

<img src="https://github.com/user-attachments/assets/703793c6-8150-4145-82d0-ca0c2672ae68" width="649" height="751" />
<img src="https://github.com/user-attachments/assets/3f02606a-05dd-4f27-af05-b4bd332448f8" width="618" height="666" />
<img src="https://github.com/user-attachments/assets/d137929f-3e8b-40fe-85cb-3fd2eda8324a" width="626" height="549" />


---

## 📁 Estrutura do Projeto

```
COMEX_OO/
├── api/
│   ├── classificacoes.py
│   ├── cliente_api.py
│   ├── dados_municipios.py
│   ├── dados_produtos.py
│   ├── filtros.py
│   ├── localidades.py
│   ├── modos_transporte.py
│   ├── produtos.py
│   └── requisicao_base.py
│
├── banco/
│   └── insercao_postgres.py
│
├── stage/
│   ├── blocos_economicos_cidade.parquet
│   ├── categorias_produtos.parquet
│   ├── classificacoes.parquet
│   ├── localidade_eua.parquet
│   ├── metricas.parquet
│   └── modos_transporte.parquet
│
├── utils/
│   ├── arquivos.py
│   └── __init__.py
│
├── main.py

```

---

## 🛠️ Tecnologias Utilizadas

| Etapa            | Ferramenta/Biblioteca         |
|------------------|-------------------------------|
| Coleta           | Python `requests`             |
| Transformação    | `pandas`, `pyarrow`           |
| Armazenamento    | PostgreSQL, `sqlalchemy`      |
| Visualização     | Metabase                      |
| Output final     | PDF com dashboard             |

---

## ✅ Resultados

- 🔍 Dados de comércio exterior organizados e consultáveis
- 🚀 Consultas rápidas e dashboards claros no Metabase
- 🧾 Relatórios em PDF com visualizações para tomada de decisão
