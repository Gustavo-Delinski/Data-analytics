# Análise do Mercado de Jogos na Steam e Monitoramento em Tempo Real

Projeto desenvolvido para a disciplina de Data Analytics, cobrindo o ciclo completo de análise de dados: desde ingestão, exploração e tratamento de dados estáticos históricos até a integração em tempo real com streaming contínuo e janelas temporais (*tumbling windows*).

---

## Integrantes do Grupo
* **Gustavo Delinski Tavares**
* **Eduardo Jabur**  
* **Turma:** 6° GCL1

---

## Link do Notebook Executável
Você pode visualizar, reproduzir e executar todo o pipeline diretamente no Google Colab através do link:  
**[Executar Projeto no Google Colab](https://colab.research.google.com/drive/1XItAKvIZUR1rbY-UR9L27kTq5z-jHJ_u?usp=sharing)**

---

## Contexto e Objetivos do Projeto

A Steam é a maior loja de jogos digitais para PC do mundo, recebendo dezenas de milhares de títulos por ano (quase 25 mil lançamentos registrados apenas em 2025). Com um mercado saturado e altamente competitivo, compreender a retenção de jogadores, a evolução de gêneros e o impacto do modelo de monetização (gratuito vs. pago) é crucial para estúdios e desenvolvedores.

O projeto foi dividido em duas grandes etapas:

### RA1 — Data Ingestion, Cleaning and Initial Analysis
Investigação exploratória e analítica de um dataset histórico com mais de 125 mil registros:
* **P1:** Quais gêneros apresentam maior tempo mediano de jogo por jogador?
* **P2:** Quais gêneros tiveram maior crescimento de lançamentos entre 2020 e 2025?
* **P3:** Entre os títulos mais populares, como jogos gratuitos e pagos se comparam em pico de jogadores simultâneos e avaliações positivas?

### RA2 — Streaming Data Processing and Time Windows
Arquitetura em tempo real (**Cenário B — Dados Reais em Streaming**):
* Consulta periódica à API oficial da Steam (`ISteamUserStats/GetNumberOfCurrentPlayers/v1/`).
* Processamento e enriquecimento em streaming com **Apache Spark Structured Streaming**.
* Agrupamento temporal com **janelas fixas (*Tumbling Windows*) de 10 segundos** para comparar o fluxo dinâmico de jogadores simultâneos entre jogos gratuitos e pagos.

---

## Tecnologias e Bibliotecas Utilizadas
* **Linguagem:** Python
* **Manipulação de Dados:** `pandas`, `kagglehub`
* **Processamento Distribuído / Streaming:** `pyspark` (`SparkSession`, Structured Streaming, Tumbling Windows)
* **Concorrência e Requisições HTTP:** `requests`, `concurrent.futures.ThreadPoolExecutor`
* **Serialização e Arquivos:** `json`, `shutil`, `os`

---

## Estrutura e Pipeline do Projeto

### 1. Ingestão e Tratamento de Dados (RA1)
* **Dataset Base:** [Steam Games Dataset (Kaggle / FronkonGames)](https://www.kaggle.com/datasets/fronkongames/steam-games-dataset).
* **Tratamento de Anomalias:**
  * Correção de cabeçalho corrompido (`DiscountDLC count` desmembrado em `Discount` e `DLC count`).
  * Tratamento de zeros mascarados (valores zero em `Median playtime forever` e faixas `0 - 0` em `Estimated owners` convertidos para ausentes).
  * Limpeza de itens não-jogos (softwares, utilitários, vídeos puros) e remoção de duplicatas reais (mesmo nome, desenvolvedor e data de lançamento).
  * Validação de jogos gratuitos reais (exigência de `Price == 0` em conjunto com a tag `Free To Play`, evitando falsos gratuitos como pacotes legados e deslistados).
* **Dataset Final Gerado:** `steam_games_limpo.csv` contendo 124.252 registros estruturados e 18 colunas selecionadas.

### 2. Arquitetura de Streaming e Janelas Temporais (RA2)
```
  [API Oficial da Steam]
            │ (Requisições paralelas via ThreadPoolExecutor)
            ▼
  [Lotes JSON com timestamp] ──> [Pasta Monitorada: stream_input/]
                                           │
                                           ▼ (spark.readStream)
                 [Join em Tempo Real com Metadados do RA1 (df_final)]
                                           │
                                           ▼
                 [Agrupamento: Tumbling Window 10s + Gratuito vs. Pago]
                                           │
                                           ▼
                 [Métricas: Leituras & Média de Jogadores Ativos]
```

#### Demonstração de Janelas (Antes e Depois)
* **Lote 1 (Segundo `:02`):** Inicia a janela `HH:mm:00` a `HH:mm:10` registrando 5 leituras para jogos gratuitos e 5 para pagos.
* **Lote 2 (Segundo `:05`):** Cai na **mesma janela**. A contagem de leituras é atualizada para 10 por categoria, recalculando a média de jogadores online em tempo real.
* **Lote 3 (Segundo `:12`):** Como o tempo avançou além de 10s, o Spark congela a janela anterior e **abre automaticamente uma nova janela temporal** (`HH:mm:10` a `HH:mm:20`).

---

## Como Executar o Projeto

1. Abra o notebook no **[Google Colab](https://colab.research.google.com/drive/1XItAKvIZUR1rbY-UR9L27kTq5z-jHJ_u?usp=sharing)**.
2. Execute as células em ordem:
   * **Células 1 a 27 (RA1):** Baixam a base, corrigem o cabeçalho, realizam a limpeza e exportam o `steam_games_limpo.csv`.
   * **Células 28 e 29 (RA2 - Configuração):** Inicializam a sessão do PySpark e testam o primeiro lote da API da Steam.
   * **Células 30 a 33 (RA2 - Janelas):** Ativam a janela *Tumbling* de 10s e executam a demonstração ao vivo dos Lotes 1, 2 e 3 comprovando o comportamento do streaming.
