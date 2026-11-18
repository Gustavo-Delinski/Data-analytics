# Análise do Mercado de Jogos na Steam e Monitoramento em Tempo Real

Projeto desenvolvido para a disciplina de **Data Analytics** (Turma 6° GCL1), cobrindo o ciclo completo de análise e engenharia de dados: desde ingestão, exploração e tratamento de dados estáticos históricos (**RA1**), passando pelo processamento em tempo real com streaming contínuo e janelas temporais (**RA2**), até a análise estatística descritiva, visualização de dados e aprendizagem de máquina supervisionada (**RA3**).

---

## Integrantes do Grupo
* **Gustavo Delinski Tavares**
* **Eduardo Jabur**  
* **Turma:** 6° GCL1

---

## Link do Notebook Executável
Abra a versão deste repositório diretamente no Google Colab; o link abaixo acompanha os arquivos atualizados:
**[Executar Projeto no Google Colab](https://colab.research.google.com/github/Gustavo-Delinski/Data-analytics/blob/main/Cemplate_projeto_data_analytics_%281%29.ipynb)**

---

## Contexto e Objetivos do Projeto

A Steam é a maior loja de jogos digitais para PC do mundo, recebendo dezenas de milhares de títulos por ano (quase 25 mil lançamentos registrados apenas em 2025). Com um mercado saturado e altamente competitivo, compreender o engajamento dos jogadores, a evolução dos gêneros e o comportamento dos diferentes modelos de monetização (gratuito vs. pago) é essencial para estúdios e publicadoras.

O projeto está organizado nas três etapas do template da disciplina:

### RA1 — Data Ingestion, Cleaning and Initial Analysis
Investigação exploratória e preparação de um dataset histórico com mais de 125 mil registros:
* **P1:** Quais gêneros apresentam maior tempo mediano de jogo por jogador?
* **P2:** Quais gêneros tiveram maior crescimento no número de lançamentos entre 2020 e 2025?
* **P3:** Entre os 100 jogos com mais donos estimados, como gratuitos e pagos se comparam em pico de jogadores simultâneos e proporção de avaliações positivas?

### RA2 — Streaming Data Processing and Time Windows
Arquitetura em tempo real (**Cenário B — Dados Reais em Streaming**):
* Consultas em lotes à API pública oficial da Steam (`ISteamUserStats/GetNumberOfCurrentPlayers/v1/`).
* Processamento e enriquecimento em streaming com **Apache Spark Structured Streaming**.
* Agrupamento temporal com **janelas fixas (*Tumbling Windows*) de 10 segundos** para acompanhar a média de jogadores simultâneos entre jogos gratuitos confirmados e pagos.

### RA3 — Análise, Visualização e Aprendizagem de Máquina
Consolidação analítica e preditiva sobre a base preparada no RA1:
* **Estatística Descritiva:** Avaliação de tendência central, dispersão e distribuição global e por recortes (P1 e P3).
* **Visualizações e Interpretações:** Gráficos dedicados às perguntas analíticas e à distribuição das variáveis do projeto com leitura crítica dos padrões e limites.
* **Amostragem e Representatividade:** Documentação do tamanho e dos vieses de cada recorte utilizado.
* **Machine Learning Supervisionado:** Classificação binária com **Random Forest** para estimar se o tempo mediano de jogo supera o corte de referência do conjunto de treino.

---

## Tecnologias e Bibliotecas Utilizadas
* **Linguagem:** Python 3.12 (e Java 17/21 para execução local do Spark)
* **Manipulação e Análise de Dados:** `pandas`, `numpy`, `kagglehub`
* **Processamento Distribuído / Streaming:** `pyspark` (`SparkSession`, Structured Streaming, Tumbling Windows)
* **Visualização de Dados:** `matplotlib`, `seaborn`
* **Machine Learning:** `scikit-learn` (`RandomForestClassifier`, `DummyClassifier`, métricas de classificação)
* **Concorrência e Requisições HTTP:** `requests`, `concurrent.futures.ThreadPoolExecutor`
* **Serialização e Arquivos:** `json`, `tempfile`, `pathlib`, `os`, `hashlib`

---

## Estrutura e Pipeline do Projeto

### 1. Ingestão e Tratamento de Dados (RA1)
* **Dataset Base:** [Steam Games Dataset (Kaggle / FronkonGames)](https://www.kaggle.com/datasets/fronkongames/steam-games-dataset) — versão 59 (125.855 registros e 40 colunas), com integridade validada por hash `SHA-256`.
* **Tratamento de Anomalias:**
  * Correção de cabeçalho corrompido (`DiscountDLC count` desmembrado em `Discount` e `DLC count`).
  * Tratamento de zeros mascarados: valores zero em `Median playtime forever` (99.682 registros) e faixas `0 - 0` em `Estimated owners` (24.579 registros) convertidos para ausentes (`NaN`), evitando distorção nas medianas.
  * Limpeza de itens não-jogos: exclusão de 1.491 registros exclusivamente de software/vídeo, 1 registro sem nome e 111 duplicatas exatas (`Name + Developers + Release date`), mantendo a cópia com maior volume de dados.
  * Engenharia de atributos: criação de `generos_jogo` (removendo tags não-gênero), `ano_lancamento` e `modelo_negocio`, dividindo os títulos em **Pago** (`Price > 0`: 98.120 jogos), **Gratuito confirmado** (`Price == 0` com tag `Free To Play`: 12.336 jogos) e **Não confirmado** (demais casos: 13.796 jogos, excluídos das comparações diretas entre modelos).
* **Dataset Final Gerado:** `steam_games_limpo.csv` contendo **124.252 registros** estruturados e **18 colunas** selecionadas.

### 2. Arquitetura de Streaming e Janelas Temporais (RA2)
```
  [API Oficial da Steam]
            │ (Requisições paralelas via ThreadPoolExecutor)
            ▼
  [Lotes JSON (coletado_em + timestamp)] ──> [Pasta Monitorada: stream_input/]
                                                       │
                                                       ▼ (spark.readStream)
                             [Join em Tempo Real com Metadados do RA1 (AppID)]
                                                       │
                                                       ▼
                             [Agrupamento: Tumbling Window 10s + modelo_negocio]
                                                       │
                                                       ▼
                             [Métricas: Leituras & Média de Jogadores por Observação]
```
* **Amostra Monitorada:** 5 jogos `Gratuito confirmado` (*Counter-Strike 2*, *Dota 2*, *PUBG: BATTLEGROUNDS*, *Apex Legends™* e *Team Fortress 2*) e 5 jogos `Pago` (*Left 4 Dead 2*, *Palworld*, *Black Myth: Wukong*, *New World: Aeternum* e *Rust*), selecionados por `owners_medio`, `Peak CCU` e `AppID`.
* **Demonstração de Janelas (Antes e Depois):**
  * **Lote 1 (Evento `:02`):** Inicia a janela `HH:mm:00` a `HH:mm:10` com 5 leituras para `Gratuito confirmado` e 5 para `Pago`.
  * **Lote 2 (Evento `:05`):** Cai na **mesma janela**, atualizando o agregado em tempo real para 10 leituras por grupo e recalculando a média.
  * **Lote 3 (Evento `:12`):** Entra no intervalo seguinte e faz o Spark **criar uma nova janela temporal** (`HH:mm:10` a `HH:mm:20`).
  * *Nota:* Os horários de evento (`:02`, `:05`, `:12`) são atribuídos para fins didáticos, preservando o instante real da consulta UTC na coluna `coletado_em`.

### 3. Estatística, Visualização e Machine Learning (RA3)
* **3.1 Análise Estatística Descritiva:**
  * Avaliação de tendência central (média e mediana), dispersão (desvio-padrão e IQR) e distribuição (`Min`, `P90` e `Max` e histogramas em escala log).
  * **Assimetria Global:** A mediana do tempo de jogo informado é de **204,00 min** frente a uma média de **779,46 min** (`DP = 24.333,72 min`), confirmando forte assimetria à direita e presença de valores extremos; o preço tem mediana de **US$ 2,39** (`IQR = US$ 4,80` e `P90 = US$ 9,99`).
  * **Por Gênero (P1) e Modelo de Negócio (P3):** Entre gêneros com $\ge 100$ jogos medidos, **RPG (266,0 min)**, **Strategy (254,0 min)** e **Simulation (224,0 min)** lideram as medianas. No Top 100 por donos estimados (excluindo 7 não confirmados), os **27 gratuitos confirmados** registram pico mediano de **14.169,0** (`Peak CCU` médio de **92.319,44**) contra **13.517,5** dos **66 pagos** (média de **19.531,17**), enquanto os jogos pagos lideram a aprovação mediana (**92,66%** vs. **78,57%**).
* **3.2 Visualizações e Interpretações:**
  * **Gráfico 1 (P1):** Ranking em barras horizontais do tempo mediano de jogo por gênero (jogos multigênero participam de todas as categorias associadas).
  * **Gráfico 2 (P2):** Expansão percentual de lançamentos entre 2020 e 2025 (exigindo $\ge 100$ títulos no ano-base), destacando **Simulation (+189,5%)**, **RPG (+176,0%)**, **Strategy (+163,3%)** e **Casual (+160,4%)**.
  * **Gráfico 3 (P3):** *Boxplot* combinado com *stripplot* da taxa de aprovação no Top 100 confirmado: os títulos pagos têm maior mediana e caixa interquartil mais estreita, mas com cauda inferior longa e alguns outliers abaixo de 50%.
  * **Gráficos complementares:** histogramas em escala log do tempo de jogo, do preço e do pico de jogadores (distribuição, seção 3.1) e *boxplot* do pico de jogadores no Top 100 (P3).
* **3.3 Amostragem e Representatividade:**
  * Documentação dos recortes analíticos: **124.252** itens preparados na base final (não tratada como censo completo do catálogo); **25.745** jogos na P1; **30.026** na P2 (anos 2020 e 2025); **100** selecionados e **93** comparados na P3; **23.647** na modelagem preditiva.
  * Discussão explícita dos vieses: alta proporção de tempos ausentes (~79% sem comprovação de aleatoriedade), estimativas por faixa do SteamSpy, sobreposição de gêneros e uso do tempo de jogo como aproximação de engajamento (sem medir retenção longitudinal).
* **3.4 Aplicação de Machine Learning (Classificação Supervisionada):**
  * **Problema Preditivo:** Classificar se o tempo mediano de jogo observado supera o corte de referência calculado exclusivamente no treino (**> 204 minutos**, mediana do treino, evitando *data leakage*).
  * **Features e Divisão:** **23.647 jogos** válidos divididos aleatoriamente em **80% treino (18.917)** e **20% teste (4.730)** (`random_state=42`), utilizando como entradas `Price`, `ano_lancamento`, `taxa_aprovacao`, `log_total_avaliacoes` ($\log(1 + \text{avaliações})$), indicadores das três categorias de `modelo_negocio` e presença dos 10 principais gêneros.
  * **Modelo e Avaliação:** `RandomForestClassifier` (150 árvores, `max_depth=12`) comparado à referência `DummyClassifier(strategy='most_frequent')`.
  * **Desempenho no Teste:** Acurácia de **66,11%** contra **48,92%** da referência (**+17,19 pontos percentuais**), acurácia balanceada de **66,27%**, F1-score de **0,68** (`Até o corte`) e **0,64** (`Acima do corte`), e F1 macro de **0,6599**, acompanhados da Matriz de Confusão e da Importância Relativa por redução de impureza (`log_total_avaliacoes`, `Price`, `taxa_aprovacao` e `ano_lancamento` no topo); validação cruzada de 5 folds no treino: média de **68,20%** (DP de 0,56 p.p.)

---

## Como Executar o Projeto e Limites

### Opção 1: Google Colab (Recomendado)
1. Abra o notebook no **[Google Colab](https://colab.research.google.com/github/Gustavo-Delinski/Data-analytics/blob/main/Cemplate_projeto_data_analytics_%281%29.ipynb)**.
2. Execute as células em ordem:
   * **Células 1 a 27 (RA1):** Instalam as dependências, baixam a versão 59 da base (ou leem `games.csv` local), validam o `SHA-256`, realizam a limpeza e geram `steam_games_limpo.csv` (não é necessário versionar o CSV).
   * **Células 28 a 33 (RA2):** Inicializam o PySpark, consultam a API da Steam e executam a demonstração das janelas *Tumbling* de 10s (exige acesso à internet).
   * **Células 34 a 40 (RA3):** Executam as análises estatísticas, geram os gráficos e treinam/avaliam o modelo *Random Forest*. Para rodar **apenas o RA3**, carregue `steam_games_limpo.csv` e execute as seções **3.1 a 3.5** (não dependem do Spark).

### Opção 2: Execução Local
Utilize **Python 3.12** e **Java 17 ou 21** (para o PySpark) e instale as versões exatas das dependências:
```bash
python -m pip install pandas==2.2.3 numpy==2.1.2 matplotlib==3.9.2 seaborn==0.13.2 scikit-learn==1.6.1 pyspark==4.0.1 requests==2.32.3 kagglehub==0.3.13
```

### Limites Metodológicos
* **RA2:** As conclusões do streaming restringem-se aos 10 jogos consultados e aos instantes da execução; no modo `complete` sem *watermark*, janelas anteriores não são congeladas definitivamente contra eventos atrasados.
* **RA3:** O modelo utiliza variáveis observadas após o lançamento (como volume e taxa de avaliações) e foi avaliado em uma divisão aleatória (e validação cruzada também aleatória), sem validação temporal; portanto, classifica padrões históricos de tempo de jogo sem demonstrar previsão pré-lançamento ou relação de causalidade.