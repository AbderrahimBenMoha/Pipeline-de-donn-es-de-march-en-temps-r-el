# Real-Time Market Data Pipeline

Pipeline de Data Engineering en temps réel permettant de collecter des transactions BTC/USDT, de les traiter avec Apache Spark et de produire des indicateurs consultables dans un dashboard interactif.

## Objectif

Ce projet personnel vise à mettre en pratique les concepts de Data Engineering : ingestion d'événements en continu, traitement distribué, qualité des données, stockage analytique et visualisation.

Les données de marché proviennent du flux public WebSocket Spot de Binance.

## Architecture

```text
Binance WebSocket (BTC/USDT)
            |
            v
    Python Producer
            |
            v
     Apache Kafka
            |
            v
 Spark Structured Streaming
            |
            v
    Bronze - Delta Lake
            |
            v
    Silver - Delta Lake
            |
            v
     Gold - Delta Lake
            |
            v
       Streamlit
       + Plotly
```

### Bronze : ingestion des données

Les événements reçus depuis Kafka sont conservés sous forme brute avec leurs métadonnées d'ingestion. Cette couche permet de garder une trace des données avant leur transformation.

### Silver : qualité et transformation

Les événements JSON sont structurés en colonnes typées. Les prix et les quantités sont convertis en valeurs décimales, les dates en horodatages, et les transactions invalides sont filtrées. Une déduplication avec watermark est également appliquée.

### Gold : indicateurs analytiques

Les transactions sont agrégées par fenêtre d'une minute et par paire de marché.

Les indicateurs calculés comprennent :
- Le nombre de transactions.
- Le prix moyen, minimum et maximum.
- Le volume total échangé en BTC.
- La valeur totale estimée des échanges en USDT.

### Dashboard

L'interface Streamlit présente les principaux indicateurs, l'évolution du prix moyen, les volumes échangés et un tableau des données agrégées. Les graphiques sont réalisés avec Plotly.

## Technologies

- **Python** : collecte et orchestration des traitements applicatifs.
- **Apache Kafka** : transport des événements.
- **Apache Spark Structured Streaming** : traitement continu des données.
- **Delta Lake** : stockage transactionnel des couches Bronze, Silver et Gold.
- **Docker Compose** : exécution des services dans des conteneurs.
- **Streamlit, Pandas et Plotly** : visualisation des résultats.

## Organisation du projet

```text
Projet-Data/
├── producer/
│   └── main.py
├── spark/
│   ├── main.py
│   ├── silver.py
│   ├── gold.py
│   └── check_gold.py
├── dashboard/
│   └── app.py
├── data/                 # Données locales, non versionnées
├── tests/
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## Installation

Prérequis : Python, Docker Desktop et Docker Compose.

Créer un environnement Python virtuel et installer les dépendances :

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Démarrer Kafka :

```powershell
docker compose up -d
```

Créer le topic utilisé par le producteur :

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic market-data --partitions 1 --replication-factor 1
```

Lancer ensuite le producteur Python :

```powershell
.\.venv\Scripts\python.exe .\producer\main.py
```

Dans des terminaux distincts, démarrer les traitements Spark dans cet ordre : Bronze, Silver, puis Gold.

Les commandes ci-dessous récupèrent les dépendances nécessaires à Spark et Delta Lake lors de leur première exécution.

### Bronze

```powershell
docker compose --profile tools run --rm --entrypoint /bin/bash spark -lc "mkdir -p /tmp/ivy/cache /tmp/ivy/jars && /opt/spark/bin/spark-submit --master 'local[*]' --conf spark.jars.ivy=/tmp/ivy --packages io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4 /opt/app/spark/main.py"
```

### Silver

```powershell
docker compose --profile tools run --rm --entrypoint /bin/bash spark -lc "mkdir -p /tmp/ivy/cache /tmp/ivy/jars && /opt/spark/bin/spark-submit --master 'local[*]' --conf spark.jars.ivy=/tmp/ivy --packages io.delta:delta-spark_2.13:4.0.1 /opt/app/spark/silver.py"
```

### Gold

```powershell
docker compose --profile tools run --rm --entrypoint /bin/bash spark -lc "mkdir -p /tmp/ivy/cache /tmp/ivy/jars && /opt/spark/bin/spark-submit --master 'local[*]' --conf spark.jars.ivy=/tmp/ivy --packages io.delta:delta-spark_2.13:4.0.1 /opt/app/spark/gold.py"
```

Lancer enfin le dashboard :

```powershell
.\.venv\Scripts\python.exe -m streamlit run .\dashboard\app.py
```

L'application est alors accessible sur `http://localhost:8501`.

## Résultats

Le pipeline a été exécuté localement avec des transactions de marché reçues depuis Binance. Les données ont été enregistrées dans les trois couches Delta Lake et les indicateurs Gold ont été consultés dans le dashboard.
