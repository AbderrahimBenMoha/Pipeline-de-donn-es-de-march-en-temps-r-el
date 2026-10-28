from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from deltalake import DeltaTable


# Retrouver la racine du projet
ROOT_DIR = Path(__file__).resolve().parents[1]
GOLD_PATH = ROOT_DIR / "data" / "gold" / "minute_metrics"

st.set_page_config(
    page_title="BTC/USDT - Data Pipeline",
    page_icon="📈",
    layout="wide",
)

st.title("BTC/USDT — Real-Time Data Pipeline")
st.caption(
    "Données de marché Binance | Kafka | Spark | Delta Lake"
)

# Vérifier que la table Gold existe
if not (GOLD_PATH / "_delta_log").exists():
    st.error("Table Gold introuvable. Vérifie le pipeline Spark.")
    st.stop()

# Lire les données de Delta Lake
try:
    df = DeltaTable(str(GOLD_PATH)).to_pandas()
except Exception as error:
    st.error(f"Impossible de lire la table Gold : {error}")
    st.stop()

if df.empty:
    st.warning("Aucune donnée disponible pour le moment.")
    st.stop()

# Préparer les colonnes pour les graphiques
df["minute_start"] = pd.to_datetime(
    df["minute_start"], errors="coerce"
)

numeric_columns = [
    "avg_price",
    "min_price",
    "max_price",
    "total_btc",
    "total_value_usdt",
    "trade_count",
]

for column in numeric_columns:
    df[column] = pd.to_numeric(
        df[column], errors="coerce"
    )

df = (
    df.dropna(subset=["minute_start"])
    .sort_values("minute_start")
)

if df.empty:
    st.warning("Aucune fenêtre temporelle exploitable.")
    st.stop()

# Dernière minute disponible
latest = df.iloc[-1]

if st.button("Actualiser les données"):
    st.rerun()

# Indicateurs clés
col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Prix moyen",
    f"{latest['avg_price']:,.2f} USDT",
)

col2.metric(
    "Transactions",
    f"{int(latest['trade_count']):,}",
)

col3.metric(
    "Volume échangé",
    f"{latest['total_btc']:,.6f} BTC",
)

col4.metric(
    "Valeur échangée",
    f"{latest['total_value_usdt']:,.2f} USDT",
)

st.caption(
    f"Dernière fenêtre disponible : "
    f"{latest['minute_start']}"
)

# Graphique d'évolution des prix
st.subheader("Évolution du prix moyen par minute")

price_chart = px.line(
    df,
    x="minute_start",
    y="avg_price",
    color="symbol",
    markers=True,
    labels={
        "minute_start": "Minute",
        "avg_price": "Prix moyen (USDT)",
        "symbol": "Paire",
    },
)

st.plotly_chart(
    price_chart,
    width="stretch",
)

# Graphique des volumes
st.subheader("Volume échangé par minute")

volume_chart = px.bar(
    df,
    x="minute_start",
    y="total_btc",
    color="symbol",
    labels={
        "minute_start": "Minute",
        "total_btc": "Volume (BTC)",
        "symbol": "Paire",
    },
)

st.plotly_chart(
    volume_chart,
    width="stretch",
)

# Tableau de données
st.subheader("Données agrégées — Gold")

st.dataframe(
    df.sort_values("minute_start", ascending=False),
    width="stretch",
    hide_index=True,
)
