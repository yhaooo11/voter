import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st
import psycopg2
from kafka import KafkaConsumer
import simplejson as json

@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # total # of votes
    cur.execute("""
        SELECT count(*) voters_count from voters
    """)
    voters_count = cur.fetchone()[0]

    # total # of candidates
    cur.execute("""
        SELECT count(*) candidates_count from candidates
    """)
    candidates_count = cur.fetchone()[0]

    return voters_count, candidates_count

def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    return consumer

def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []

    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)

    return data

def plot_bar_chart(results):
    data_type = results['candidate_name']

    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))

def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    voters_count, candidates_count = fetch_voting_stats()

    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric('Total Voters', voters_count)
    col2.metric('Total Candidates', candidates_count)

    consumer = create_kafka_consumer(topic_name)

    data = fetch_data_from_kafka(consumer)

    results = pd.DataFrame(data)

    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    st.markdown("""---""")
    st.header('Leading Candidate')
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party'])
        st.subheader(f"Total Votes: {leading_candidate['total_votes']}")
    
    st.markdown("""---""")
    st.header('Voting Statistics')
    results = results[['candidate_id', 'candidate_name', 'party', 'total_votes']]
    results = results.reset_index(drop=True)

    col1, col2 = st.columns(2)

    with col1:
        bar_fig = plot_bar_chart(results)
        st.pyplot(bar_fig)
    
    with col2:
        donut_fig = plot_donut_chart(results)
        st.pyplot(bar_fig)

st.title('Realtime Election Voting Dashboard')
topic_name = 'aggregated_votes_per_candidate'

update_data()

