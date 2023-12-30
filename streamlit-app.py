import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st
import psycopg2
from kafka import KafkaConsumer
import simplejson as json
from streamlit_autorefresh import st_autorefresh

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

    # messages.values() for each partition
    for message in messages.values():
        # actual message objects
        for sub_message in message:
            data.append(sub_message.value)

    return data

def plot_bar_chart(data):
    x = data['candidate_name']

    colors = plt.cm.viridis(np.linspace(0, 1, len(x)))
    plt.bar(x, data['total_votes'], color=colors)
    plt.xlabel('Candidate')
    plt.ylabel('Total Votes')
    plt.title('Vote Count per Candidate')
    plt.xticks(rotation=90)
    return plt

def plot_donut_chart(data):
    labels = list(data['candidate_name'])
    sizes = list(data['total_votes'])

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    plt.title('Candidate Votes')
    return fig

def sidebar():
    if st.session_state.get('latest_update') is None:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider('Refresh interval (seconds)', 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key='auto')

    # if st.sidebar.button('Refresh Data'):
    #     update_data()

@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df.loc[i: i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df

def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=1, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio(
                "Direction", options=["⬆️", "⬇️"], horizontal=True
            )
        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
        )
    pagination = st.container()

    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = (
            int(len(table_data) / batch_size) if int(len(table_data) / batch_size) > 0 else 1
        )
        current_page = st.number_input(
            "Page", min_value=1, max_value=total_pages, step=1
        )
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}** ")

    pages = split_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)

def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    voters_count, candidates_count = fetch_voting_stats()

    total_votes_consumer = create_kafka_consumer('aggregated_total_votes')
    total_votes = fetch_data_from_kafka(total_votes_consumer)
    total_votes_df = pd.DataFrame(total_votes)

    st.markdown("""---""")
    col1, col2, col3 = st.columns(3)
    col1.metric('Total Voters', voters_count)
    col2.metric('Total Candidates', candidates_count)
    col3.metric('Current # of Votes', total_votes_df.iloc[-1])

    votes_per_candidate_consumer = create_kafka_consumer('aggregated_votes_per_candidate')
    votes_per_candidate_data = fetch_data_from_kafka(votes_per_candidate_consumer)
    votes_per_candidate_df = pd.DataFrame(votes_per_candidate_data)

    votes_per_candidate_df = votes_per_candidate_df.loc[votes_per_candidate_df.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = votes_per_candidate_df.loc[votes_per_candidate_df['total_votes'].idxmax()]

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
    votes_per_candidate_df = votes_per_candidate_df[['candidate_id', 'candidate_name', 'party', 'total_votes']]
    votes_per_candidate_df = votes_per_candidate_df.reset_index(drop=True)

    col1, col2 = st.columns(2)

    with col1:
        bar_fig = plot_bar_chart(votes_per_candidate_df)
        st.pyplot(bar_fig)
    
    with col2:
        donut_fig = plot_donut_chart(votes_per_candidate_df)
        st.pyplot(donut_fig)

    st.table(votes_per_candidate_df)

    location_consumer = create_kafka_consumer('aggregated_turnout_by_location')
    location_data = fetch_data_from_kafka(location_consumer)
    location_result = pd.DataFrame(location_data)

    location_result = location_result.loc[location_result.groupby('state')['count'].idxmax()]
    location_result = location_result.reset_index(drop=True)

    st.header('Location of Voters')
    paginate_table(location_result)

    st.session_state['last_update'] = time.time()

st.title('Realtime Election Voting Dashboard')

sidebar()
update_data()

