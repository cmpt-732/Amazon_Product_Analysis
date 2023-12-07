import pandas as pd
from plotly import express as px
import streamlit as st
import streamlit_wordcloud as wordcloud
import re

def showdataframe():
    col1, col2 = st.columns(2)
    df1 = pd.read_csv("WeightedDf1.csv")
    df1 = df1.loc[:, ~df1.columns.str.contains('^Unnamed')]
    col1.dataframe(df1, width=1800)

    df2 = pd.read_csv("sorteddfPur2.csv")
    df2 = df2.loc[:, ~df2.columns.str.contains('^Unnamed')]
    col2.dataframe(df2,width=1800)

def render():
    st.subheader("Click the buttons to view the analysis for seller and customer")
    col1, col2 = st.columns(2)
    button1 = col1.button("Seller Analytics", key = "bt-4")
    button2 = col2.button("Customer Analytics", key = "bt-5")
    if button1:
        pd_df = pd.read_csv('sorteddfPur2.csv')
        fig = px.pie(pd_df, values = 'Num_purchases', names = 'product_name' ,title = 'Top 100 Customer Preferences in Movies and TV', height = 1000, width = 2000)
        st.header("Top 100 Customer Preferences in Movies and TV")
        st.plotly_chart(fig)
        st.divider()
        st.subheader("Predicted Number of purchases by customers")
        df3 = pd.read_csv('ml_movies.csv')
        fig1 = px.histogram(df3, x = 'num_purchase', color = 'type', histnorm = 'percent')
        st.plotly_chart(fig1)
    elif button2:
       df = pd.read_csv('WeightedDf1.csv')
       fig3 = px.scatter(df, x = 'final_weighted_avg', y = 'product_name' ,title = 'Movies to recommend',height=800, width=2000)
       fig3.update_layout(xaxis_title = 'Weighted Average', yaxis_title = 'Products')
       fig3.update_layout(margin=dict(l=50, r=50, t=50, b=50))
       st.header("Movies to recommend based on the weighted average")
       st.plotly_chart(fig3)
       st.divider()
    st.subheader("More Insights from the dataset")
    dfN = pd.read_json('movies_keywords.json')
    rows = dfN.values
    pairs=[]
    for row in rows:
        for item in row:
            for i in item:
                pairs.append(i)

    keywords_dict = {}
    for item in pairs:
        score = item[0]*10
        keyword = item[1]
        keyword = keyword.lower()
        if not bool(re.match(r'^[a-zA-Z\s]+$', keyword)):
            continue
        if keyword in keywords_dict:
            keywords_dict[keyword] += score
        else:
            keywords_dict[keyword] = score
    sorted_keyword_dict = dict(sorted(keywords_dict.items(), key=lambda item: item[1], reverse=True))

    keys = list(sorted_keyword_dict.keys())

    values = list(sorted_keyword_dict.values())
    words = []
    for i in range(100):
        wordcloud_item = dict(text=keys[i], value=values[i])
        words.append(wordcloud_item)

    wordcloud.visualize(words, per_word_coloring=False, width='100%', height='100%', padding=3, max_words=80)