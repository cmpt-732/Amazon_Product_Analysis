import pandas as pd
from plotly import express as px
import streamlit as st
def showdataframe():
    col1, col2 = st.columns(2)
    df1 = pd.read_csv("WeightedDf1.csv")
    df1 = df1.loc[:, ~df1.columns.str.contains('^Unnamed')]
    col1.dataframe(df1, width=1800)

    df2 = pd.read_csv("clothing.csv")
    df2 = df2.loc[:, ~df2.columns.str.contains('^Unnamed')]
    col2.dataframe(df2,width=1800)

def render():
    weighted = pd.read_csv("sorteddfPur2.csv")
    fig1 = px.scatter(weighted, x = 'final_weighted_avg', y = 'product_name' ,title = 'Products to recommend',height=800, width=2000)
    fig1.update_layout(xaxis_title = 'Weighted Average', yaxis_title = 'Products')
    st.plotly_chart(fig1)
    pd_df = pd.read_csv('sorteddfPur2.csv')
    fig = px.pie(pd_df, values = 'Num_purchases', names = 'product_name' ,title = 'Top 100 Customer Preferences in Movies and TV', height = 1000, width = 2000)
    st.header("Top 100 Customer Preferences in Movies and TV")
    st.plotly_chart(fig)