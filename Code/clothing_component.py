import pandas as pd
from plotly import express as px
import streamlit as st
def showdataframe():
    col1, col2 = st.columns(2)
    df1 = pd.read_csv("weighted_df.csv")
    df1 = df1.loc[:, ~df1.columns.str.contains('^Unnamed')]
    col1.dataframe(df1, width = 1800)

    df2 = pd.read_csv("clothing.csv")
    df2 = df2.loc[:, ~df2.columns.str.contains('^Unnamed')]
    col2.dataframe(df2)

def render():
    weighted = pd.read_csv("weighted_df.csv")
    pd_df = pd.read_csv('clothing.csv')
    fig = px.pie(pd_df, values = 'Num_purchases', names = 'product_name' ,title = 'Top 100 Customer Preferences in Clothing, Shoes and Jewelery Category', height = 1000, width = 2000)
    st.header("Top 100 Customer Preferences in Clothing, Shoes and Jewelery Category")
    st.plotly_chart(fig)

    fig1 = px.scatter(weighted, x = 'final_weighted_avg', y = 'product_name' ,title = 'Products to recommend',height=1000, width=1800)
    fig1.update_layout(xaxis_title = 'Weighted Average', yaxis_title = 'Products')
    st.plotly_chart(fig1)