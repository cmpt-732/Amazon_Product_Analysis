import pandas as pd
from plotly import express as px
import streamlit as st

def render():
    weighted = pd.read_csv("weighted_df.csv")

    fig1 = px.scatter(weighted, x = 'final_weighted_avg', y = 'product_name' ,title = 'Products to recommend',height=800, width=2000)
    fig1.update_layout(xaxis_title = 'Weighted Average', yaxis_title = 'Products')
    st.plotly_chart(fig1)
    
    pd_df = pd.read_csv('clothing.csv')
    fig = px.pie(pd_df, values = 'Num_purchases', names = 'product_name' ,title = 'Top 100 Customer Preferences in Clothing, Shoes and Jewelery Category', height = 1000, width = 2000)
    st.title("chart for clothing")
    st.plotly_chart(fig)