import pandas as pd
from plotly import express as px
import streamlit as st

def showdataframe():
    df_off = pd.read_csv("office.csv")
    st.dataframe(df_off)
    
def render():
    df_office = pd.read_csv("office.csv")
    fig3 = px.pie(df_office, values = 'Num_purchases', names = 'product_name', title = 'Top 100 Customer Preferences in Office Products Category', height = 1000, width = 2000)
    fig3.update_layout(legend = dict(y = 0.5,x=1.5))
    st.header("Top 100 Customer Preferences in Office Products")
    st.plotly_chart(fig3, height = 800)