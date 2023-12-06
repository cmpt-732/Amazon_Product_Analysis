import pandas as pd
from plotly import express as px
import streamlit as st

def render():
    pd_df = pd.read_csv('frequencies.csv')
    st.header("Customer preference based on the 3 categories")
    num = st.slider("Pick a number", 0, 1000)
    dfUpdate = pd_df.head(num)
    fig = px.pie(dfUpdate,values = 'max_value', names = 'most_often', title='Categories', hole = 0.5 ,height = 1000)
    st.plotly_chart(fig)