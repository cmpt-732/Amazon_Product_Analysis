import pandas as pd
from plotly import express as px
import streamlit as st

def render():
    pd_df = pd.read_csv('frequencies.csv')
    fig = px.pie(pd_df,values = 'max_value', names = 'most_often', title='categories', hole = 0.5)
    st.title("pie chart for frequencies")
    st.plotly_chart(fig)