import pandas as pd
from plotly import express as px
import streamlit as st

def render():
    st.write("""Below is the visualised analysis of the datasets. Since our aim is to help both the customers as well as the sellers.
             Click the respective buttons to view the analysis for sellers and customers.
             We have also done an analysis for the three categories respectively. Choose any category from the side bar to view the
             analysis. 
             """)
    st.divider()
    st.header("Customer preference based on the 3 categories")
    st.divider()
    pd_df = pd.read_csv('frequencies.csv')
    st.markdown("Choose the number of customers from the slider and then click the button to view.")
    num = st.slider("Choose a number", 0, 1000, key = "sl-1")
    button3 = st.button("Seller Analytics", key = "bt-1")
    dfUpdate = pd_df.head(num)
    if button3:
        fig = px.pie(dfUpdate,values = 'max_value', names = 'most_often', title='Categories', hole = 0.5 ,height = 1000)
        st.plotly_chart(fig)