import streamlit as st
import frequency_component, clothing_component, movie_component, office_component
st.set_page_config(layout="wide")
def main():
    st.title("Amazon Products Dashboard")
    st.write("This is an analysis of the sales on Amazon in order to make more precise, more customized and better recommendations to customers and improve the products selling on the platform.")
    st.write("We are performing this analysis on 5-core subset with the size of 14.3gb. 5-core subset makes sure all users and items have at least 5 reviews (75.26 million reviews).")
    st.write("So we are using 3 categories here: 1) Office Products, 2) Clothing, Shoes and Jewelry,3) Movies and Tv")  

    st.header("Here is some info about the processed data")
    st.subheader("Office Products")
    office_component.showdataframe()

    st.subheader("Clothing, Shoes, and Jewelry")
    clothing_component.showdataframe()

    st.subheader("Movies and Tv")
    movie_component.showdataframe()
    st.divider()
    frequency_component.render()
    selected_component = st.sidebar.selectbox("Select the category", ["Pick one category","Clothing", "Movies", "Office"])
    
    if selected_component == "Clothing":
        clothing_component.render()
    elif selected_component == "Movies":
        movie_component.render()
    elif selected_component == "Office":
        office_component.render()
    
    
if __name__ == "__main__":
    main()
