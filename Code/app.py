import streamlit as st
import frequency_component, clothing_component, movie_component

def main():
    st.title("Streamlit Dashboard")

    # Add navigation or layout logic
    selected_component = st.sidebar.selectbox("Select Component", ["Frequency", "Clothing", "Movies"])

    if selected_component == "Frequency":
        frequency_component.render()
    elif selected_component == "Clothing":
        clothing_component.render()
    elif selected_component == "Movies":
        movie_component.render()

if __name__ == "__main__":
    main()