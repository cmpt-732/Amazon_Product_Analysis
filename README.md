## Demo

The video/live demo for this project can been viewed here: 
[Video Demonstration](https://www.youtube.com/watch?v=-G7sMxim-Cg)

## Overview

This is the project for SFU's CMPT 732 (Big Data I) course, with the goal of analyzing the sales on Amazon in order to make more precise, more
customized and better recommendations to customers and improve the products selling on the platform. 

Notes on how to run the code can be found in RUNNING.md, and a more detailed overview can be found in the project report (under Documents).
## Datasets: 
We have used the Amazon Review Data 2018 data. Here we have used the 5-core subset and the metadata for the categories: 1) Clothing, Shoes, and Jewelry  2) Movies and Tv 3) Office Products. 
Link to the datasets: [Dataset](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/)

## Structure

```

|-- Code
    |--Analyzation
        |-- Keyword Extraction
            |-- keywords-clothing.py
            |-- keywords-movies.py
            |-- keywords-office.py
        |-- Perdiction
            |-- ml-clothing.py
            |-- ml-movies.py
            |-- ml-office.py
    |--Recommendation
        |-- Product Appraisal
            |-- Clothes_shoes_jewel.py
            |-- Movies_Tv.py
            |-- Office_products.py
        |-- Customer Preference
            |-- category_frequency.py
    |--Visualization
        |-- app.py
        |-- clothing_component.py
        |-- frequency_component.py
        |-- movie_component.py
        |-- office_component.py

|-- Documents
    |-- Project Proposal
    |-- Project Report
|-- README.md
|-- RUNNING.md
```


## Team Members

- Zhenmin He, 301589203
- Hershil Piplani, 301591594
- Houli Huang, 301593424
