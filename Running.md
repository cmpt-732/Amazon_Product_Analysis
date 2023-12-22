# Instructions to test the code:

## Overview

The dataset we have used is quite large and can't be uploaded to the GitHub repository since it is exceeding the maximum limit. So we have provided,
the link to the data sets: [Link](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/).

## Setup
 Downloading the Dataset:
Since we are using a dataset of total size of 27Gb we have provided the link to the dataset. So we are using 3 categories:
a) Clothing,Shoes,and Jewelry b) Office products, c) Movies and Tv. So First download the datasets. For reference here is a screen shot from where data has to be downloaded. 
We will  be downloading the reviews data as well as the meta data for that category:
![](https://github.com/cmpt-732/Amazon_Product_Analysis/assets/54028832/4b073811-b4bf-4a8b-9cf2-a68828c64ff8)
Download these files and store them in a folder named: Dataset.
Then install the following packages:
1. Streamlit: It's important to install this package so that we can use this for building the dashboard

    ``` pip install streamlit ```

2. Plotly: Used for visualization in our project

    ```pip install plotly```

3. Pandas
  ```pip install pandas```

4. streamlit_wordcloud: Used to build the wordcloud, make sure that you have python >= 3.6.0
   ```pip install streamlit-wordcloud```

5. Install rake: since we are extracting the keywords from the review text we have used rake_nltk from Rake. ``` pip install rake-nltk ```
6.  After installing rake, run the code as shown below. We need to create a file named: "run.py" and then run the given code.
   In this file copy the below python code and run it.
   ```
import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('stopwords')
nltk.download('punkt')

   ```

## Running the project
1. Download the Github repository and ```cd Amazon_Product_Analysis_main```.
2. Make sure you run the following commands under path ```Amazon_Product_Analysis_main/```. Add a folder for holding our intermediate results by    running ```mkdir Results``` Then run the 3 python files as mentioned below:
   For Clothing_shoes_jewel.py file we need to run the following command ```spark-submit Code/Recommendation/Product_Appraisal/Clothes_shoes_jewel.py```. 
   Then do the same for the other 2 files namely: Movies_Tv.py, and Office_products.py. Note: It will take some 5-7 mins to run each code since every dataset is at least 1 Gb. These files will produce csv files as the output. Create a folder named "results" or whichever name you prefer and store the output csv files in that folder    there. We will use these csv files for visualzations. 
4. After running these 3 files, run the python file with the following command:
   ```spark-submit Code/Recommendation/Customer_Prefernces/category_frequency.py```. This will produce an output file that is again to be stored in the results folder.
5. Run the three files in prediction folder using the ```spark-submit Code/Analyzation/Prediction/ml_<category name>.py``` command.
   and make sure that you are storing the output csv files in the results folder like we are doing in the above steps.
7. Now we will extract the keywords. Run all the 3 files using the spark-submit command and store the output json files in results folder.
8. Now comes the final step where we are visualizing the data from the csv files and the json files stored in the results folder. So to do this
   change the directory to Visualization folder. and run the following command:  ``` streamlit run Code/Visualization/app.py ```

In the end you will be redirected to the dashboard. 

