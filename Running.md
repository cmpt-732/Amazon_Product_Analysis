# Instructions to test the code:

## Overview

The dataset we have used is quite large and can't be uploaded to the GitHub repository since it is exceeding the maximum limit. So we have provided,
the link to the data sets: [Drive Link]().

## Setup
After downloading the Dataset folder.

1. Streamlit: It's important to install this package so that we can use this for building the dashboard

    ``` pip install streamlit ```

2. Plotly: Used for visualization in our project

    ```pip install plotly```

3. Pandas
  ```pip install pandas```

4. streamlit_wordcloud: Used to build the wordcloud
   ```pip install streamlit-wordcloud```

5. Since we are extracting the keywords from the review text we have used rake_nltk from Rake. So to run this we need to create a file named: "run.py".
   In this file copy the below python code and run it.
   ```
   from rake_nltk import Rake
   import nltk
   import ssl
   try:
     _create_unverified_https_context = ssl._create_unverified_context
   except AttributeError:
      pass
   else:
    ssl._create_default_https_context = _create_unverified_https_context
   nltk.download('stopwords')
   nltk.download('punkt')```

## Usage
1. Download the Github repository and navigate to the Code directory.
2. Then navigate to the Recommendations folder and then further navigate to Product_Appraisal folder. Then run the 3 python files as mentioned below:
   For Clothing_shoes_jewel.py file we need to run the following command ```spark-submit Clothing_shoes_jewel.py```. Then do the same for the other
   2 files namely: Movies_Tv.py, and Office_products.py. Note: It will take some 5-7 mins to run each code since every dataset is at least 1 Gb. These files will
   produce csv files as the output. Store that in a folder named "results" or whichever name you prefer. Since we will use these csv files for visualzations. 
4. After running these 3 files, change the directory to "Customer_Prefernces".Then run the python file with the following command:
   ```spark-submit category_frequency.py```. This will produce an output file that is again to be stored in the results folder.
5. Change the directory to Analyzation. Then again change the directory to Prediction. Run the three files using the ```spark-submit <filename>``` command.
   and make sure that you are storing the output csv files in the results folderm like we are doing in the above steps.
7. Now we will change the directory to extract the keywords. So change the directory to Keywords_Extraction. Now run all the 3 files using the spark-submit command and store the output json files in results folder.
8. Now comes the final step where we are visualizing the data from the csv files and the json files stored in the results folder. So to do this
   change the directory to Visualization folder. and run the following command:  ``` streamlit run app.py ```

In the end you will be redirected to the dashboard. 

