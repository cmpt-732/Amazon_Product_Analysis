import pandas as pd

import streamlit as st
import streamlit_wordcloud as wordcloud
import re

def office():
    pd_df = pd.read_json('office_keywords.json')
    render(pd_df)

def movies():
    pd_df = pd.read_json('movies_keywords.json')
    render(pd_df)

def clothing():
    pd_df = pd.read_json('clothing_keywords.json')
    render(pd_df)

def render(pd_df):

    rows = pd_df.values
    pairs=[]
    for row in rows:
        for item in row:
            for i in item:
                pairs.append(i)

    keywords_dict = {}
    for item in pairs:
        score = item[0]*10
        keyword = item[1]
        keyword = keyword.lower()
        if not bool(re.match(r'^[a-zA-Z\s]+$', keyword)):
            continue
        if keyword in keywords_dict:
            keywords_dict[keyword] += score
        else:
            keywords_dict[keyword] = score
    sorted_keyword_dict = dict(sorted(keywords_dict.items(), key=lambda item: item[1], reverse=True))

    keys = list(sorted_keyword_dict.keys())

    values = list(sorted_keyword_dict.values())
    words = []
    for i in range(100):
        wordcloud_item = dict(text=keys[i], value=values[i])
        words.append(wordcloud_item)

    wordcloud.visualize(words, per_word_coloring=False, width='100%', height='100%', padding=3, max_words=80)

if __name__ == "__main__":
    render()