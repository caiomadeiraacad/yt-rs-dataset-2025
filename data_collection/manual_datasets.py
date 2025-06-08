import pandas as pd
import numpy as np
from web_scrap import extract_meta
import asyncio

g_profile1 = pd.read_csv("dataframe/g_profile1/g_profile1_final.csv")
print(g_profile1.head())

j_profile2 = pd.read_csv("dataframe/j_profile2/j_profile2_final.csv")
print(j_profile2.head())

def insert_tags(df, meta):
    keywords = []
    for id in df['Video ID']:
        url_video = f"https://www.youtube.com/watch?v={id}"
        keywords.append(asyncio.run(extract_meta(url_video)))
        print("Keywords do vídeo:", keywords) 
    if meta == 'keywords':       
        df['Tags'] = keywords
    elif meta == 'title':
        df['Title'] = keywords

# tags
insert_tags(g_profile1, meta='keywords')
insert_tags(j_profile2, meta='keywords')
print(g_profile1.head())
print(j_profile2.head())

# title
insert_tags(g_profile1, meta='title')
insert_tags(j_profile2, meta='title')
print(g_profile1.head())
print(j_profile2.head())

g_profile1.to_csv("dataframe/g_profile1/g_profile1_final.csv", index=False)
j_profile2.to_csv("dataframe/j_profile2/j_profile2_final.csv", index=False)