import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import random
from web_scrap import extract_meta
import asyncio
from dotenv import load_dotenv
import os
import dateparser
from video_evaluation import profile4_video_evaluation, profile3_video_evaluation

DEBUG=False
if not DEBUG:
    GET_TAGS=True
else:
    GET_TAGS=False

if DEBUG:
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
 
profile = int(input("[4] profile 4 (caio)\n[3] profile 3 (pedro)\n[2] profile 2 (joao)\n[1] profile 1 (giovanni)\n"))

profile_type = ""
save_path = ""

# channels.csv
CHANNEL_ID = ""
CHANNEL_TITLE = ""

# comments.csv
DATE_TIME_COMMENT = ""
VIDEO_ID = ""
COMMENT_TEXT = ""
profile_evaluation = {  }

if profile == 4:
    profile_type = "c_profile4/updated"
    save_path = 'dataframe/c_profile4/c_profile4_final.csv'
    CHANNEL_ID = "Channel ID"
    CHANNEL_TITLE = "Channel Title (Original)"
    DATE_TIME_COMMENT = "Comment Create Timestamp"
    VIDEO_ID = "Video ID"
    COMMENT_TEXT = "Comment Text"
    profile_evaluation = profile4_video_evaluation
    
elif profile == 3:
    profile_type = "p_profile3"
    save_path = f'dataframe/{profile_type}/{profile_type}_final.csv'
    CHANNEL_ID = "ID do canal"
    CHANNEL_TITLE = "Título do canal (Original)"
    DATE_TIME_COMMENT = "Carimbo de data/hora em que o comentário foi criado"
    VIDEO_ID = "ID do vídeo"
    COMMENT_TEXT = "Texto do comentário"
    profile_evaluation = profile3_video_evaluation

elif profile == 2:
    profile_type = "j_profile2"
    save_path = f'dataframe/{profile_type}/{profile_type}_final.csv'
    
elif profile == 1:
    profile_type = "g_profile1"
    save_path = f'dataframe/{profile_type}/{profile_type}_final.csv'
    
else:
    raise Exception("Invalid option.")

channel= f"google_takeout/{profile_type}/channels/channel.csv"
comments= f"google_takeout/{profile_type}/comments/comments.csv"
search_history= f"google_takeout/{profile_type}/history/search-history.html"
watch_history= f"google_takeout/{profile_type}/history/watch-history.html"

channel_df = pd.read_csv(channel)
comments_df = pd.read_csv(comments)

def parse_date_time(date_str) -> tuple:
    cleaned = date_str.replace(' GMT-03:00', '').replace('\u202f', '').strip()
    #print(cleaned)
    if 'AM' in cleaned or 'PM' in cleaned:
        #print("en")
        dt = dateparser.parse(cleaned, languages=['en'])
    elif 'BRT' in cleaned:
        dt = dateparser.parse(cleaned, languages=['pt'])
    else:
        raise ValueError("Date language not identified.")
    if dt is None:
        raise ValueError(f"Erro ao parsear data: {cleaned}")
    date_formatted = dt.strftime('%d-%m-%Y')  # ex. 25-05-2025
    time_formatted = dt.strftime('%H:%M:%S')  # ex. 01:03:32
    return date_formatted, time_formatted


def get_historic_info(hist_type='search'):
    cell_info = []
    filename = ""
    if hist_type == 'search':
        filename = search_history
    elif hist_type == 'watch':
        filename = watch_history
    else:
        raise Exception("Invalid hist_type.")
    
    with open(filename, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    for info in soup.find_all('div', class_='outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp'):
        personal_info = info.find('div', class_='content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1')
        google_info = info.find('div', class_='content-cell mdl-cell mdl-cell--12-col mdl-typography--caption')
        # cell_info[f"register {count}"] = {"personal_info": list(personal_info.stripped_strings), "google_info": list(google_info.stripped_strings)}
        # count = count + 1
        p_info = list(personal_info.stripped_strings)
        g_info = list(google_info.stripped_strings)
        
        if hist_type == 'search':
            if 'Searched for' in p_info:
                date_time = parse_date_time(p_info[2])
                row = {
                    'Action': p_info[0].replace(' for', ''),
                    'Text': p_info[1],
                    'Date': date_time[0],
                    'Time': date_time[1]
                }
                cell_info.append(row)
                
        if hist_type == 'watch':
            #print(p_info)
            a_tag = personal_info.find('a')
            if len(p_info) > 3:
                if 'From Google Ads' in g_info:
                    author_channel = "Google Ads"
                else:
                    author_channel = p_info[2]
                video_id = a_tag['href'].replace('https://www.youtube.com/watch?v=', '')
                date_time = parse_date_time(p_info[3])
                row = {
                    'Action': p_info[0],
                    'Title': p_info[1],
                    'Video ID': video_id,
                    'Author Channel': author_channel,
                    'Date': date_time[0],
                    'Time': date_time[1]
                }
                cell_info.append(row)
    return cell_info
        
        
# possui uma unica linha dai posso usar iloc[0]
comments_df['Profile Name/ID'] = channel_df[CHANNEL_TITLE].iloc[0] + " " + channel_df[CHANNEL_ID].iloc[0]

comments_df = comments_df[['Profile Name/ID', VIDEO_ID, DATE_TIME_COMMENT, COMMENT_TEXT]]
watch = get_historic_info(hist_type='watch')
search = get_historic_info(hist_type='search')

watch_df = pd.DataFrame(watch)
search_df = pd.DataFrame(search)
# criando coluna datetime
watch_df['Datetime'] = pd.to_datetime(watch_df['Date'] + ' ' + watch_df['Time'], format='%d-%m-%Y %H:%M:%S')
search_df['Datetime'] = pd.to_datetime(search_df['Date'] + ' ' + search_df['Time'], format='%d-%m-%Y %H:%M:%S')
# unindo os dois dfs
combined_df = pd.concat([watch_df, search_df], ignore_index=True)
combined_df = combined_df.sort_values(by='Datetime').reset_index(drop=True)
# removo a coluna do df
combined_df.drop(columns=['Datetime'], inplace=True)

comments_df[DATE_TIME_COMMENT] = pd.to_datetime(comments_df[DATE_TIME_COMMENT])
comments_df['Date'] = comments_df[DATE_TIME_COMMENT].dt.strftime('%d-%m-%Y')
comments_df['Time'] = comments_df[DATE_TIME_COMMENT].dt.strftime('%H:%M:%S')
comments_df.drop(columns=[DATE_TIME_COMMENT], inplace=True)

# removendo o {"text: "} do texto
comments_df[COMMENT_TEXT] = comments_df[COMMENT_TEXT].str.replace('{"text":"', '', regex=False).str.replace('"}', '', regex=False)

# renomeando colunas
comments_df = comments_df.rename(columns={COMMENT_TEXT: 'Interaction'})

# juntar coluna interaction com text
#profile_df = profile_df.merge(profile_df['Interaction'], profile_df['Text'])

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# Lidando com NaN
# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# repetir coluna de Profile Name/ID (virar coluna de valor constante)
profile_df = pd.concat([combined_df, comments_df], ignore_index=True)
profile_df['Profile Name/ID'] = profile_df['Profile Name/ID'].fillna(comments_df['Profile Name/ID'][0])
# fill nan em texto 
# profile_df['Text'] = profile_df['Text'] aqui vou ter que interagir olhando a tabela tambem pra poder preencher com o nivel da interacao
profile_df = profile_df.rename(columns={'Interaction': 'Comment'})
# profile_df['Like'] = np.random.randint(0, 1, size=len(profile_df))
# profile_df['Dislike'] = np.random.randint(0, 1, size=len(profile_df))

profile_df = (
    profile_df.groupby("Video ID", group_keys=False)
              .apply(lambda group: group.ffill().bfill())
              .reset_index(drop=True)
)

# drop text. The axis=1 argument specifies that we are dropping a column, not a row. By list of column names.
profile_df.drop('Text', axis=1, inplace=True)
# fill nan into was commented
was_commented_values = []
for comment in profile_df['Comment']:
    if type(comment) == str:
        was_commented_values.append(1)
    else:
        was_commented_values.append(0)

profile_df['Was commented'] = was_commented_values

# profile_df = profile_df[['Profile Name/ID', 'Video ID', 'Title', 'Author Channel', 'Action', 'Was commented', 'Like', 'Dislike', 'Comment', 'Date', 'Time']]
profile_df = profile_df[['Profile Name/ID', 'Video ID', 'Title', 'Author Channel', 'Action', 'Was commented','Comment', 'Date', 'Time']]
profile_df = profile_df.sort_values(by='Date').reset_index(drop=True)

if GET_TAGS:
    # get video tags
    keywords = []
    for id in profile_df['Video ID']:
        url_video = f"https://www.youtube.com/watch?v={id}"
        keywords.append(asyncio.run(extract_meta(url_video)))
        print("Keywords do vídeo:", keywords)        
    profile_df['Tags'] = keywords

# concat comments_df com combined_df = prof_df e da um sort values por date
profile_df['Date'] = pd.to_datetime(profile_df['Date'], format='%d-%m-%Y')
profile_df = profile_df.sort_values(by='Date', ascending=True).reset_index(drop=True)
profile_df['Date'] = profile_df['Date'].dt.strftime('%d-%m-%Y')

# setings likes dislike
profile_df['Rating'] = profile_df['Video ID'].map(
    lambda vid: profile_evaluation.get(f"https://www.youtube.com/watch?v={vid}", "-")
)

# substitute comment
profile_df['Comment'] = profile_df['Comment'].fillna('-')

# threat nan for search rows
searched_rows = profile_df.loc[profile_df['Action'] == 'Searched'].fillna('-')
print(searched_rows)

for i in profile_df['Video ID']:
    print(i)

if not DEBUG:
    profile_df.to_csv(save_path, index=False)
else:
    print(profile_df)
