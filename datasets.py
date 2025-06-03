import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

channel= "google_takeout/c_profile4/channels/channel.csv"
comments= "google_takeout/c_profile4/comments/comments.csv"
search_history= "google_takeout/c_profile4/history/search-history.html"
watch_history="google_takeout/c_profile4/history/watch-history.html"

channel_df = pd.read_csv(channel)
comments_df = pd.read_csv(comments)

def parse_date_time(date_str) -> tuple:
    cleaned = date_str.replace(' GMT-03:00', '').replace('\u202f', '').strip()
    dt = datetime.strptime(cleaned, '%B %d, %Y, %I:%M:%S%p')
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
                    'Query': p_info[1],
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
    for item in cell_info:
        print(item)
    return cell_info
        
        
# possui uma unica linha dai posso usar iloc
comments_df['Profile Name/ID'] = channel_df['Channel Title (Original)'].iloc[0] + " " + channel_df['Channel ID'].iloc[0]

prof4_df = comments_df[['Profile Name/ID', 'Video ID', 'Comment Create Timestamp', 'Comment Text']]
print(prof4_df.head())
l = get_historic_info(hist_type='search')
# for i in l:
#     print(i)