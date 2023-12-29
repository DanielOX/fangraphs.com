import json, os 
import pandas as pd 
from bs4 import BeautifulSoup
import concurrent.futures

years = [i for i in range(1871, 2024)]
import os 

def process(year):
    current_path = os.path.join(os.getcwd(),f'{year}.txt')
    os.system(f"""wget https://www.fangraphs.com/leaders/war?season={year} -O {current_path}""")
    data = open(f'./{year}.txt')
    soup = BeautifulSoup(data, 'html.parser')
    _ = soup.find("script",{"id":"__NEXT_DATA__"}).get_text()
    _ = json.loads(_)
    __ = _['props']['pageProps']['dehydratedState']['queries'][0]['state']['data']
    df = pd.DataFrame(__).drop_duplicates()
    df.to_csv(f'output/{year}.csv',index=False)
    os.unlink(current_path)

with concurrent.futures.ThreadPoolExecutor(max_workers=128) as executor:
    executor.map(process,years)