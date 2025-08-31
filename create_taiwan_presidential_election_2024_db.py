# conda activate taiwan_presidential_election_2024
# C:\Users\Steve\anaconda3\envs\taiwan_presidential_election_2024\python.exe
import pandas as pd
import os
import sqlite3
import re

class CreateTaiwanPresidentialElection2024DB:
    def __init__(self, file_path, db_name):
        self.file_path = file_path
        self.db_name = db_name
        polling_county = os.listdir(self.file_path)
        county_name = []
        for i in polling_county:
            if '.xlsx' in i:
                county_name += [re.split('\\(|\\)', i)[1]]
        self.county_name = county_name

    def tidy_county_dataframe(self, county_name):
        # 單一文件處理
        # county_name = "臺北市"
        county_file_path = f'{self.file_path}總統-A05-4-候選人得票數一覽表-各投開票所({county_name}).xlsx'
        df = pd.read_excel(county_file_path)

        # 移出多於列
        df = df.iloc[:,:6]

        # 刪除多於欄位
        df = df.drop([2,3,4])

        # 補齊縣市資料
        df.iloc[:,0] = df.iloc[:,0].ffill()

        # 修改欄位名稱
        columns = ['town', 'village', 'polling_place']  + df.iloc[1,3:].to_list()
        df.columns = columns

        # 刪除 nan 欄位(縣市統計(town)、前兩列資料)
        df = df.dropna().reset_index(drop=True)

        # 新增縣市欄位
        df['county'] = county_name

        # 調整開票所資料型態
        df['polling_place'] = df['polling_place'].astype(int)

        # 寬資料表 轉 長資料表
        df = pd.melt(df, id_vars=['county', 'town', 'village', 'polling_place'], var_name="candidate_info", value_name="votes")

        return (df)

    def concat_country_dataframe(self):
        # 合併各縣市資料
        country_data = pd.DataFrame()
        for c in self.county_name:
            county_data = self.tidy_county_dataframe(c)
            country_data = pd.concat([country_data, county_data])

        # 分割 候選人編號(candidate_id) 資料
        country_data['candidate_id'] = country_data['candidate_info'].map(lambda i: re.split("\\(|\\)",i)[1])

        # 分割 候選人名稱(candidate) 資料
        country_data['candidate'] = country_data['candidate_info'].map(lambda i: re.split(r"\n|\(|\)",i)[3] + "/" + re.split(r"\n|\(|\)",i)[4])
        
        # 多餘空白去除
        country_data.loc[:, "town"] = country_data["town"].str.strip()

        return country_data.drop(columns='candidate_info')

    def create_database(self):
        country_data = self.concat_country_dataframe()

        # 建立 candidates (candidate_id, candidate)
        candidates = country_data[['candidate_id', 'candidate']].drop_duplicates().reset_index(drop=True).copy()

        # 建立 polling_places (polling_place_id, county, town, village, polling_place)
        polling_places = country_data[['county', 'town', 'village', 'polling_place']].drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index':'polling_place_id'}).copy()

        # 建立 votes (polling_place_id, candideate_id, votes)
        join = ['county', 'town', 'village', 'polling_place']
        votes = pd.merge(country_data, polling_places, left_on=join, right_on=join, how='left')
        votes = votes[['polling_place_id', 'candidate_id', 'votes']]

        # 建立 SQL
        connection = sqlite3.connect(f'{self.file_path}/{self.db_name}')

        # 建立資表 (name=資料表名稱, con=檔案位置, index=是否載入 pd 排, if_exists="replace" 若同名檔案存在蓋)
        candidates.to_sql(name='candidates', con=connection, index=False, if_exists="replace") 
        polling_places.to_sql(name='polling_places', con=connection, index=False, if_exists="replace") 
        votes.to_sql(name='votes', con=connection, index=False, if_exists="replace") 

        # 建立SQL指令
        cur = connection.cursor()

        # 檢查 votes_by_village 是否重複
        drop_view_sql = """
        drop view if exists votes_by_village
        """
        # 建立 votes_by_village 
        create_view_sql = """
        create view votes_by_village as
        select pp.county,
        	   pp.town,
        	   pp.village,
        	   c.candidate_id,
        	   c.candidate,
        	   sum(v.votes) as sum_votes
         from votes v 
         join polling_places pp 
           on v.polling_place_id = pp.polling_place_id
         join candidates c 
           on v.candidate_id = c.candidate_id
         group by pp.county,
         		  pp.town,
         		  pp.village,
         		  c.candidate_id
        """
        cur.execute(drop_view_sql)
        cur.execute(create_view_sql)
        connection.close()

file_path = 'data/'
db_name = 'taiwan_presidential_election_2024.db'
db = CreateTaiwanPresidentialElection2024DB(file_path, db_name)
db.create_database()

