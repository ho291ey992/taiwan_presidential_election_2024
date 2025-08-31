# conda activate taiwan_presidential_election_2024
# C:\Users\Steve\anaconda3\envs\taiwan_presidential_election_2024\python.exe

import pandas as pd
import numpy as np
import sqlite3

# 餘弦相似度 計算
A = np.array([0.4, 0.3, 0.3])
B = np.array([0.42, 0.28, 0.30])

cos_sim = np.dot(A, B) / (np.linalg.norm(A) * np.linalg.norm(B))

# ---------------------------------------------------------------------------------------------------------------------

# 讀取資料表 votes_by_village
file_path = "練習專案四：找出章魚里/taiwan_presidential_election_2024/data/taiwan_presidential_election_2024_1.db"
connection = sqlite3.connect(file_path)
votes_by_village = pd.read_sql('select * from votes_by_village', con=connection)
connection.close()

# 計算全國得票率
total_votes = votes_by_village['sum_votes'].sum()
country_percentage = votes_by_village.groupby(['candidate_id' ,'candidate']).sum() / total_votes
vector_a = country_percentage.values

# 計算各區人數
village_all = votes_by_village.groupby(['county' ,'town' ,'village']).sum().reset_index()

# 轉寬
votes_by_village = votes_by_village.pivot(index=['county' ,'town' ,'village'], columns='candidate_id', values='sum_votes').reset_index().reset_index(drop=True)

test = votes_by_village.iloc[1,[3,4,5]].values