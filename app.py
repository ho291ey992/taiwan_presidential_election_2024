import pandas as pd
import numpy as np
import sqlite3
import gradio as gr

class TaiwanPresidentalElection2024():
    def __init__(self, file_path):
        self.file_path = file_path

    # 餘弦相似度
    def create_gradio_dataframe(self):
        # 讀取資料表 votes_by_village
        connection = sqlite3.connect(self.file_path)
        votes_by_village = pd.read_sql('select * from votes_by_village', con=connection)
        connection.close()

        # 計算全國得票率 A
        total_votes = votes_by_village['sum_votes'].sum()
        country_percentage = votes_by_village.iloc[:,3:].groupby(['candidate_id' ,'candidate']).sum() / total_votes
        vector_a = country_percentage.reset_index()['sum_votes'].tolist()

        # 計算村鄰里總票數
        village_all = votes_by_village.iloc[:,[0,1,2,5]].groupby(['county' ,'town' ,'village']).sum().reset_index()

        # 合併
        merge_list = ['county' ,'town' ,'village']
        merged = pd.merge(votes_by_village, village_all, left_on=merge_list, right_on=merge_list, how='left')

        # 計算村鄰里得票率 B
        merged["village_percentage"] = merged['sum_votes_x'] / merged['sum_votes_y']

        # 轉寬
        pivot_df = merged.pivot(index=merge_list, columns='candidate_id', values='village_percentage').reset_index()
        pivot_df.rename_axis(None, axis=1, inplace=True)

        # 計算餘弦相似度
        pivot_df['cosine_similarities'] = pivot_df.apply(lambda x: np.dot(vector_a, x.tolist()[3:]) / (np.linalg.norm(vector_a) * np.linalg.norm(x.tolist()[3:])), axis=1)

        # 建立最後的資料框
        cosine_similarity_df = pivot_df.copy()
        sort_values_list = ['cosine_similarities','county' ,'town' ,'village']
        cosine_similarity_df = cosine_similarity_df.sort_values(sort_values_list, ascending=[False, True, True, True]) # 排序
        cosine_similarity_df = cosine_similarity_df.reset_index(drop=True).reset_index()
        cosine_similarity_df["index"] = cosine_similarity_df["index"] + 1 # 建立 rank
        column_names_to_revise = {"index": "rank",
                                '1': "candidate_1",
                                '2': "candidate_2",
                                '3': "candidate_3"}
        cosine_similarity_df = cosine_similarity_df.rename(columns=column_names_to_revise)
        return vector_a, cosine_similarity_df

    # 篩選
    def filter_county_town_village(self, df, county_name, town_name, village_name):
        county_condition = df["county"] == county_name if len(county_name) > 0 else True
        town_condition = df["town"] == town_name if len(town_name) > 0 else True
        village_condition = df["village"] == village_name if len(village_name) > 0 else True
        return df[county_condition & town_condition & village_condition]

    # 建立網頁
    def create_web(self):
        country_percentage, gradio_dataframe = self.create_gradio_dataframe()
        # 調整小數點到第6位
        gradio_dataframe.loc[:,['candidate_1', 'candidate_2','candidate_3', 'cosine_similarities']] = gradio_dataframe.loc[:,['candidate_1', 'candidate_2','candidate_3', 'cosine_similarities']].round(6)
        # 猜分各
        ko_wu, lai_hsiao, hou_chao = country_percentage

        interface = gr.Interface(fn=self.filter_county_town_village,
                                inputs=[gr.DataFrame(gradio_dataframe),
                                        'text',
                                        'text',
                                        'text'],
                                outputs='dataframe',
                                title='找出章魚里',
                                description=f'輸入你想篩選的縣市、鄉鎮市區與村鄰里。( 柯吳配, 賴蕭配, 候趙配 )=( {ko_wu:.6f}, {lai_hsiao:.6f}, {hou_chao:.6f}) ')
        # 啟動網頁是伺服器，關閉 Ctrl + Z or interface.close()。
        interface.launch()

file_path = 'data/taiwan_presidential_election_2024.db'
test = TaiwanPresidentalElection2024(file_path)
test.create_web()
