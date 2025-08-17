import sqlite3
import pandas as pd
import numpy as np

connection=sqlite3.connect("data/create_taiwan_presidential_election_2024.db")
votes_by_village= pd.read_sql("""SELECT* FROM votes_by_village ;""",con= connection)
connection.close()

total_votes= votes_by_village["sum_votes"].sum()
country_percentage= votes_by_village.groupby("id")["sum_votes"].sum() / total_votes
vector_a= country_percentage.values

groupby_varibles=['county', 'town', 'village']
village_total_votes= votes_by_village.groupby(groupby_varibles)["sum_votes"].sum().reset_index()
merged= pd.merge(votes_by_village,village_total_votes,on=groupby_varibles,how="left")
merged["village_percentage"]= merged["sum_votes_x"] / merged["sum_votes_y"]
merged= merged[['county', 'town', 'village',"id","village_percentage"]]
pivot_df= merged.pivot(index=['county', 'town', 'village'], columns="id",values="village_percentage").reset_index()

cosine_similarities=[]
for row in pivot_df.iterrows():
    vector_bi = np.array([ row[1][1], row[1][2], row[1][3] ])  # 明確指定用欄位名稱
    vector_a_dot_vector_bi = np.dot(vector_a, vector_bi)
    length_vector_a = pow((vector_a**2).sum(), 0.5)
    length_vector_bi = pow((vector_bi**2).sum(), 0.5)
    cosine_similarity = vector_a_dot_vector_bi / (length_vector_a*length_vector_bi)
    cosine_similarities.append(cosine_similarity)

cosine_similarity_df= pivot_df.copy()
cosine_similarity_df["cosine_similarity"]= cosine_similarities
cosine_similarity_df= cosine_similarity_df.sort_values(["cosine_similarity", "county", "town", "village"],ascending=[False,True,True,True])
cosine_similarity_df = cosine_similarity_df.reset_index(drop=True).reset_index()
cosine_similarity_df["index"] = cosine_similarity_df["index"] + 1

column_names_to_revise= {
    "index":"rank",
    1 : "candidate_1",
    2 : "candidate_2",
    3 : "candidate_3",
}
cosine_similarity_df= cosine_similarity_df.rename(columns=column_names_to_revise)

def filter_county_town_village(df,county :str, town:str, village:str):
    county_condition= df["county"]== county
    town_condition= df["town"]== town
    village_condition= df["village"]== village
    return df[county_condition & town_condition & village_condition]

print(filter_county_town_village(cosine_similarity_df,county="臺中市",town="豐原區",village="社皮里"))