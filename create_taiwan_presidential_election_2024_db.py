import pandas as pd
import os
import re
import sqlite3

class CreateTaiwanPresidentialElection2024Db:
    def __init__(self):
        file_names=os.listdir("data")    # 全台縣市
        county_names=[]
        for file_name in file_names:
            if ".xlsx" in file_name:
                file_name_split= re.split("\\(|\\)",file_name)
                county_names.append(file_name_split[1])
        self.county_names=county_names

    def tidy_county_dataframe(self,county_name :str):
        file_path=(f"data/總統-A05-4-候選人得票數一覽表-各投開票所({county_name}).xlsx")
        df= pd.read_excel(file_path, skiprows=[0, 3, 4])
        df=df.iloc[:,:6]

        candidate_info= df.iloc[0,3:].values.tolist()
        df.columns= ["town","village","polling_place"]+candidate_info

        df["town"] = df["town"].ffill()
        df["town"]= df["town"].str.strip()
        df= df.dropna()
        df["polling_place"]= df["polling_place"].astype(int)

        id_variables= ["town","village","polling_place"]
        df_melted= pd.melt(df,id_vars=id_variables,var_name="candidate_info",value_name="votes")
        df_melted["county"]= county_name
        return df_melted
    
    def concat_country_dataframe(self):
        country_df= pd.DataFrame()      # 全台資料
        for county_name in self.county_names:
            county_df= self.tidy_county_dataframe(county_name)
            country_df=pd.concat([country_df,county_df])
        country_df= country_df.reset_index(drop=True)

        numbers,candidates= [],[]      # tidy 
        for elem in country_df["candidate_info"].str.split("\n"):
            number= re.sub("\\(|\\)","",elem[0])
            numbers.append(int(number))
            candidate= elem[1]+ "/" + elem[2]
            candidates.append(candidate)

        presidential_votes= country_df.loc[:,["county","town","village","polling_place"]]
        presidential_votes["number"]= numbers
        presidential_votes["candidate"]= candidates
        presidential_votes["votes"]= country_df["votes"].values
        return presidential_votes
    
    def create_database(self):
        presidential_votes= self.concat_country_dataframe()
        polling_place_df= presidential_votes.groupby(["county","town","village","polling_place"]).count().reset_index()
        polling_place_df= polling_place_df[["county","town","village","polling_place"]]
        polling_place_df= polling_place_df.reset_index()
        polling_place_df["index"]= polling_place_df["index"]+1
        polling_place_df= polling_place_df.rename(columns={"index":"id"})

        candidate_df= presidential_votes.groupby(["number","candidate"]).count().reset_index()
        candidate_df= candidate_df[["number","candidate"]]
        candidate_df= candidate_df.rename(columns={"number":"id"})

        join_keys=["county","town","village","polling_place"]
        votes_df = pd.merge(presidential_votes,polling_place_df,on=join_keys,how="left")
        votes_df = votes_df[["id", "number", "votes"]]
        votes_df= votes_df.rename(columns={"id":"polling_place_id","number":"candidate_id"})
        
        connection= sqlite3.connect("data/create_taiwan_presidential_election_2024.db")
        polling_place_df.to_sql("polling_place",con=connection,if_exists="replace",index=False)
        candidate_df.to_sql("candidate",con=connection,if_exists="replace",index=False)
        votes_df.to_sql("votes",con=connection,if_exists="replace",index=False)
        cur= connection.cursor()
        drop_view_sql="""DROP VIEW IF EXISTS votes_by_village"""
        create_view_sql="""
        CREATE VIEW votes_by_village AS
        SELECT  polling_place.county,
                polling_place.town,
                polling_place.village,
                candidate.id,
                candidate.candidate,
                SUM(votes.votes) AS sum_votes
        FROM votes
        LEFT JOIN polling_place
        ON votes.polling_place_id = polling_place.id
        LEFT JOIN candidate
        ON votes.candidate_id = candidate.id
        GROUP BY polling_place.county,
                 polling_place.town,
                 polling_place.village,
                 candidate.id
        """
        cur.execute(drop_view_sql)
        cur.execute(create_view_sql)
        connection.close()

create_taiwan_presidential_election_2024_db= CreateTaiwanPresidentialElection2024Db()
create_taiwan_presidential_election_2024_db.create_database()

