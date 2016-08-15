import sqlite3
import xmltodict
import numpy as np
import pandas as pd
from collections import Counter

if pd.__version__ != '0.18.1':
    print "Esse script necessita do pandas == 0.18.1"
    exit()

class Dataset:
    def __init__(self, filename):
        self.con = sqlite3.connect(filename)
        self.matches = pd.read_sql_query("""
        SELECT season, home_team_goal, away_team_goal, possession, shoton,
        home_player_1,  home_player_2,  home_player_3, home_player_4,  home_player_5,  
        home_player_6,  home_player_7,  home_player_8,  home_player_9,  home_player_10,
        home_player_11,
        away_player_1, away_player_2, away_player_3, away_player_4, away_player_5, 
        away_player_6, away_player_7, away_player_8, away_player_9, away_player_10, away_player_11, 
        home_player_Y1, home_player_Y2, home_player_Y3, home_player_Y4, home_player_Y5, home_player_Y6,
        home_player_Y7, home_player_Y8, home_player_Y9, home_player_Y10, home_player_Y11,
        away_player_Y1, away_player_Y2, away_player_Y3, away_player_Y4, away_player_Y5,
        away_player_Y6, away_player_Y7, away_player_Y8, away_player_Y9, away_player_Y10, away_player_Y11
FROM Match
WHERE
  possession IS NOT NULL AND
  shoton IS NOT NULL AND

  home_player_1 IS NOT NULL AND
  home_player_2 IS NOT NULL AND
  home_player_3 IS NOT NULL AND
  home_player_4 IS NOT NULL AND
  home_player_5 IS NOT NULL AND
  home_player_6 IS NOT NULL AND
  home_player_7 IS NOT NULL AND
  home_player_8 IS NOT NULL AND
  home_player_9 IS NOT NULL AND
  home_player_10 IS NOT NULL AND
  home_player_11 IS  NOT NULL AND

  away_player_1 IS NOT NULL AND
  away_player_2 IS NOT NULL AND
  away_player_3 IS NOT NULL AND
  away_player_4 IS NOT NULL AND
  away_player_5 IS NOT NULL AND
  away_player_6 IS NOT NULL AND
  away_player_7 IS NOT NULL AND
  away_player_8 IS NOT NULL AND
  away_player_9 IS NOT NULL AND
  away_player_10 IS NOT NULL AND
  away_player_11 IS NOT NULL AND

  home_player_Y1 IS NOT NULL AND
  home_player_Y2 IS NOT NULL AND
  home_player_Y3 IS NOT NULL AND
  home_player_Y4 IS NOT NULL AND
  home_player_Y5 IS NOT NULL AND
  home_player_Y6 IS NOT NULL AND
  home_player_Y7 IS NOT NULL AND
  home_player_Y8 IS NOT NULL AND
  home_player_Y9 IS NOT NULL AND
  home_player_Y10 IS NOT NULL AND
  home_player_Y11 IS NOT NULL AND

  away_player_Y1 IS NOT NULL AND
  away_player_Y2 IS NOT NULL AND
  away_player_Y3 IS NOT NULL AND
  away_player_Y4 IS NOT NULL AND
  away_player_Y5 IS NOT NULL AND
  away_player_Y6 IS NOT NULL AND
  away_player_Y7 IS NOT NULL AND
  away_player_Y8 IS NOT NULL AND
  away_player_Y9 IS NOT NULL AND
  away_player_Y10 IS NOT NULL AND
  away_player_Y11 IS NOT NULL
;
        """, self.con)

    def pre_process(self):
        #Calculates the target variable
        goals_balance = []
        for goal in self.matches["home_team_goal"] - self.matches["away_team_goal"]:
            if goal<=0:
                goals_balance.append(0)
            else:
                goals_balance.append(1)

        self.matches["goals_balance"]=pd.Series(goals_balance)

        del self.matches["home_team_goal"]
        del self.matches["away_team_goal"]


        #Substitutes the home_team_playerXX and away_team_playerXX for the historical max overall_rating
        self.player_stats=pd.read_sql_query("SELECT player_api_id, max(overall_rating) as overall_rating FROM Player_Stats GROUP BY player_api_id;",
                                            self.con, index_col="player_api_id")
        for i in range(len(self.matches)):
            print "Running pre-process for match %s" % i
            for j in range(1,12):
                self.matches.loc[i,"home_player_%s" % j] = \
                    self.player_stats.loc[self.matches.loc[i,"home_player_%s" % j], "overall_rating"  ]
            for j in range(1, 12):
                self.matches.loc[i, "away_player_%s" % j] = \
                    self.player_stats.loc[self.matches.loc[i, "away_player_%s" % j], "overall_rating"]

    def pre_process_position(self):
        self.formations = [None] * 2
        self.formations[0] = list()
        self.formations[1] = list()

        for i in range(  len(self.matches) ):
            home_players_y = list()
            away_players_y = list()

            for j in range(1, 12):
                home_players_y.append(self.matches.loc[i,'home_player_Y%d' % j])
                away_players_y.append(self.matches.loc[i,'away_player_Y%d' % j])

            players_y = [home_players_y, away_players_y]

            for i in range(2):
                formation_dict = Counter(players_y[i])
                sorted_keys = sorted(formation_dict)
                formation = ''
                for key in sorted_keys[1:-1]:
                    y = formation_dict[key]
                    formation += '%d-' % y
                formation += '%d' % formation_dict[sorted_keys[-1]]
                self.formations[i].append(formation)

            print('Home team formation: ' + self.formations[0][-1])
            print('Away team formation: ' + self.formations[1][-1])

        for i in range(1, 12):
          del self.matches["home_player_Y%s" % i]

        for i in range(1, 12):
          del self.matches["away_player_Y%s" % i]

    def pre_process_possession(self):
      possession_ = []
      nullposs = 0
      for i in range(len(self.matches) ):
        xml = xmltodict.parse(self.matches.loc[i,"possession"])
        #print "Runnin possession for game: %d" % i

        if xml["possession"] == None:
          possession = 50
          nullposs+=1
          #continue
        else:
          if type(xml["possession"]["value"]) == list:
            if xml["possession"]["value"][-1].has_key("homepos") == True:
              possession = xml["possession"]["value"][-1]["homepos"]
            else:
              possession = 50
              nullposs+=1
          else:
            possession = xml["possession"]["value"]["homepos"]
        possession_.append(possession)

      self.matches["possession"] = pd.Series(possession_)
      self.matches["possession"] = self.matches["possession"].astype(np.int64, copy=True)

if __name__=="__main__":
    dataset = Dataset("database.sqlite")
    dataset.pre_process()
    dataset.pre_process_position()
    dataset.pre_process_possession()