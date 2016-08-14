import sqlite3
import xmltodict
import numpy as np
import pandas as pd

if pd.__version__ != '0.18.1':
    print "Esse script necessita do pandas == 0.18.1"
    exit()

class Dataset:
    def __init__(self, filename):
        self.con = sqlite3.connect(filename)
        self.matches = pd.read_sql_query("""
        SELECT season, home_team_goal, away_team_goal, possession, shoton,  home_player_1,  home_player_2,  home_player_3,
 home_player_4,  home_player_5,  home_player_6,  home_player_7,  home_player_8,  home_player_9,  home_player_10,
 home_player_11, away_player_1, away_player_2, away_player_3, away_player_4, away_player_5, away_player_6, away_player_7,
 away_player_8, away_player_9, away_player_10, away_player_11, home_player_X1, home_player_X2, home_player_X3,
  home_player_X4, home_player_X5, home_player_X6, home_player_X7, home_player_X8, home_player_X9, home_player_X10,
  home_player_X11, home_player_Y1, home_player_Y3, home_player_Y3, home_player_Y4, home_player_Y5, home_player_Y6,
  home_player_Y7, home_player_Y8, home_player_Y9, home_player_Y10, home_player_Y11, away_player_X1, away_player_X2,
  away_player_X3, away_player_X4, away_player_X5, away_player_X6, away_player_X7, away_player_X8, away_player_X9,
  away_player_X10, away_player_X11, away_player_Y1, away_player_Y2, away_player_Y3, away_player_Y4, away_player_Y5,
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


  home_player_X1 IS NOT NULL AND
  home_player_X2 IS NOT NULL AND
  home_player_X3 IS NOT NULL AND
  home_player_X4 IS NOT NULL AND
  home_player_X5 IS NOT NULL AND
  home_player_X6 IS NOT NULL AND
  home_player_X7 IS NOT NULL AND
  home_player_X8 IS NOT NULL AND
  home_player_X9 IS NOT NULL AND
  home_player_X10 IS NOT NULL AND
  home_player_X11 IS NOT NULL AND

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

  away_player_X1 IS NOT NULL AND
  away_player_X2 IS NOT NULL AND
  away_player_X3 IS NOT NULL AND
  away_player_X4 IS NOT NULL AND
  away_player_X5 IS NOT NULL AND
  away_player_X6 IS NOT NULL AND
  away_player_X7 IS NOT NULL AND
  away_player_X8 IS NOT NULL AND
  away_player_X9 IS NOT NULL AND
  away_player_X10 IS NOT NULL AND
  away_player_X11 IS NOT NULL AND

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


if __name__=="__main__":
    dataset = Dataset("database.sqlite")
    dataset.pre_process()