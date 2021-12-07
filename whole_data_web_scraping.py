import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
import numpy as np
from sqlalchemy import create_engine, select, MetaData

engine = create_engine('postgresql')
META_DATA = MetaData(bind=engine)
MetaData.reflect(META_DATA)

# -------------------------------------------------------------------------Teams data---------------------------------------------------------------
import main_dict

base_url = 'https://understat.com/league'
leagues = ['La_liga', 'EPL', 'Bundesliga', 'Serie_A', 'Ligue_1']
seasons = ['2020', '2021']
all_data = []
for season in seasons:
    for league in leagues:
        url = base_url + '/' + league + '/' + season
# url = base_url + '/' + leagues[0] + '/' + seasons[0]
        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'lxml')
        scripts = soup.find_all('script')
        string_with_json_obj = ''

        for el in scripts:
            if 'teamsData' in str(el):
                string_with_json_obj = str(el).strip()


                ind_start = string_with_json_obj.index("('") + 2
                ind_end = string_with_json_obj.index("')")
                json_data = string_with_json_obj[ind_start:ind_end]
                json_data = json_data.encode('utf8').decode('unicode_escape')

# print(json_data)
                data = json.loads(json_data)
                all_data.append(data)



# print(all_data)
team_data = {}
teams_name = []
team_id = []

for i in all_data:
    for team in i:
        for name, value in i[team].items():
            if name == 'id':
                team_id.append(value)
            if name == 'title':
                teams_name.append(value)



teams_list = []
for i in teams_name:
    teams_list.append(i.split())

teams = []
for i in teams_list:
    if len(i) == 1:
        teams.append(i[0])
    elif len(i) == 2:
        teams.append(i[0] + '_' + i[1])
    elif len(i) == 3:
        teams.append(i[0] + '_' + i[1] + '_' + i[2])

team_data = dict(zip(teams_name, team_id))
# print(team_data)
teams_id = []
teams_title = []
league_id = []
country_id = []
city_id = []
main_sponsore_id = []
shirt_sponsore_id = []
captain_id = []
coach_id = []
stadium_id = []
for i, j in team_data.items():
    teams_id.append(int(j))
    teams_title.append(i)
    league_id.append(main_dict.league_dict[int(j)])
    country_id.append(main_dict.country_dict[int(j)])
    city_id.append(main_dict.city_dict[int(j)])
    main_sponsore_id.append(main_dict.main_sponsores[int(j)])
    shirt_sponsore_id.append(main_dict.shirt_sponsores[int(j)])
    captain_id.append(main_dict.captain_dict[int(j)])
    coach_id.append(main_dict.coach_dict[int(j)])
    stadium_id.append(main_dict.stadium_dict[int(j)])




column_names = ['id', 'team_title', 'league_id', 'country_id', 'city_id', 'main_sponsore_id', 'shirt_sponsore_id', 'captain_id',
                'coach_id', 'stadium_id']
value_names = [teams_id, teams_title, league_id, country_id, city_id, main_sponsore_id, shirt_sponsore_id, captain_id, coach_id, stadium_id]
teams_data = dict(zip(column_names, value_names))
# team_df = pd.DataFrame(teams_data)
# team_df.to_sql('teams', engine, index=False, if_exists='append')
print('Teams done')
# ---------------------------------------------------Players data -------------------------------------------------------------------------
base_url = 'https://understat.com/league'
all_data = []
for league in leagues:
    for season in seasons:
        url = base_url + '/' + league + '/' + season
# url = base_url + '/' + leagues[0] + '/' + seasons[0]
        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'lxml')
        scripts = soup.find_all('script')
        string_with_json_obj = ''

        for el in scripts:
            if 'playersData' in str(el):
                string_with_json_obj = str(el).strip()


                ind_start = string_with_json_obj.index("('") + 2
                ind_end = string_with_json_obj.index("')")
                json_data = string_with_json_obj[ind_start:ind_end]
                json_data = json_data.encode('utf8').decode('unicode_escape')

# print(json_data)
                data = json.loads(json_data)
                all_data.append(data)

positions = {'F M S': 11, 'F S': 12, 'F M': 13, 'D F M S': 14, 'M S': 15, 'F': 16, 'D M S': 17, 'D': 18, 'D M': 19, 'D S': 20, 'M': 21,
             'GK': 22, 'GK S': 23, 'S': 24, 'D F S': 25, 'D F M': 26}
# print(all_data)
# print(data)
players_id = []
players_name = []
team_id = []
position_id = []
games = []
time = []
red_cards = []
yellow_cards = []
goals = []
assists = []
key_passes = []
shots = []
id = []
seasons = []
x = 0
play_dict = {}

for i in all_data:

    # print(type(i))
    for j in i:
        x += 1
        id.append(x)
        for key in j:
            if key == 'player_name':
                players_name.append(j[key])
            if key == 'id':
                players_id.append(j[key])
                play_dict[j[key]] = j['player_name']
            if key == 'team_title':
                try:
                   team_id.append(team_data[j[key]])
                except:
                    team_id.append(team_data[j[key].split(',')[0]])
            if key == 'position':
                position_id.append(positions[j[key]])
            if key == 'games':
                games.append(j[key])
            if key == 'time':
                time.append(j[key])
            if key == 'red_cards':
                red_cards.append(j[key])
            if key == 'yellow_cards':
                yellow_cards.append(j[key])
            if key == 'goals':
                goals.append(j[key])
            if key == 'assists':
                assists.append(j[key])
            if key == 'key_passes':
                key_passes.append(j[key])
            if key == 'shots':
                shots.append(j[key])

player_id = []
player_name = []
country_id = []
for i, j in play_dict.items():
    player_id.append(i)
    player_name.append(j)
    country_id.append(np.random.randint(110, 351))

players_table = dict(zip(['id', 'player_name', 'country_id'], [player_id, player_name, country_id]))
player_df = pd.DataFrame(players_table)
player_df.to_csv('player.csv', index=False)
# player_df.to_excel('all_players.xlsx')
# player_df = player_df.drop_duplicates()
# player_df.to_sql('players', engine, index=False, if_exists='append')
print('players done')
players_detail_table = dict(zip(['id', 'player_id', 'games', 'time', 'team_id', 'position_id', 'yellow_cards', 'red_cards', 'goals',
                                 'assists', 'key_passes', 'shots'],
                                [id, players_id, games, time, team_id, position_id, yellow_cards, red_cards, goals, assists, key_passes, shots]))
# players_det_df = pd.DataFrame(players_detail_table)
# players_det_df.to_sql('players_detail', engine, index=False, if_exists='append')
# players_det_df.to_csv('players_det.csv', index=False)
print('players_det done')



# -----------------------------------------------------------------------Matches data---------------------------------------------------------------
base_url = 'https://understat.com/team/'
seasons = ['2020', '2021']
all_data = []
for team in teams:
    for season in seasons:
        url = base_url + team + '/' + season
        data = requests.get(url)
        soup = BeautifulSoup(data.content, 'lxml')
        scripts = soup.find_all('script')
        string_with_json_obj = ''


        for el in scripts:
            if 'datesData' in str(el):
                string_with_json_obj = str(el).strip()


                ind_start = string_with_json_obj.index("('") + 2
                ind_end = string_with_json_obj.index("')")
                json_data = string_with_json_obj[ind_start:ind_end]
                json_data = json_data.encode('utf8').decode('unicode_escape')

# print(json_data)
                data = json.loads(json_data)
                all_data.append(data)

match_id = []
home_team_id = []
guest_team_id = []
match_date = []
home_team_goals = []
guest_team_goals = []
isResult = []
for i in all_data:
    # print(i)
    for j in i:
        for key, value in j.items():
            if key == 'isResult' and value == True:
                isResult.append(j)

for i in isResult:
    for key in i:
        if key == 'id':
            match_id.append(i[key])
        if key == 'h':
            for home in i[key]:
                if home == 'id':
                    home_team_id.append(i[key][home])

        if key == 'a':
            for guest in i[key]:
                if guest == 'id':
                    guest_team_id.append(i[key][guest])
        if key == 'goals':
            for k in i[key]:
                if k == 'h':
                    home_team_goals.append(i[key][k])
                elif k == 'a':
                    guest_team_goals.append(i[key][k])
        if key == 'datetime':
                match_date.append(i[key])






match_table = dict(zip(['id', 'match_date', 'home_team_id', 'guest_team_id', 'home_team_goals', 'guest_team_goals'],
                       [match_id, match_date, home_team_id, guest_team_id, home_team_goals, guest_team_goals]))
# match_table_df = pd.DataFrame(match_table)
# match_table_df.set_index('id')
# match_table_pg = match_table_df.drop_duplicates()
# match_table_pg.to_csv('matches_table.csv', index=False)
# match_table_pg.to_sql('matches', engine, index=False, if_exists='append')

print('Matches Inserted: ')

# -------------------------------------------------------------------Match details data ------------------------------------------------------
matches = []
for i in all_data:
    for j in i:
        # print(j)
        matches.append(j)

matches_id = {}
for i in matches:
    for j in i:
        # print(i)
        if j == 'id':
            matches_id[i[j]] = False if i['isResult'] == False else i['isResult']

# print(matches_id)
# print(len(matches_id))

base_url = 'https://understat.com/match/'
match_info = []
for i in matches_id:
    if matches_id[i]:
       url = base_url + i
       data = requests.get(url)
       soup = BeautifulSoup(data.content, 'lxml')
       scripts = soup.find_all('script')
       string_with_json_obj = ''
       for el in scripts:
           if 'match_info' in str(el):
                string_with_json_obj = str(el).strip()
                ind_start = string_with_json_obj.index("('") + 2
                ind_end = string_with_json_obj.index("')")
                json_data = string_with_json_obj[ind_start:ind_end]
                json_data = json_data.encode('utf8').decode('unicode_escape')

# print(json_data)
                data = json.loads(json_data)
                match_info.append(data)


match_detail_id = []
match_id = []
player_id = []
player = []
h_team = []
a_team = []
h_goals = []
a_goals = []
minute = []
result = []
season = []
shotType = []
date = []
player_assisted = []
h_a = []
situation = []
seasons = {'2020': '20201', '2021': '20211'}
results = {'SavedShot': 101, 'MissedShots': 102, 'Goal': 103, 'BlockedShot': 104, 'ShotOnPost': 105, 'OwnGoal': 106}
shotstype = {'RightFoot': 201, 'LeftFoot': 202, 'Head': 203, 'OtherBodyPart': 204}
situations = {'OpenPlay': 301, 'FromCorner': 302, 'SetPiece': 303, 'DirectFreekick': 304, 'Penalty': 305}


for i in match_info:
    for j in i:
        for key in i[j]:
            for action in key:
                if action == 'id':
                    match_detail_id.append(key[action])
                elif action == 'match_id':
                    match_id.append(key[action])
                elif action == 'player_id':
                    player_id.append(key[action])
                elif action == 'player':
                    player.append(key[action])
                elif action == 'h_team':
                    h_team.append(int(team_data[key[action]]))
                elif action == 'a_team':
                    a_team.append(int(team_data[key[action]]))
                elif action == 'h_goals':
                    h_goals.append(int(key[action]))
                elif action == 'a_goals':
                    a_goals.append(int(key[action]))
                elif action == 'minute':
                    minute.append(key[action])
                elif action == 'result':
                    result.append(int(results[key[action]]))
                elif action == 'season':
                    season.append(int(seasons[key[action]]))
                elif action == 'shotType':
                    shotType.append(int(shotstype[key[action]]))
                elif action == 'date':
                    date.append(key[action])
                elif action == 'player_assisted':
                    player_assisted.append(key[action])
                elif action == 'h_a':
                    h_a.append(key[action])
                elif action == 'situation':
                    situation.append(int(situations[key[action]]))


keys = ['id', 'match_id', 'player_id', 'h_team_id', 'a_team_id', 'h_goals', 'a_goals', 'minute', 'result_id', 'season_id', 'shotType_id', 'match_date',
        'player_assisted', 'h_a', 'situation_id']
values = [match_detail_id, match_id, player_id, h_team, a_team, h_goals, a_goals, minute, result, season, shotType, date,
          player_assisted, h_a, situation]

match_info = dict(zip(keys, values))
# print(match_info)
# match_info_df = pd.DataFrame(match_info)
# match_info_df.set_index('id')
# match_details = match_info_df.drop_duplicates()
# faile_name = 'match_details_new.csv'
# match_details.to_csv(faile_name, index=False)
# match_details.to_sql('match_details', engine, index=False, if_exists='append')
print('Match details inserted')
# shotsData, match_info, rostersData
