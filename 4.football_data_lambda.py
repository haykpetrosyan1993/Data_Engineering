import json
import awswrangler as wr
import boto3
import csv
import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
# from sqlalchemy import create_engine, select, MetaData
from csv import writer
import main_dict

matches = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/matches.csv')
players = wr.s3.read_csv(path='s3://football-data-buckets/s3-bucket/players.csv', encoding = "ISO-8859-1")
match_values = matches.values
players_values = players.values


    
def lambda_handler(event, context):

    

    base_url = 'https://understat.com/league'
    leagues = ['La_liga', 'EPL', 'Bundesliga', 'Serie_A', 'Ligue_1']
    seasons = ['2021']
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
    teams_name, team_id = [], []
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
    teams_id, teams_title, league_id, country_id, city_id, main_sponsore_id, shirt_sponsore_id, captain_id, \
    coach_id, stadium_id = [], [], [], [], [], [], [], [], [], []
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
    lst = []
    for v in teams_data:
        for i in range(len(teams_data[v])):
            ls = []
            for d in teams_data:
                ls.append(teams_data[d][i])
            if ls not in lst:
                lst.append(ls)
            else:
                continue

    print('Teams done')
    # ---------------------------------------------------Players data ---------------------------------------------------------------------------------------------------------------
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
    

                    data = json.loads(json_data)
                    all_data.append(data)
    

    players_id, players_name = [], []
    play_dict = {}
    exis_players = []
    exist_players = {}
    for i in players_values:
        exis_players.append(i[0])
        

    
    for i in all_data:
    
        # print(type(i))
        for j in i:
    
            for key in j:
                if key == 'player_name' and (int(j['id']) not in exis_players and j['id'] not in exis_players):
                    players_name.append(j[key])
                if key == 'id' and (int(j['id']) not in exis_players and j['id'] not in exis_players):
                    players_id.append(j[key])
                    play_dict[j[key]] = j['player_name']
    
    player_id, player_name, country_id = [], [], []
    for i, j in play_dict.items():
        if int(i) not in exis_players and i not in exis_players:
            player_id.append(i)
            player_name.append(j)
            country_id.append(np.random.randint(110, 351))
        else:
            continue
    
    players_table = dict(zip(['id', 'player_name', 'country_id'], [player_id, player_name, country_id]))
    lst = []
    for v in players_table:
        for i in range(len(players_table[v])):
            ls = []
            for d in players_table:
                ls.append(players_table[d][i])
            if ls not in lst:
                lst.append(ls)
            else:
                continue
    print(lst)


    s3 = boto3.resource('s3')

    # # --------------------------------------------------------- for players_trans transactions ------------------------------------------------------------------
    bucket_transactions = s3.Bucket('football-data-new-actions')
    key_trans = 's3-bucket-trans/players_trans.csv'
    local_file_trans = '/tmp/players_trans.csv'
    s3.Bucket('football-data-new-actions').download_file(key_trans,local_file_trans)
    
    
    # # write the data into '/tmp' folder
    with open('/tmp/players_trans.csv','r', encoding = "ISO-8859-1") as infile:
         reader = list(csv.reader(infile))
         del reader[1:]
         # reader = reader[::-1] # the date is ascending order in file
         for i in lst:
             reader.insert(1, i)
    
    with open('/tmp/players_trans.csv', 'w', encoding = "ISO-8859-1", newline='') as outfile:
         writer = csv.writer(outfile)
         for line in reader: # reverse order
             writer.writerow(line)
    
    # # upload file from tmp to s3 key
    bucket_transactions.upload_file('/tmp/players_trans.csv', key_trans)
    

    
    
    
    
    # # -----------------------------------------------------------------------Matches data---------------------------------------------------------------
    base_url = 'https://understat.com/team/'
    seasons = ['2021']
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
    
    # # print(json_data)
                     data = json.loads(json_data)
                     all_data.append(data)
    
    match_id_main, home_team_id, guest_team_id, match_date,  home_team_goals, guest_team_goals, isResult = [], [], [], [], [], [], []
    
    for i in all_data:
         # print(i)
         for j in i:
             for key, value in j.items():
                 if key == 'isResult' and value == True:
                     isResult.append(j)
    

    exist_match = []
    for i in match_values:
         exist_match.append(str(i[0]))
    
    for i in isResult:
         for key in i:
             if key == 'id' and i['id'] not in exist_match:
                 match_id_main.append(i[key])
             if key == 'h' and i['id'] not in exist_match:
                 for home in i[key]:
                     if home == 'id':
                         home_team_id.append(i[key][home])
    
             if key == 'a' and i['id'] not in exist_match:
                 for guest in i[key]:
                     if guest == 'id':
                         guest_team_id.append(i[key][guest])
             if key == 'goals' and i['id'] not in exist_match:
                 for k in i[key]:
                     if k == 'h':
                         home_team_goals.append(i[key][k])
                     elif k == 'a':
                         guest_team_goals.append(i[key][k])
             if key == 'datetime' and i['id'] not in exist_match:
                     match_date.append(i[key])
    
    
    match_table = dict(zip(['id', 'match_date', 'home_team_id', 'guest_team_id', 'home_team_goals', 'guest_team_goals'],
                           [match_id_main, match_date, home_team_id, guest_team_id, home_team_goals, guest_team_goals]))
    lst = []
    for v in match_table:
         for i in range(len(match_table[v])):
             ls = []
             for d in match_table:
                 ls.append(match_table[d][i])
             if ls not in lst:
                 lst.append(ls)
             else:
                 continue

    
    # # ---------------------------------------------------------------------------for matches transactions--------------------------------------------------------------------------
    s3 = boto3.resource('s3')
    

    # # --------------------------------------------------------- for matches_trans transactions ------------------------------------------------------------------
    bucket_transactions = s3.Bucket('football-data-new-actions')
    key_trans = 's3-bucket-trans/matches_trans.csv'
    local_file_trans = '/tmp/matches_trans.csv'
    s3.Bucket('football-data-new-actions').download_file(key_trans,local_file_trans)
    
    
    # # write the data into '/tmp' folder
    with open('/tmp/matches_trans.csv','r') as infile:
         reader = list(csv.reader(infile))
         del reader[1:]
         # reader = reader[::-1] # the date is ascending order in file
         for i in lst:
             reader.insert(1, i)
    
    with open('/tmp/matches_trans.csv', 'w', newline='') as outfile:
         writer = csv.writer(outfile)
         for line in reader: # reverse order
             writer.writerow(line)
    
    # # upload file from tmp to s3 key
    bucket_transactions.upload_file('/tmp/matches_trans.csv', key_trans)
    
    print('Matches Inserted: ')
    
    # # -------------------------------------------------------------------Match details data -------------------------------------------------------
    
    # # print(matches_id)
    # # print(len(matches_id))
    
    
    base_url = 'https://understat.com/match/'
    match_info = []
    for i in match_table:
         if i == 'id':
             for j in match_table[i]:
                 url = base_url + j
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
    
    # # print(json_data)
                         data = json.loads(json_data)
                         match_info.append(data)
    
    
    
    match_detail_id, match_id, player_id, player, h_team, a_team,  h_goals, a_goals, minute, result, season, shotType, date, player_assisted,\
         h_a, situation = [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []
    
    seasons = {'2020': '20201', '2021': '20211'}
    results = {'SavedShot': 101, 'MissedShots': 102, 'Goal': 103, 'BlockedShot': 104, 'ShotOnPost': 105, 'OwnGoal': 106}
    shotstype = {'RightFoot': 201, 'LeftFoot': 202, 'Head': 203, 'OtherBodyPart': 204}
    situations = {'OpenPlay': 301, 'FromCorner': 302, 'SetPiece': 303, 'DirectFreekick': 304, 'Penalty': 305}
    home_away = {'a': 1, 'h': 2}

    
    for i in match_info:
         for j in i:
             for key in i[j]:
                 for action in key:
                     if action == 'id':  # and key['id'] not in exist_matches_det:
                         match_detail_id.append(key[action])
                     elif action == 'match_id':  # and key['id'] not in exist_matches_det:
                         match_id.append(key[action])
                     elif action == 'player_id':  # and key['id'] not in exist_matches_det:
                         player_id.append(key[action])
                     elif action == 'player':  # and key['id'] not in exist_matches_det:
                         player.append(key[action])
                     elif action == 'h_team':  # and key['id'] not in exist_matches_det:
                         h_team.append(int(team_data[key[action]]))
                     elif action == 'a_team':  # and key['id'] not in exist_matches_det:
                         a_team.append(int(team_data[key[action]]))
                     elif action == 'h_goals':  # and key['id'] not in exist_matches_det:
                         h_goals.append(int(key[action]))
                     elif action == 'a_goals':  # and key['id'] not in exist_matches_det:
                         a_goals.append(int(key[action]))
                     elif action == 'minute':  # and key['id'] not in exist_matches_det:
                         minute.append(key[action])
                     elif action == 'result':  # and key['id'] not in exist_matches_det:
                         result.append(int(results[key[action]]))
                     elif action == 'season':  # and key['id'] not in exist_matches_det:
                         season.append(int(seasons[key[action]]))
                     elif action == 'shotType':  # and key['id'] not in exist_matches_det:
                         shotType.append(int(shotstype[key[action]]))
                     elif action == 'date':  # and key['id'] not in exist_matches_det:
                         date.append(key[action])
                     # elif action == 'player_assisted':  # and key['id'] not in exist_matches_det:
                     #     player_assisted.append(exist_players.get(key[action]))
                     elif action == 'h_a':  # and key['id'] not in exist_matches_det:
                         h_a.append(home_away[key[action]])
                     elif action == 'situation':  # and key['id'] not in exist_matches_det:
                         situation.append(int(situations[key[action]]))
    
    
    keys = ['id', 'match_id', 'player_id', 'h_team_id', 'a_team_id',  'minute', 'result_id', 'season_id', 'shotType_id',
              'h_a', 'situation_id']
    values = [match_detail_id, match_id, player_id, h_team, a_team, minute, result, season, shotType,
               h_a, situation]
    
    match_info = dict(zip(keys, values))
    # print(match_info)
    lst = []
    for v in match_info:
         for i in range(len(match_info[v])):
             ls = []
             for d in match_info:
                 ls.append(match_info[d][i])
             if ls not in lst:
                 lst.append(ls)
             else:
                 continue
 

    
    s3 = boto3.resource('s3')
    

    # # --------------------------------------------------------- for match_details_trans transactions ------------------------------------------------------------------
    bucket_transactions = s3.Bucket('football-data-new-actions')
    key_trans = 's3-bucket-trans/match_details_trans.csv'
    local_file_trans = '/tmp/match_details_trans.csv'
    s3.Bucket('football-data-new-actions').download_file(key_trans,local_file_trans)
    
    
    # # write the data into '/tmp' folder
    with open('/tmp/match_details_trans.csv','r') as infile:
         reader = list(csv.reader(infile))
         del reader[1:]
         # reader = reader[::-1] # the date is ascending order in file
         for i in lst:
             reader.insert(1, i)
    
    with open('/tmp/match_details_trans.csv', 'w', newline='') as outfile:
         writer = csv.writer(outfile)
         for line in reader: # reverse order
             writer.writerow(line)
    
    # # upload file from tmp to s3 key
    bucket_transactions.upload_file('/tmp/match_details_trans.csv', key_trans)

    
    
    # # --------------------------------------------------------------------------------------------players detail data--------------------------------------------------------------------
    base_url = 'https://understat.com/match/'
    match_info = []
    for i in match_id_main:
         #if matches_id[i]:
           url = base_url + str(i)
           data = requests.get(url)
           soup = BeautifulSoup(data.content, 'lxml')
           scripts = soup.find_all('script')
           string_with_json_obj = ''
           for el in scripts:
               if 'rostersData' in str(el):
                     string_with_json_obj = str(el).strip()
                     ind_start = string_with_json_obj.index("('") + 2
                     ind_end = string_with_json_obj.index("')")
                     json_data = string_with_json_obj[ind_start:ind_end]
                     json_data = json_data.encode('utf8').decode('unicode_escape')
    
    # # print(json_data)
                     data = json.loads(json_data)
                     match_info.append(data)
               if 'match_info' in str(el):
                     string_with_json_obj = str(el).strip()
                     ind_start = string_with_json_obj.index("('") + 2
                     ind_end = string_with_json_obj.index("')")
                     json_data = string_with_json_obj[ind_start:ind_end]
                     json_data = json_data.encode('utf8').decode('unicode_escape')
    
    # # print(json_data)
                     data = json.loads(json_data)
                     match_info.append(data)
    
    positions = {'GK':1,	'DR':2,	'DC':3,	'DL':4,	'DMC':5,	'AMR':6,	'AMC':7,	'AML':8,	'FW':9,	'Sub':10,
                  'MC':11,	'FWR':12,	'FWL':13,	'MR':14,	'ML':15,	'DMR':16,	'DML':17 }
    
    home_away = {'a': 1, 'h': 2}
    
    all = {}
    id, goals, own_goals, shots, time, player_id, team_id, position, h_a, yellow_card, red_card, key_passes, assists, match_id =\
         [], [], [], [], [], [], [], [], [], [], [], [], [], []
    a = None
    for k in match_info:
         for i in k:
             for j in k[i]:
                 if type(j) == type({}):
                     for l in j:
                         if l == 'match_id':
                             a = j[l]
    
                 else:
                     k[i][j]['match_id'] = a
                     # print(k[i][j])
                     for x in k[i][j]:
                         if x == 'id':
                             id.append(k[i][j][x])
                         if x == 'goals':
                             goals.append(k[i][j][x])
                         if x == 'own_goals':
                             own_goals.append(k[i][j][x])
                         if x == 'shots':
                             shots.append(k[i][j][x])
                         if x == 'time':
                             time.append(k[i][j][x])
                         if x == 'player_id':
                             player_id.append(k[i][j][x])
                         if x == 'team_id':
                             team_id.append(k[i][j][x])
                         if x == 'position':
                             position.append(positions[k[i][j][x]])
                         if x == 'h_a':
                             h_a.append(home_away[k[i][j][x]])
                         if x == 'yellow_card':
                             yellow_card.append(k[i][j][x])
                         if x == 'red_card':
                             red_card.append(k[i][j][x])
                         if x == 'key_passes':
                             key_passes.append(k[i][j][x])
                         if x == 'assists':
                             assists.append(k[i][j][x])
                         if x == 'match_id':
                             match_id.append(k[i][j][x])
    
    values = [id, goals, own_goals, shots, time, player_id, team_id, position, h_a, yellow_card, red_card, key_passes, assists, match_id]
    keys = ['id', 'goals', 'own_goals', 'shots', 'time', 'player_id', 'team_id', 'position', 'h_a', 'yellow_card', 'red_card', 'key_passes', 'assists', 'match_id']
    palyer_details = dict(zip(keys, values))
    
    
    lst = []
    for v in palyer_details:
         for i in range(len(palyer_details[v])):
             ls = []
             for d in palyer_details:
                 ls.append(palyer_details[d][i])
             if ls not in lst:
                 lst.append(ls)
             else:
                 continue
            
    

    

    s3 = boto3.resource('s3')

    # # --------------------------------------------------------- for player_details_trans transactions ------------------------------------------------------------------
    bucket_transactions = s3.Bucket('football-data-new-actions')
    key_trans = 's3-bucket-trans/player_details_trans.csv'
    local_file_trans = '/tmp/player_details_trans.csv'
    s3.Bucket('football-data-new-actions').download_file(key_trans,local_file_trans)
    
    
    # # write the data into '/tmp' folder
    with open('/tmp/player_details_trans.csv','r') as infile:
         reader = list(csv.reader(infile))
         del reader[1:]
         # reader = reader[::-1] # the date is ascending order in file
         for i in lst:
             reader.insert(1, i)
    
    with open('/tmp/player_details_trans.csv', 'w', newline='') as outfile:
         writer = csv.writer(outfile)
         for line in reader: # reverse order
             writer.writerow(line)
    
    # # upload file from tmp to s3 key
    bucket_transactions.upload_file('/tmp/player_details_trans.csv', key_trans)
       
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
