import time
import pandas as pd
from bs4 import BeautifulSoup
from bs4 import Tag
import requests
import concurrent.futures
import json
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import boxscoretraditionalv2
import os

MISSED_LAST = False

ROWS_COMPLETED = 12864
CHUNK_SIZE = 2500



headers = {
    'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
    'Referer': 'https://www.nba.com/',
    'If-None-Match': 'W/"8360eb270b919a1fb4776bc448d9ed14"',
    'If-Modified-Since': 'Wed, 30 Jun 2021 21:14:33 GMT',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'sec-ch-ua-platform': '"Windows"',
}

teamAbbreviations = {'ATLANTA HAWKS': 'ATL',
                     'ST. LOUIS HAWKS': 'SLH',
                     'MILWAUKEE HAWKS': 'MIL',
                     'TRI-CITIES BLACKHAWKS': 'TCB',
                     'BOSTON CELTICS': 'BOS',
                     'BROOKLYN NETS': 'BRK',
                     'NEW JERSEY NETS': 'NJN',
                     'CHICAGO BULLS': 'CHI',
                     'CHARLOTTE HORNETS': 'CHO',
                     'CHARLOTTE BOBCATS': 'CHA',
                     'CLEVELAND CAVALIERS': 'CLE',
                     'DALLAS MAVERICKS': 'DAL',
                     'DENVER NUGGETS': 'DEN',
                     'DETROIT PISTONS': 'DET',
                     'FORT WAYNE PISTONS': 'FWP',
                     'GOLDEN STATE WARRIORS': 'GSW',
                     'SAN FRANCISCO WARRIORS': 'SFW',
                     'PHILADELPHIA WARRIORS': 'PHI',
                     'HOUSTON ROCKETS': 'HOU',
                     'INDIANA PACERS': 'IND',
                     'LOS ANGELES CLIPPERS': 'LAC',
                     'SAN DIEGO CLIPPERS': 'SDC',
                     'BUFFALO BRAVES': 'BUF',
                     'LOS ANGELES LAKERS': 'LAL',
                     'MINNEAPOLIS LAKERS': 'MIN',
                     'MEMPHIS GRIZZLIES': 'MEM',
                     'VANCOUVER GRIZZLIES': 'VAN',
                     'MIAMI HEAT': 'MIA',
                     'MILWAUKEE BUCKS': 'MIL',
                     'MINNESOTA TIMBERWOLVES': 'MIN',
                     'NEW ORLEANS PELICANS': 'NOP',
                     'NEW ORLEANS/OKLAHOMA CITY HORNETS': 'NOK',
                     'NEW ORLEANS HORNETS': 'NOH',
                     'NEW YORK KNICKS': 'NYK',
                     'OKLAHOMA CITY THUNDER': 'OKC',
                     'SEATTLE SUPERSONICS': 'SEA',
                     'ORLANDO MAGIC': 'ORL',
                     'PHILADELPHIA 76ERS': 'PHI',
                     'SYRACUSE NATIONALS': 'SYR',
                     'PHOENIX SUNS': 'PHO',
                     'PORTLAND TRAIL BLAZERS': 'POR',
                     'SACRAMENTO KINGS': 'SAC',
                     'KANSAS CITY KINGS': 'KCK',
                     'KANSAS CITY-OMAHA KINGS': 'KCK',
                     'CINCINNATI ROYALS': 'CIN',
                     'ROCHESTER ROYALS': 'ROR',
                     'SAN ANTONIO SPURS': 'SAS',
                     'TORONTO RAPTORS': 'TOR',
                     'UTAH JAZZ': 'UTA',
                     'NEW ORLEANS JAZZ': 'NOJ',
                     'WASHINGTON WIZARDS': 'WAS',
                     'WASHINGTON BULLETS': 'WAS',
                     'CAPITAL BULLETS': 'CAP',
                     'BALTIMORE BULLETS': 'BAL',
                     'CHICAGO ZEPHYRS': 'CHI',
                     'CHICAGO PACKERS': 'CHI',
                     'ANDERSON PACKERS': 'AND',
                     'CHICAGO STAGS': 'CHI',
                     'INDIANAPOLIS OLYMPIANS': 'IND',
                     'SHEBOYGAN RED SKINS': 'SRS',
                     'ST. LOUIS BOMBERS': 'SLB',
                     'WASHINGTON CAPITOLS': 'WAS',
                     'WATERLOO HAWKS': 'WAT',
                     'SAN DIEGO ROCKETS': 'SDR',
                     'NEW YORK NETS': 'BRK'
                     }

months = {'JAN':1, 'FEB':2, 'MAR':3, 'APR':4, 'MAY':5, 'JUN':6, 'JUL':7, 'AUG':8, 'SEP':9, 'OCT':10, 'NOV':11, 'DEC':12}


def is_player_row(tag: Tag):
    return tag.name == 'tr' and not tag.has_attr('class') and not tag.contents[0] == '\n' and not tag.contents[
                                                                                                      0].text == 'Team Totals'


def create_box_score_masterfile():
    data_headers = ['player_id', 'name', 'team', 'mp', 'fg', 'fga', 'fg_pct', 'fg3', 'fg3a', 'fg3_pct', 'ft', 'fta',
                    'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'plus_minus']


    def create_box_score_file(home_team: str, away_team: str, year: str, month: str, day: str):
        year = int(year)
        month = int(month)
        day = int(day)
        url = 'https://www.basketball-reference.com/boxscores/{}{:2d}{:2d}0{}.html'.format(year, month, day, home_team)
        page_response = requests.get(url, proxies=my_proxies, headers=headers, cookies=cookies)
        box_score = []
        if page_response.status_code == 200:
            box_score_soup = BeautifulSoup(page_response.text.replace('<!--', '').replace('-->', ''), 'html.parser').select(
                'table[id*="-game-basic"]')
            for row in box_score_soup[0].findAll(is_player_row):
                row_data = {key: None for key in data_headers}
                children = row.children
                player_id_tag: Tag = next(children)
                row_data['player_id'] = player_id_tag['data-append-csv']
                row_data['team'] = home_team
                row_data['opp'] = away_team
                row_data['name'] = player_id_tag['csk']
                for stat_cell in children:
                    row_data[stat_cell['data-stat']] = stat_cell.text
                box_score.append(row_data)
            for row in box_score_soup[1].findAll(is_player_row):
                row_data = {key:None for key in data_headers}
                children = row.children
                player_id_tag: Tag = next(children)
                row_data['player_id'] = player_id_tag['data-append-csv']
                row_data['team'] = away_team
                row_data['opp'] = home_team
                row_data['name'] = player_id_tag['csk']
                for stat_cell in children:
                    row_data[stat_cell['data-stat']] = stat_cell.text
                box_score.append(row_data)
            with open('seasons/{}-{:2d}-{:2d}-{}-{}.json'.format(year,month, day, away_team, home_team), 'w') as output_file:
                json.dump(box_score, output_file)
                print(output_file.name)
                time.sleep(5)
        else:
            print(page_response.status_code)
            raise requests.exceptions.HTTPError


    def get_game_identifiers(schedule_file_name) -> list:

        def valid_row(row):
            valid = False
            if isinstance(row['PTS'], str):
                valid = row['PTS'].isnumeric()
            else:
                valid = isinstance(row['PTS'], int)
            return valid

        df: pd.DataFrame = pd.read_csv(schedule_file_name)
        games_info = []
        for index, row in df.iterrows():
            if not valid_row(row):
                continue
            date: list = row['Date'].split(',')
            date = [ele.strip() for ele in date]
            month, day = date[1].split()
            month = '{:02d}'.format(months[month.upper()])
            day: str = '{:02d}'.format(int(date[1].split()[1].strip()))
            year: str = date[2].strip()
            away_team: str = teamAbbreviations[row['Visitor/Neutral'].strip().upper()]
            home_team: str = teamAbbreviations[row['Home/Neutral'].strip().upper()]
            games_info.append([home_team, away_team, year, month, day])
        return games_info

    def create_all_game_info_file(season_files: list):
        for file in season_files:
            with open('{}_identifiers.json'.format(file[:-5]), 'w') as all_season_games_file:
                game_idents = get_game_identifiers(file)
                json.dump(game_idents, all_season_games_file)

    def create_all_box_scores(source_file_name: list):

        for file in source_file_name:

            with open(file, 'r') as all_games:
                all_game_identifiers = json.load(all_games)

            num_threads = min(MAX_THREADS, len(all_game_identifiers))
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                threads = [executor.submit(create_box_score_file, *game) for game in all_game_identifiers]
            concurrent.futures.wait(threads, return_when=concurrent.futures.FIRST_EXCEPTION)

    create_all_box_scores(['NBA_{}_game_identifiers.json'.format(year) for year in range(1960, 2022)])




def get_schedule(year: int) -> pd.DataFrame:
    df = pd.DataFrame()
    months = ['october', 'november', 'december', 'january', 'february', 'march', 'april', 'may', 'june']

    if year == 2020:
        months = ['october-2019', 'november', 'december', 'january', 'february', 'march', 'july', 'august', 'september',
                  'october-2020']
    elif year == 2021:
        months = ['december', 'january', 'february', 'march', 'april', 'may', 'june', 'july']

    for month in months:
        url = 'https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'.format(year, month)
        page_response = requests.get(url, proxies=my_proxies)
        if page_response.status_code == 200:
            schedule_soup = BeautifulSoup(page_response.text, 'html.parser').select('table', id='schedule')
            try:
                df = pd.concat([df, pd.read_html(str(schedule_soup))[0]])
            except ValueError as e:
                continue
            print('Finished {} of {} season'.format(month, year))
        time.sleep(8)

    return df


if __name__ == '__main__':

    all_games: pd.DataFrame = pd.read_json('all_games_from_nba_api.json', orient='records')
    for start in range(max(0, ROWS_COMPLETED), len(all_games), CHUNK_SIZE):
        for index, row in all_games.iloc[max(start, ROWS_COMPLETED):start+CHUNK_SIZE].iterrows():
            try:
                box_score: pd.DataFrame = boxscoretraditionalv2.BoxScoreTraditionalV2(f"{row['GAME_ID']:0{10}}", headers=headers, timeout=200).get_data_frames()[0]
                if not box_score.empty or box_score is None:
                    folders = os.path.join(os.getcwd(), 'teams', row['TEAM_ABBREVIATION'], str(row['SEASON_ID']))
                    os.makedirs(folders, exist_ok=True)
                    box_score.to_json('{}/{}_{}.json'.format(folders, row['MATCHUP'], row['GAME_DATE']),
                                      orient='records')
                time.sleep(.5)
                if index + 1 % 50 == 0:
                    time.sleep(15)
                done_string = f'Completed {index} out of {len(all_games)}'
                print(done_string)
                MISSED_LAST = False
                with open('games_completed', 'a') as finished_games_file:
                    finished_games_file.write(done_string + '\n')
            except json.JSONDecodeError as e:
                with open('games_missed', 'a') as missed_games_file:
                    missed_games_file.write(f'{row["GAME_ID"]:0{10}}    \n')
                delay = 3600 if MISSED_LAST else 60
                print(f"Missed {row['GAME_ID']:0{10}}, waiting {delay} seconds")
                time.sleep(delay)
                MISSED_LAST = True
                print('Resuming')
        #End of Chunk
        time.sleep(300)

