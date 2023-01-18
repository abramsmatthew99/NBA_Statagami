from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder



teams_dict = teams.get_teams()
celtics = [team for team in teams_dict if team['abbreviation'] == 'BOS'][0]
celtics_id = celtics['id']
game_finder = leaguegamefinder.LeagueGameFinder(team_id_nullable=celtics_id, timeout=150, headers=headers)
games = game_finder.get_data_frames()[0]
print(games)