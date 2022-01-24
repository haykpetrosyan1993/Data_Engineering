from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, ForeignKeyConstraint, TIMESTAMP

engine = create_engine('postgresql')
meta = MetaData()

country = Table(
   'country', meta,
   Column('id', Integer, primary_key=True),
   Column('country_name', String),
   Column('short_name', String),
)

city = Table(
   'city', meta,
   Column('id', Integer, primary_key=True),
   Column('city_name', String),
   Column('country_id', Integer,  ForeignKey('country.id'), nullable=False),
)

leagues = Table(
   'leagues', meta,
   Column('id', Integer, primary_key=True),
   Column('league_name', String),
   Column('country_id', Integer,  ForeignKey('country.id'), nullable=False),
)

seasons = Table(
   'seasons', meta,
   Column('id', Integer, primary_key=True),
   Column('season', String),
)

shotstype = Table(
   'shotstype', meta,
   Column('id', Integer, primary_key=True),
   Column('shotType', String),
)

situations = Table(
   'situations', meta,
   Column('id', Integer, primary_key=True),
   Column('situation', String),
)

results = Table(
   'results', meta,
   Column('id', Integer, primary_key=True),
   Column('result', String),
)

positions = Table(
   'positions', meta,
   Column('id', Integer, primary_key=True),
   Column('positions_played', String),
)

shirt_sponsores = Table(
   'shirt_sponsores', meta,
   Column('id', Integer, primary_key=True),
   Column('shirt_sponsore', String),
)

main_sponsores = Table(
   'main_sponsores', meta,
   Column('id', Integer, primary_key=True),
   Column('sponsore_title', String),
)


stadiums = Table(
   'stadiums', meta,
   Column('id', Integer, primary_key=True),
   Column('stadium_name', String),
   Column('city_id', Integer, ForeignKey('city.id'), nullable=False),
   Column('capacity', Integer),
   Column('country_id', Integer, ForeignKey('country.id'), nullable=False),
)

captains = Table(
   'captains', meta,
   Column('id', Integer, primary_key=True),
   Column('captain_name', String),
)

coaches = Table(
   'coaches', meta,
   Column('id', Integer, primary_key=True),
   Column('coach_name', String),
   Column('country_id', Integer, ForeignKey('country.id'), nullable=False),
)

teams = Table(
   'teams', meta,
   Column('id', Integer, primary_key=True),
   Column('team_title', String),
   Column('league_id', Integer, ForeignKey('leagues.id'), nullable=False),
   Column('country_id', Integer, ForeignKey('country.id'), nullable=False),
   Column('city_id', Integer, ForeignKey('city.id'), nullable=False),
   Column('main_sponsore_id', Integer, ForeignKey('main_sponsores.id'), nullable=False),
   Column('shirt_sponsore_id', Integer, ForeignKey('shirt_sponsores.id'), nullable=False),
   Column('captain_id', Integer, ForeignKey('captains.id'), nullable=False),
   Column('coach_id', Integer, ForeignKey('coaches.id'), nullable=False),
   Column('stadium_id', Integer, ForeignKey('stadiums.id'), nullable=False),
)

players = Table(
   'players', meta,
   Column('id', Integer, primary_key=True),
   Column('player_name', String),
   Column('country_id', Integer, ForeignKey('country.id'), nullable=False),
)

players_detail = Table(
   'players_detail', meta,
   Column('id', Integer, primary_key=True),
   Column('player_id', Integer, ForeignKey('players.id'), nullable=False),
   Column('games', Integer),
   Column('time', Integer),
   Column('team_id', Integer, ForeignKey('teams.id'), nullable=False),
   Column('position_id', Integer, ForeignKey('positions.id'), nullable=False),
   Column('yellow_cards', Integer),
   Column('red_cards', Integer),
   Column('goals', Integer),
   Column('assists', Integer),
   Column('key_passes', Integer),
   Column('shots', Integer),
)


matches = Table(
   'matches', meta,
   Column('id', Integer, primary_key=True),
   Column('match_date', TIMESTAMP),
   Column('home_team_id', Integer, ForeignKey('teams.id'), nullable=False),
   Column('guest_team_id', Integer, ForeignKey('teams.id'), nullable=False),
   Column('home_team_goals', Integer),
   Column('guest_team_goals', Integer),
)

match_details = Table(
   'match_details', meta,
   Column('id', Integer, primary_key=True),
   Column('match_id', Integer, ForeignKey('matches.id'), nullable=False),
   Column('player_id', Integer, ForeignKey('players.id'), nullable=False),
   Column('h_team_id', Integer, ForeignKey('teams.id'), nullable=False),
   Column('a_team_id', Integer, ForeignKey('teams.id'), nullable=False),
   Column('h_goals', Integer),
   Column('a_goals', Integer),
   Column('minute', Integer),
   Column('result_id', Integer, ForeignKey('results.id'), nullable=False),
   Column('season_id', Integer, ForeignKey('seasons.id'), nullable=False),
   Column('shotType_id', Integer, ForeignKey('shotstype.id'), nullable=False),
   Column('match_date', TIMESTAMP),
   Column('player_assisted', String),
   Column('h_a', String),
   Column('situation_id', Integer, ForeignKey('situations.id'), nullable=False),
)


meta.create_all(engine)
