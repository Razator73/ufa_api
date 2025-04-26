#!/usr/bin/env pipenv-shebang
import os

import pandas as pd
from audl.stats.endpoints.seasonschedule import SeasonSchedule
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def upsert_rows(df_upsert, table_name, id_col):
    engine = create_engine(f"postgresql://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}@"
                           f"{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}")
    if isinstance(id_col, str):
        id_col = [id_col]
    try:
        with engine.connect() as connection:
            for index, row in df_upsert.iterrows():
                # Construct the INSERT statement with ON CONFLICT
                insert_stmt = text(f"""
                    INSERT INTO {table_name} ({', '.join(df_upsert.columns)})
                    VALUES ({', '.join([':' + col for col in df_upsert.columns])})
                    ON CONFLICT ({', '.join(id_col)}) DO UPDATE
                    SET {', '.join([f"{col} = :{col}" for col in df_upsert.columns if col not in id_col])};
                """)
                connection.execute(insert_stmt, row.to_dict())
            connection.commit()
        print(f"DataFrame upserted to table {table_name}.")
    except Exception as e:
        print(f"An error occurred during the upsert: {e}")
    finally:
        engine.dispose()



if __name__ == '__main__':
    load_dotenv()
    season_schedule = SeasonSchedule(2025).get_schedule()
    rename_cols = {
        'gameID': 'id',
        'awayTeamID': 'away_team_id',
        'homeTeamID': 'home_team_id',
        'awayScore': 'away_score',
        'homeScore': 'home_score',
        'status': 'status',
        'week': 'week',
        'streamingURL': 'streaming_url',
        'hasRosterReport': 'has_roster_report',
        'startTimestamp': 'start_timestamp',
        'startTimezone': 'start_timezone',
        'startTimeTBD': 'start_time_tbd'
    }
    season_schedule = season_schedule.rename(columns=rename_cols)

    teams_df = season_schedule[['home_team_id', 'homeTeamCity', 'homeTeamName']].drop_duplicates()
    team_renames = {
        'home_team_id': 'id',
        'homeTeamCity': 'team_city',
        'homeTeamName': 'team_name'
    }
    teams_df = teams_df.rename(columns=team_renames)
    upsert_rows(teams_df, 'teams', 'id')

    season_schedule = season_schedule[season_schedule.week != '']
    season_schedule['week'] = season_schedule.week.str.replace('week-', '').astype(int)
    season_schedule['start_timestamp'] = pd.to_datetime(season_schedule.start_timestamp, utc=True)
    upsert_rows(season_schedule[rename_cols.values()], 'games', 'id')
