# Databricks notebook source
# MAGIC %run ./00-ddl

# COMMAND ----------

import os
import requests
import json
import time
import pyspark.sql.functions as F
from datetime import datetime

# COMMAND ----------

# dbutils.fs.rm(f"{VOLUME_PREFIX}/mlb_gumbo_checkpoints", True)

# COMMAND ----------

# Get the current date
# today = datetime.now().strftime("%Y-%m-%d") # Use current date to get games today

# Define the start and end dates for the schedule
start_date = "2024-10-01"
end_date = "2024-10-30"

# Construct the URL to fetch the schedule data from the MLB API
URL = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start_date}&endDate={end_date}"
data = requests.get(URL).json()  # Fetch the data and parse it as JSON

# Initialize an empty list to store game PKs
game_pks = []

# Loop through the dates in the fetched data
for date in data.get("dates", []):
  # Loop through the games on each date
  for game in date.get("games", []):
    # Extract the game PK and append it to the list
    game_pk = game.get("gamePk")
    game_pks.append(game_pk)

# Output the list of game PKs
print(f"Extracted {len(game_pks)} games.")

# COMMAND ----------

# Get the current date and time
current_run = datetime.now()
folder_path = f"{DATA_LOCATION}/year={current_run.year}/month={current_run.month}/day={current_run.day}/hour={current_run.hour}/minute={current_run.minute}"

# Create the directory structure if it doesn't exist
os.makedirs(folder_path, exist_ok=True)

# Loop through the GamePKs and download the GUMBO data
for game_pk in game_pks:
  URL = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live?hydrate=credits,alignment,flags,officials,preState"
  data = requests.get(URL).json()

  # Save the data to a JSON file in DBFS
  with open(f'{folder_path}/game_data_{game_pk}.json', 'w') as f:
    json.dump(data, f)

  # Log and sleep
  print(game_pk)
  time.sleep(1)

# COMMAND ----------

# Define Bronze Table
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.{BRONZE_PREFIX}_data"

# Ingest
query = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("singleVariantColumn", "data")
  .load(f"{DATA_LOCATION}/*.json")
  .withColumn("file_path", F.col("_metadata.file_path"))
  .withColumn("file_name", F.col("_metadata.file_name"))
  .withColumn("file_size", F.col("_metadata.file_size"))
  .withColumn("file_modification_time", F.col("_metadata.file_modification_time"))
  .withColumn("file_batch_time", F.lit(current_run))
  .withColumn("last_update_time", F.current_timestamp())
  .writeStream
  .option("checkpointLocation", f"{CHECKPOINT_LOCATION}")
  .trigger(availableNow=True)
  .toTable(BRONZE_TABLE)
)

query.awaitTermination()

# COMMAND ----------

df = spark.sql("""
SELECT 
    -- Metadata
    data:gamePk::int as game_pk,
    data:link::string as link,
    data:gameData.game.type::string as game_type,
    data:gameData.game.doubleHeader::string as double_header,
    data:gameData.game.id::string as game_id,
    data:gameData.game.gamedayType::string as gameday_type,
    data:gameData.game.tiebreaker::string as tiebreaker,
    data:gameData.game.gameNumber::int as game_number,
    data:gameData.game.calendarEventID::string as calendar_event_id,
    data:gameData.game.season::int as season,
    data:gameData.game.seasonDisplay::int as season_display,

    -- Game Datetime
    data:gameData.datetime.dateTime::timestamp_ntz as date_time,
    data:gameData.datetime.originalDate::date as original_date,
    data:gameData.datetime.officialDate::date as official_date,
    data:gameData.datetime.dayNight::string as day_night,
    data:gameData.datetime.time::string as time,
    data:gameData.datetime.ampm::string as ampm,

    -- Game Status
    data:gameData.status.abstractGameState::string as abstract_game_state,
    data:gameData.status.codedGameState::string as coded_game_state,
    data:gameData.status.detailedState::string as detailed_state,
    data:gameData.status.statusCode::string as status_code,
    data:gameData.status.startTimeTBD::string as start_time_tbd,
    data:gameData.status.abstractGameCode::string as abstract_game_code,

    -- Away Team
    data:gameData.teams.away.abbreviation::string as away_team_abbreviation,
    data:gameData.teams.away.active::boolean as away_team_active,
    data:gameData.teams.away.allStarStatus::string as away_team_all_star_status,
    data:gameData.teams.away.clubName::string as away_team_club_name,
    data:gameData.teams.away.division.id::integer as away_team_division_id,
    data:gameData.teams.away.division.link::string as away_team_division_link,
    data:gameData.teams.away.division.name::string as away_team_division_name,
    data:gameData.teams.away.fileCode::string as away_team_file_code,
    data:gameData.teams.away.firstYearOfPlay::integer as away_team_first_year_of_play,
    data:gameData.teams.away.franchiseName::string as away_team_franchise_name,
    data:gameData.teams.away.id::integer as away_team_id,
    data:gameData.teams.away.league.id::integer as away_team_league_id,
    data:gameData.teams.away.league.link::string as away_team_league_link,
    data:gameData.teams.away.league.name::string as away_team_league_name,
    data:gameData.teams.away.link::string as away_team_link,
    data:gameData.teams.away.locationName::string as away_team_location_name,
    data:gameData.teams.away.name::string as away_team_name,
    data:gameData.teams.away.record.conferenceGamesBack::string as away_team_record_conference_games_back,
    data:gameData.teams.away.record.divisionGamesBack::string as away_team_record_division_games_back,
    data:gameData.teams.away.record.divisionLeader::boolean as away_team_record_division_leader,
    data:gameData.teams.away.record.gamesPlayed::int as away_team_record_games_played,
    data:gameData.teams.away.record.leagueGamesBack::string as away_team_record_league_games_back,
    data:gameData.teams.away.record.leagueRecord.losses::int as away_team_record_league_losses,
    data:gameData.teams.away.record.leagueRecord.pct::double as away_team_record_league_pct,
    data:gameData.teams.away.record.leagueRecord.ties::int as away_team_record_league_ties,
    data:gameData.teams.away.record.leagueRecord.wins::int as away_team_record_league_wins,
    data:gameData.teams.away.record.losses::int as away_team_record_losses,
    data:gameData.teams.away.record.sportGamesBack::string as away_team_record_sport_games_back,
    data:gameData.teams.away.record.springLeagueGamesBack::string as away_team_record_spring_league_games_back,
    data:gameData.teams.away.record.wildCardGamesBack::string as away_team_record_wild_card_games_back,
    data:gameData.teams.away.record.winningPercentage::double as away_team_record_winning_percentage,
    data:gameData.teams.away.record.wins::int as away_team_record_wins,
    data:gameData.teams.away.season::string as away_team_season,
    data:gameData.teams.away.shortName::string as away_team_short_name,
    data:gameData.teams.away.sport.id::int as away_team_sport_id,
    data:gameData.teams.away.sport.link::string as away_team_sport_link,
    data:gameData.teams.away.sport.name::string as away_team_sport_name,
    data:gameData.teams.away.springLeague.abbreviation::string as away_team_spring_league_abbreviation,
    data:gameData.teams.away.springLeague.id::int as away_team_spring_league_id,
    data:gameData.teams.away.springLeague.link::string as away_team_spring_league_link,
    data:gameData.teams.away.springLeague.name::string as away_team_spring_league_name,
    data:gameData.teams.away.springVenue.id::string as away_team_spring_venue_id,
    data:gameData.teams.away.springVenue.link::string as away_team_spring_venue_link,
    data:gameData.teams.away.teamCode::string as away_team_team_code,
    data:gameData.teams.away.teamName::string as away_team_team_name,
    data:gameData.teams.away.venue.id::int as away_team_venue_id,
    data:gameData.teams.away.venue.link::string as away_team_venue_link,
    data:gameData.teams.away.venue.name::string as away_team_venue_name,

    -- Home Team
    data:gameData.teams.home.abbreviation::string as home_team_abbreviation,
    data:gameData.teams.home.active::boolean as home_team_active,
    data:gameData.teams.home.allStarStatus::string as home_team_all_star_status,
    data:gameData.teams.home.clubName::string as home_team_club_name,
    data:gameData.teams.home.division.id::int as home_team_division_id,
    data:gameData.teams.home.division.link::string as home_team_division_link,
    data:gameData.teams.home.division.name::string as home_team_division_name,
    data:gameData.teams.home.fileCode::string as home_team_file_code,
    data:gameData.teams.home.firstYearOfPlay::string as home_team_first_year_of_play,
    data:gameData.teams.home.franchiseName::string as home_team_franchise_name,
    data:gameData.teams.home.id::int as home_team_id,
    data:gameData.teams.home.league.id::int as home_team_league_id,
    data:gameData.teams.home.league.link::string as home_team_league_link,
    data:gameData.teams.home.league.name::string as home_team_league_name,
    data:gameData.teams.home.link::string as home_team_link,
    data:gameData.teams.home.locationName::string as home_team_location_name,
    data:gameData.teams.home.name::string as home_team_name,
    data:gameData.teams.home.record.conferenceGamesBack::string as home_team_record_conference_games_back,
    data:gameData.teams.home.record.divisionGamesBack::string as home_team_record_division_games_back,
    data:gameData.teams.home.record.divisionLeader::boolean as home_team_record_division_leader,
    data:gameData.teams.home.record.gamesPlayed::int as home_team_record_games_played,
    data:gameData.teams.home.record.leagueGamesBack::string as home_team_record_league_games_back,
    data:gameData.teams.home.record.leagueRecord.losses::int as home_team_record_league_losses,
    data:gameData.teams.home.record.leagueRecord.pct::double as home_team_record_league_pct,
    data:gameData.teams.home.record.leagueRecord.ties::int as home_team_record_league_ties,
    data:gameData.teams.home.record.leagueRecord.wins::int as home_team_record_league_wins,
    data:gameData.teams.home.record.losses::int as home_team_record_losses,
    data:gameData.teams.home.record.sportGamesBack::string as home_team_record_sport_games_back,
    data:gameData.teams.home.record.springLeagueGamesBack::string as home_team_record_spring_league_games_back,
    data:gameData.teams.home.record.wildCardGamesBack::string as home_team_record_wild_card_games_back,
    data:gameData.teams.home.record.winningPercentage::double as home_team_record_winning_percentage,
    data:gameData.teams.home.record.wins::int as home_team_record_wins,
    data:gameData.teams.home.season::string as home_team_season,
    data:gameData.teams.home.shortName::string as home_team_short_name,
    data:gameData.teams.home.sport.id::int as home_team_sport_id,
    data:gameData.teams.home.sport.link::string as home_team_sport_link,
    data:gameData.teams.home.sport.name::string as home_team_sport_name,
    data:gameData.teams.home.springLeague.abbreviation::string as home_team_spring_league_abbreviation,
    data:gameData.teams.home.springLeague.id::int as home_team_spring_league_id,
    data:gameData.teams.home.springLeague.link::string as home_team_spring_league_link,
    data:gameData.teams.home.springLeague.name::string as home_team_spring_league_name,
    data:gameData.teams.home.springVenue.id::int as home_team_spring_venue_id,
    data:gameData.teams.home.springVenue.link::string as home_team_spring_venue_link,
    data:gameData.teams.home.teamCode::string as home_team_team_code,
    data:gameData.teams.home.teamName::string as home_team_team_name,
    data:gameData.teams.home.venue.id::int as home_team_venue_id,
    data:gameData.teams.home.venue.link::string as home_team_venue_link,
    data:gameData.teams.home.venue.name::string as home_team_venue_name,

    -- Venue
    data:gameData.venue.id::int as venue_id,
    data:gameData.venue.name::string as venue_name,
    data:gameData.venue.link::string as venue_link,
    data:gameData.venue.location.address1::string as venue_address1,
    data:gameData.venue.location.azimuthAngle::string as venue_azimuth_angle,
    data:gameData.venue.location.city::string as venue_city,
    data:gameData.venue.location.country::string as venue_country,
    data:gameData.venue.location.defaultCoordinates.latitude::double as venue_latitude,
    data:gameData.venue.location.defaultCoordinates.longitude::double as venue_longitude,
    data:gameData.venue.location.elevation::int as venue_elevation,
    data:gameData.venue.location.phone::string as venue_phone,
    data:gameData.venue.location.postalCode::string as venue_postal_code,
    data:gameData.venue.location.state::string as venue_state,
    data:gameData.venue.location.stateAbbrev::string as venue_state_abbrev,
    data:gameData.venue.timeZone.id::string as venue_time_zone_id,
    data:gameData.venue.timeZone.offset::int as venue_time_zone_offset,
    data:gameData.venue.timeZone.offsetAtGameTime::int as venue_time_zone_offset_at_game_time,
    data:gameData.venue.timeZone.tz::string as venue_time_zone_tz,
    data:gameData.venue.fieldInfo.capacity::int as venue_field_capacity,
    data:gameData.venue.fieldInfo.center::int as venue_field_center,
    data:gameData.venue.fieldInfo.leftCenter::int as venue_field_left_center,
    data:gameData.venue.fieldInfo.leftLine::int as venue_field_left_line,
    data:gameData.venue.fieldInfo.rightCenter::int as venue_field_right_center,
    data:gameData.venue.fieldInfo.rightLine::int as venue_field_right_line,
    data:gameData.venue.fieldInfo.roofType::string as venue_field_roof_type,
    data:gameData.venue.fieldInfo.turfType::string as venue_field_turf_type,
    data:gameData.venue.active::boolean as venue_active,
    data:gameData.venue.season::int as venue_season,
    data:gameData.officialVenue.id::int as official_venue_id,
    data:gameData.officialVenue.link::string as official_venue_link,

    -- Weather & Flags
    data:gameData.weather.condition::string as weather_condition,
    data:gameData.weather.temp::int as weather_temp,
    data:gameData.weather.wind::string as weather_wind,
    data:gameData.gameInfo.attendance::int as game_info_attendance,
    data:gameData.gameInfo.firstPitch::timestamp_ntz as game_info_first_pitch,
    data:gameData.gameInfo.gameDurationMinutes::int as game_info_duration,
    data:gameData.review.hasChallenges::boolean as review_has_challenges,
    data:gameData.review.away.remaining::int as review_away_remaining,
    data:gameData.review.away.used::int as review_away_used,
    data:gameData.review.home.remaining::int as review_home_remaining,
    data:gameData.review.home.used::int as review_home_used,
    data:gameData.flags.noHitter::boolean as flags_no_hitter,
    data:gameData.flags.perfectGame::boolean as flags_perfect_game,
    data:gameData.flags.awayTeamNoHitter::boolean as flags_away_team_no_hitter,
    data:gameData.flags.awayTeamPerfectGame::boolean as flags_away_team_perfect_game,
    data:gameData.flags.homeTeamNoHitter::boolean as flags_home_team_no_hitter,
    data:gameData.flags.homeTeamPerfectGame::boolean as flags_home_team_perfect_game,

    -- Probable Pitchers, Officials
    data:gameData.probablePitchers.away.fullName::string as probable_pitchers_away_full_name,
    data:gameData.probablePitchers.away.id::int as probable_pitchers_away_id,
    data:gameData.probablePitchers.away.link::string as probable_pitchers_away_link,
    data:gameData.probablePitchers.home.fullName::string as probable_pitchers_home_full_name,
    data:gameData.probablePitchers.home.id::int as probable_pitchers_home_id,
    data:gameData.probablePitchers.home.link::string as probable_pitchers_home_link,
    data:gameData.officialScorer.id::int as official_scorer_id,
    data:gameData.officialScorer.fullName::string as official_scorer_full_name,
    data:gameData.officialScorer.link::string as official_scorer_link,
    data:gameData.primaryDatacaster.id::int as primary_datacaster_id,
    data:gameData.primaryDatacaster.fullName::string as primary_datacaster_full_name,
    data:gameData.primaryDatacaster.link::string as primary_datacaster_link,

    -- Mound Visits
    data:gameData.moundVisits.away.remaining::int as mound_visits_away_remaining,
    data:gameData.moundVisits.away.used::int as mound_visits_away_used,
    data:gameData.moundVisits.home.remaining::int as mound_visits_home_remaining,
    data:gameData.moundVisits.home.used::int as mound_visits_home_used,

    -- File Metadata
    file_path,
    file_name,
    file_size,
    file_modification_time,
    file_batch_time,
    last_update_time
FROM
    main.mlb_gumbo.bronze_mlb_gumbo_data
""")

display(df)

# COMMAND ----------

# spark.sql("drop table if exists main.abooth_db.silver_gumbo_game_data")
# df.write.format("delta").saveAsTable("main.abooth_db.silver_gumbo_game_data")

# COMMAND ----------

# Create a temporary view from the DataFrame
df.createOrReplaceTempView("temp_silver_mlb_gumbo_game_data")

# Use the MERGE INTO SQL command to update or insert records based on the game_pk column
spark.sql("""
MERGE INTO main.mlb_gumbo.silver_mlb_gumbo_game_data AS target
USING temp_silver_mlb_gumbo_game_data AS source
ON target.season = source.season and
  target.official_date = source.official_date and
  target.game_pk = source.game_pk  
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""").show()

# COMMAND ----------

# Pitch Data
df = spark.sql("""
SELECT
  -- Game Metadata
  data:gameData.game.season::int as season,
  data:gameData.datetime.officialDate::date as official_date,
  data:gameData.datetime.dateTime::timestamp_ntz as game_datetime,
  data:gamePk::int as game_pk,
  data:gameData.game.gamedayType::string as gameday_type,
  data:gameData.game.gameNumber::int as game_number,
  data:gameData.teams.away.id::integer as away_team_id,
  data:gameData.teams.away.name::string as away_team_name,
  data:gameData.teams.home.id::integer as home_team_id,
  data:gameData.teams.home.name::string as home_team_name,

  -- Play Metadata
  allPlays.value:result.event::string AS play_event,
  allPlays.value:result.eventType::string AS play_event_type,
  allPlays.value:result.description::string AS play_description,
  allPlays.value:result.rbi::int AS play_rbi,
  allPlays.value:result.awayScore::int AS post_play_away_score,
  allPlays.value:result.homeScore::int AS post_play_home_score,
  allPlays.value:about.atBatIndex::int AS at_bat_index,
  allPlays.value:about.halfInning::string AS half_inning,
  allPlays.value:about.isTopInning::boolean AS is_top_inning,
  allPlays.value:about.inning::int AS inning,
  allPlays.value:about.captivatingIndex::int AS captivating_index,

  -- Pitch Metadata
  playEvents.value:index::int AS pitch_index,
  playEvents.value:playId::string AS pitch_guid,
  playEvents.value:pitchNumber::int AS pitch_number,
  playEvents.value:startTime::string AS start_time,
  playEvents.value:endTime::string AS end_time,
  playEvents.value:isPitch::boolean AS is_pitch,
  playEvents.value:type::string AS event_type,

  -- Pitch Results
  playEvents.value:details:call:code::string AS call_code,
  playEvents.value:details:call:description::string AS call_description,
  playEvents.value:details:description::string AS pitch_description,
  playEvents.value:details:code::string AS pitch_code,
  playEvents.value:details:ballColor::string AS ball_color,
  playEvents.value:details:trailColor::string AS trail_color,
  playEvents.value:details:isInPlay::boolean AS is_in_play,
  playEvents.value:details:isStrike::boolean AS is_strike,
  playEvents.value:details:isBall::boolean AS is_ball,
  playEvents.value:details:type:code::string AS pitch_type_code,
  playEvents.value:details:type:description::string AS pitch_type_description,
  playEvents.value:details:isOut::boolean AS is_out,
  playEvents.value:details:hasReview::boolean AS has_review,

  -- Pitch Count
  playEvents.value:count:balls::int AS balls_count,
  playEvents.value:count:strikes::int AS strikes_count,
  playEvents.value:count:outs::int AS outs_count,

  playEvents.value:preCount:balls::int AS pre_balls_count,
  playEvents.value:preCount:strikes::int AS pre_strikes_count,
  playEvents.value:preCount:outs::int AS pre_outs_count,

  -- Pitch Data
  playEvents.value:pitchData:startSpeed::double AS pitch_start_speed,
  playEvents.value:pitchData:endSpeed::double AS pitch_end_speed,
  playEvents.value:pitchData:strikeZoneTop::double AS strike_zone_top,
  playEvents.value:pitchData:strikeZoneBottom::double AS strike_zone_bottom,

  playEvents.value:pitchData:coordinates:aY::double AS acceleration_y,
  playEvents.value:pitchData:coordinates:aZ::double AS acceleration_z,
  playEvents.value:pitchData:coordinates:pfxX::double AS pfx_x,
  playEvents.value:pitchData:coordinates:pfxZ::double AS pfx_z,
  playEvents.value:pitchData:coordinates:pX::double AS position_x,
  playEvents.value:pitchData:coordinates:pZ::double AS position_z,
  playEvents.value:pitchData:coordinates:vX0::double AS initial_velocity_x,
  playEvents.value:pitchData:coordinates:vY0::double AS initial_velocity_y,
  playEvents.value:pitchData:coordinates:vZ0::double AS initial_velocity_z,
  playEvents.value:pitchData:coordinates:x::double AS x,
  playEvents.value:pitchData:coordinates:y::double AS y,
  playEvents.value:pitchData:coordinates:x0::double AS initial_x,
  playEvents.value:pitchData:coordinates:y0::double AS initial_y,
  playEvents.value:pitchData:coordinates:z0::double AS initial_z,
  playEvents.value:pitchData:coordinates:aX::double AS acceleration_x,

  playEvents.value:pitchData:breaks:breakAngle::double AS pitch_break_angle,
  playEvents.value:pitchData:breaks:breakLength::double AS pitch_break_length,
  playEvents.value:pitchData:breaks:breakY::double AS pitch_break_y,
  playEvents.value:pitchData:breaks:breakVertical::double AS pitch_break_vertical,
  playEvents.value:pitchData:breaks:breakVerticalInduced::double AS pitch_break_vertical_induced,
  playEvents.value:pitchData:breaks:breakHorizontal::double AS pitch_break_horizontal,
  playEvents.value:pitchData:breaks:spinRate::int AS pitch_spin_rate,
  playEvents.value:pitchData:breaks:spinDirection::int AS pitch_spin_direction,

  playEvents.value:pitchData:zone::int AS pitch_zone,
  playEvents.value:pitchData:typeConfidence::double AS pitch_type_confidence,
  playEvents.value:pitchData:plateTime::double AS plate_time,
  playEvents.value:pitchData:extension::double AS extension,

  -- Hit Data
  playEvents.value:hitData:launchSpeed::double AS hit_launch_speed,
  playEvents.value:hitData:launchAngle::double AS hit_launch_angle,
  playEvents.value:hitData:totalDistance::double AS hit_total_distance,
  playEvents.value:hitData:trajectory::string AS hit_trajectory,
  playEvents.value:hitData:hardness::string AS hit_hardness,
  playEvents.value:hitData:location::string AS hit_location,
  playEvents.value:hitData:coordinates:coordX::double AS hit_coord_x,
  playEvents.value:hitData:coordinates:coordY::double AS hit_coord_y,

  -- Player Data
  -- Offense
  playEvents.value:offense:batter:id::int AS batter_id,
  allPlays.value:matchup.batter.fullName::string AS batter_full_name,
  playEvents.value:offense:batter:link::string AS batter_link,
  playEvents.value:offense:batter:batSide:code::string AS batter_side_code,
  playEvents.value:offense:batter:batSide:description::string AS batter_side_description,
  playEvents.value:offense:batterPosition:code::string AS batter_position_code,
  playEvents.value:offense:batterPosition:name::string AS batter_position_name,
  playEvents.value:offense:batterPosition:type::string AS batter_position_type,
  playEvents.value:offense:batterPosition:abbreviation::string AS batter_position_abbreviation,

  -- Defense
  playEvents.value:defense:pitcher:id::int AS pitcher_id,
  allPlays.value:matchup.pitcher.fullName::string AS pitcher_full_name,
  playEvents.value:defense:pitcher:link::string AS pitcher_link,
  playEvents.value:defense:pitcher:pitchHand:code::string AS pitcher_hand_code,
  playEvents.value:defense:pitcher:pitchHand:description::string AS pitcher_hand_description,
  playEvents.value:defense:catcher:id::int AS catcher_id,
  playEvents.value:defense:catcher:link::string AS catcher_link,
  playEvents.value:defense:first:id::int AS first_base_id,
  playEvents.value:defense:second:id::int AS second_base_id,
  playEvents.value:defense:third:id::int AS third_base_id,
  playEvents.value:defense:shortstop:id::int AS shortstop_id,
  playEvents.value:defense:left:id::int AS left_fielder_id,
  playEvents.value:defense:center:id::int AS center_fielder_id,
  playEvents.value:defense:right:id::int AS right_fielder_id,

  -- File Metadata
  file_path,
  file_name,
  file_size,
  file_modification_time,
  file_batch_time,
  last_update_time
FROM
  main.mlb_gumbo.bronze_mlb_gumbo_data,
  LATERAL variant_explode(data:liveData.plays.allPlays) AS allPlays,
  LATERAL variant_explode(allPlays.value:playEvents) AS playEvents
ORDER BY
  season,
  official_date,
  game_datetime,
  game_number,
  at_bat_index,
  pitch_index""")

display(df)

# COMMAND ----------

# spark.sql("drop table if exists main.abooth_db.silver_gumbo_pitch_data")
# df.write.format("delta").saveAsTable("main.abooth_db.silver_gumbo_pitch_data")

# COMMAND ----------

# Create a temporary view from the DataFrame
df.createOrReplaceTempView("temp_silver_mlb_gumbo_pitch_data")

# Use the MERGE INTO SQL command to update or insert records based on the game_pk, at_bat_index, pitch_index column
spark.sql("""
MERGE INTO main.mlb_gumbo.silver_mlb_gumbo_pitch_data AS target
USING temp_silver_mlb_gumbo_pitch_data AS source
ON target.season = source.season and
  target.official_date = source.official_date and
  target.game_pk = source.game_pk and 
  target.at_bat_index = source.at_bat_index and
  target.pitch_index = source.pitch_index
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""").show()

# COMMAND ----------


