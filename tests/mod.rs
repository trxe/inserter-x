pub mod tools;

#[cfg(test)]
mod tests {
    use polars::prelude::{IntoLazy, col};

    use crate::tools::tests::{
        clickhouse_converter, get_sample_df_numerical, parse_json_from_url, run_sql,
        send_db_from_inserter,
    };

    const SAMPLE_URL: &str = "https://records.nhl.com/site/api/draft?include=draftProspect.id&include=player.birthStateProvince&include=player.birthCountry&include=player.position&include=player.onRoster&include=player.yearsPro&include=player.firstName&include=player.lastName&include=player.id&include=team.id&include=team.placeName&include=team.commonName&include=team.fullName&include=team.triCode&include=team.logos&include=franchiseTeam.franchise.mostRecentTeamId&include=franchiseTeam.franchise.teamCommonName&include=franchiseTeam.franchise.teamPlaceName&sort=[{%22property%22:%22overallPickNumber%22,%22direction%22:%22ASC%22}]&cayenneExp=%20draftYear%20=%202024&start=0&limit=50";

    #[test]
    fn json_to_clickhouse_deep_nesting_date() {
        let (dfname, frame) = parse_json_from_url("draft", SAMPLE_URL);
        let lf = run_sql(
            "select data.* from (SELECT unnest(data) FROM self) inner_data;",
            &[("self", frame.lazy())],
        )
        .with_column(col("birthDate").cast(polars::prelude::DataType::Date))
        .with_column(col("notes").cast(polars::prelude::DataType::String))
        .with_column(col("removedOutrightWhy").cast(polars::prelude::DataType::String));
        let dataframe = lf.collect().unwrap();
        let ch = clickhouse_converter(
            &dfname,
            &dataframe,
            Some("default"),
            Some("MergeTree"),
            Some("pickInRound"),
            None,
            Some("playerName,id,pickInRound"),
            Some("CREATE OR REPLACE TABLE"),
        );
        send_db_from_inserter("http://default:password@localhost:8123/", &ch, &dataframe);
    }

    #[test]
    fn json_to_clickhouse_deeper_nesting_datetime() {
        let (_, frame) =
            parse_json_from_url("games", "https://api-web.nhle.com/v1/score/2023-11-10");
        let games_lf = run_sql(
            "select games.* from (SELECT unnest(games) FROM self) inner_data;",
            &[("self", frame.lazy())],
        );
        let goals_lf = run_sql(
            "select goals.* from (SELECT unnest(goals) FROM self) inner_data;",
            &[("self", games_lf.clone())],
        );
        let games = games_lf.collect().unwrap();
        let goals = goals_lf.collect().unwrap();
        let ch = clickhouse_converter(
            "games",
            &games,
            Some("default"),
            Some("MergeTree"),
            None,
            Some("id"),
            None,
            Some("CREATE OR REPLACE TABLE"),
        );
        send_db_from_inserter("http://default:password@localhost:8123/", &ch, &games);
        let ch = clickhouse_converter(
            "goals",
            &goals,
            Some("default"),
            Some("MergeTree"),
            Some("playerId"),
            None,
            None,
            Some("CREATE OR REPLACE TABLE"),
        );
        send_db_from_inserter("http://default:password@localhost:8123/", &ch, &goals);
    }

    #[test]
    fn basic_numerical_types() {
        let db = get_sample_df_numerical();
        let ch = clickhouse_converter(
            "numerical_test",
            &db,
            Some("default"),
            Some("MergeTree"),
            None,
            Some("uint8"),
            None,
            Some("CREATE OR REPLACE TABLE"),
        );
        send_db_from_inserter("http://default:password@localhost:8123/", &ch, &db);
    }
}
