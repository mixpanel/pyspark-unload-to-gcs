import json
import runpy
import sys
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest


@pytest.mark.parametrize("row_count", [0, 1, 5_000_000, 5_000_001])
@pytest.mark.parametrize("suffix", [";\n", " -- trailing comment", " -- trailing comment\n"])
def test_query_job(monkeypatch, row_count, suffix):
    query = "SELECT id AS lookup_id, name AS label FROM source" + suffix
    normalized = query.strip().rstrip(";").rstrip()
    export_query = f"SELECT * FROM ({normalized}\n) AS raw_query"
    spark = MagicMock()
    count_result = MagicMock()
    count_result.first.return_value = (row_count,)
    dataframe = MagicMock()
    dataframe.dtypes = [("lookup_id", "string"), ("label", "string")]
    spark.sql.side_effect = [count_result, dataframe]
    dbutils = MagicMock()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "export.py",
            "--query",
            query,
            "--sync_type",
            "full",
            "--validate_row_count",
            "5000000",
            "--bucket",
            "test",
            "--prefix",
            "lookup",
        ],
    )
    script = str(Path(__file__).resolve().parents[1] / "export.py")
    if row_count > 5_000_000:
        with pytest.raises(Exception, match="exceeds limit"):
            runpy.run_path(
                script, run_name="__main__", init_globals={"spark": spark, "dbutils": dbutils}
            )
        dataframe.write.format.assert_not_called()
        dbutils.notebook.exit.assert_not_called()
    else:
        runpy.run_path(
            script, run_name="__main__", init_globals={"spark": spark, "dbutils": dbutils}
        )
        assert spark.sql.call_args_list[1] == call(export_query, args={})
        dataframe.write.format.assert_called_once_with("json")
        dataframe.write.format.return_value.option.return_value.mode.return_value.save.assert_called_once_with(
            "gs://test//lookup/"
        )
        assert json.loads(dbutils.notebook.exit.call_args.args[0])["query"] == export_query
    assert spark.sql.call_args_list[0] == call(
        f"SELECT count(*) FROM ({export_query}\n) AS raw_query"
    )
    assert spark.sql.call_count == (1 if row_count > 5_000_000 else 2)


@pytest.mark.parametrize(
    "options",
    [
        ["--sync_type", "cdc"],
        ["--sync_type", "time-based"],
        ["--sync_type", "scd-latest"],
        ["--sync_type", "full", "--use_custom_sql"],
    ],
)
def test_query_rejects_incremental_modes(monkeypatch, options):
    spark = MagicMock()
    monkeypatch.setattr(sys, "argv", ["export.py", "--query", "SELECT 1", *options])
    with pytest.raises(ValueError, match="--query requires"):
        runpy.run_path(
            str(Path(__file__).resolve().parents[1] / "export.py"),
            run_name="__main__",
            init_globals={"spark": spark},
        )
    spark.sql.assert_not_called()


def test_table_count_unchanged():
    from export import validate_row_count

    spark = MagicMock()
    spark.sql.return_value.first.return_value = (10,)
    validate_row_count(spark, "catalog", "schema", "table", 10)
    spark.sql.assert_called_once_with("SELECT count(*) FROM catalog.schema.table")


@pytest.mark.parametrize("limit", ["0", "5000000"])
def test_failed_query_does_not_export(monkeypatch, limit):
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("Invalid SQL")
    dbutils = MagicMock()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "export.py",
            "--query",
            "SELECT missing FROM source",
            "--sync_type",
            "full",
            "--validate_row_count",
            limit,
        ],
    )
    with pytest.raises(Exception, match="Invalid SQL"):
        runpy.run_path(
            str(Path(__file__).resolve().parents[1] / "export.py"),
            run_name="__main__",
            init_globals={"spark": spark, "dbutils": dbutils},
        )
    spark.sql.return_value.write.format.assert_not_called()
    dbutils.notebook.exit.assert_not_called()


@pytest.mark.parametrize("query", ["   ", ";\n"])
def test_blank_sql_rejected(monkeypatch, query):
    spark = MagicMock()
    monkeypatch.setattr(sys, "argv", ["export.py", "--query", query, "--sync_type", "full"])
    with pytest.raises(ValueError, match="must contain a SQL query"):
        runpy.run_path(
            str(Path(__file__).resolve().parents[1] / "export.py"),
            run_name="__main__",
            init_globals={"spark": spark},
        )
    spark.sql.assert_not_called()
