import argparse
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from export import (
    HIVE_METASTORE,
    build_query,
    check_procedure_exists,
    datetime_to_ms,
    generate_filter,
    ms_to_datetime,
)


def test_ms_to_datetime():
    result = ms_to_datetime(1704067200000)
    expected = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert result == expected


def test_datetime_to_ms_truncates_microseconds():
    # 500 microseconds should be truncated (rounds down)
    dt = datetime(2024, 1, 1, 0, 0, 0, 500, tzinfo=timezone.utc)
    result = datetime_to_ms(dt)
    assert result == 1704067200000

    # 999 microseconds should also be truncated
    dt = datetime(2024, 1, 1, 0, 0, 0, 999, tzinfo=timezone.utc)
    result = datetime_to_ms(dt)
    assert result == 1704067200000

    # 1000 microseconds = 1 millisecond, should add 1ms
    dt = datetime(2024, 1, 1, 0, 0, 0, 1000, tzinfo=timezone.utc)
    result = datetime_to_ms(dt)
    assert result == 1704067200001


def test_check_procedure_exists_returns_false_for_hive_metastore():
    spark = MagicMock()
    result = check_procedure_exists(spark, HIVE_METASTORE, "schema", "proc")
    assert result is False
    spark.sql.assert_not_called()


def test_generate_filter_empty():
    assert generate_filter(None) == ""
    assert generate_filter("") == ""


def test_generate_filter_single_column():
    result = generate_filter("user_id")
    assert result == "user_id IS NOT NULL AND user_id != ''"


def test_generate_filter_multiple_columns():
    result = generate_filter("user_id,email")
    expected = "user_id IS NOT NULL AND user_id != '' AND email IS NOT NULL AND email != ''"
    assert result == expected


def _make_args(**kwargs) -> argparse.Namespace:
    """Helper to create args namespace with defaults."""
    defaults = {
        "catalog": "test_catalog",
        "schema_name": "test_schema",
        "table": "test_table",
        "sync_type": "full",
        "non_nullable_columns": None,
        "time_cutoff_ms": 0,
        "updated_time_column": "updated_at",
        "delay_ms": 0,
        "now_ms": 0,
        "group_id_column": None,
        "scd_time_column": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestBuildQueryFull:
    def test_full_sync_basic(self):
        spark = MagicMock()
        args = _make_args(sync_type="full")

        query, ts = build_query(spark, args)

        assert query == "SELECT * FROM test_catalog.test_schema.test_table"
        assert ts == 0

    def test_full_sync_with_filter(self):
        spark = MagicMock()
        args = _make_args(sync_type="full", non_nullable_columns="user_id,email")

        query, ts = build_query(spark, args)

        assert query == (
            "SELECT * FROM test_catalog.test_schema.test_table "
            "WHERE user_id IS NOT NULL AND user_id != '' "
            "AND email IS NOT NULL AND email != ''"
        )
        assert ts == 0


class TestBuildQueryTimeBased:
    def test_time_based_basic(self):
        spark = MagicMock()
        args = _make_args(sync_type="time-based", time_cutoff_ms=1000000)

        query, ts = build_query(spark, args)

        assert query == (
            "SELECT * FROM test_catalog.test_schema.test_table "
            "WHERE unix_timestamp(updated_at)*1000 >= 1000000"
        )
        assert ts == 0

    def test_time_based_with_delay(self):
        spark = MagicMock()
        args = _make_args(
            sync_type="time-based",
            time_cutoff_ms=1000000,
            delay_ms=5000,
            now_ms=2000000,
        )

        query, ts = build_query(spark, args)

        assert query == (
            "SELECT * FROM test_catalog.test_schema.test_table "
            "WHERE unix_timestamp(updated_at)*1000 >= 1000000 "
            "AND unix_timestamp(updated_at)*1000 <= 1995000"
        )
        assert ts == 0


class TestBuildQueryScdLatest:
    def test_scd_latest_basic(self):
        spark = MagicMock()
        args = _make_args(
            sync_type="scd-latest",
            group_id_column="user_id",
            scd_time_column="updated_at",
        )

        query, ts = build_query(spark, args)

        assert query == (
            "SELECT *\n"
            "FROM (\n"
            "    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS row_num\n"
            "    FROM test_catalog.test_schema.test_table\n"
            ") RankedRows\n"
            "WHERE row_num = 1"
        )
        assert ts == 0

    def test_scd_latest_missing_columns_raises(self):
        spark = MagicMock()
        args = _make_args(sync_type="scd-latest")

        with pytest.raises(ValueError, match="scd-latest sync requires"):
            build_query(spark, args)


class TestBuildQueryCdc:
    def test_cdc_first_sync_no_procedure(self):
        spark = MagicMock()
        # Mock: procedure doesn't exist, return commit timestamp
        spark.sql.return_value.collect.return_value = []
        spark.sql.return_value.first.return_value = [
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ]

        args = _make_args(sync_type="cdc", time_cutoff_ms=0)

        query, ts = build_query(spark, args)

        assert query == (
            "SELECT 'INSERT' as _mp_change_type, * "
            "FROM test_catalog.test_schema.test_table "
            "TIMESTAMP AS OF '2024-01-01T12:00:00+00:00'"
        )
        assert ts == datetime_to_ms(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc))

    def test_cdc_incremental_sync_no_procedure(self):
        spark = MagicMock()
        # Mock: procedure doesn't exist, return current timestamp with microseconds
        spark.sql.return_value.collect.return_value = []
        spark.sql.return_value.first.return_value = [
            datetime(2024, 1, 2, 12, 0, 0, 123456, tzinfo=timezone.utc)
        ]

        # 1704110400123 = 2024-01-01T12:00:00.123Z, +1ms = 2024-01-01T12:00:00.124Z
        args = _make_args(sync_type="cdc", time_cutoff_ms=1704110400123)

        query, ts = build_query(spark, args)

        assert query == (
            "\n"
            "    SELECT CASE\n"
            "        WHEN _change_type = 'update_postimage' THEN 'INSERT'\n"
            "        WHEN _change_type = 'update_preimage' THEN 'DELETE'\n"
            "        WHEN _change_type = 'insert' THEN 'INSERT'\n"
            "        ELSE 'DELETE'\n"
            "    END as _mp_change_type, *\n"
            "    FROM table_changes('test_catalog.test_schema.test_table', '2024-01-01T12:00:00.124000+00:00', '2024-01-02T12:00:00.123456+00:00')\n"
            "    "
        )
        # Microseconds are truncated when converting to ms
        assert ts == 1704196800123

    def test_cdc_with_procedure_first_sync(self):
        spark = MagicMock()
        # Mock: procedure exists
        spark.sql.return_value.collect.return_value = [("some", "result")]
        spark.sql.return_value.first.return_value = [
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ]

        args = _make_args(sync_type="cdc", time_cutoff_ms=0)

        query, ts = build_query(spark, args)

        assert query == (
            "CALL test_catalog.test_schema.get_test_table_cdc("
            "'FIRST_SYNC', '2024-01-01T12:00:00+00:00')"
        )
        assert ts == datetime_to_ms(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc))

    def test_cdc_with_procedure_incremental(self):
        spark = MagicMock()
        # Mock: procedure exists, return timestamp with microseconds
        spark.sql.return_value.collect.return_value = [("some", "result")]
        spark.sql.return_value.first.return_value = [
            datetime(2024, 1, 2, 12, 0, 0, 456789, tzinfo=timezone.utc)
        ]

        # 1704110400456 = 2024-01-01T12:00:00.456Z, +1ms = 2024-01-01T12:00:00.457Z
        args = _make_args(sync_type="cdc", time_cutoff_ms=1704110400456)

        query, ts = build_query(spark, args)

        assert query == (
            "CALL test_catalog.test_schema.get_test_table_cdc("
            "'2024-01-01T12:00:00.457000+00:00', '2024-01-02T12:00:00.456789+00:00')"
        )
        # Microseconds are truncated when converting to ms
        assert ts == 1704196800456
