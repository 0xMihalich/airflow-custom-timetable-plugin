"""Tests for ExactTimetable."""

from airflow_custom_timetable_plugin import ExactTimetable
from pendulum import (
    datetime,
    UTC,
)
from unittest.mock import patch


class TestParseSchedules:
    """Tests for _parse_schedules method."""

    def test_daily_schedule(self) -> None:
        """Parse daily format: "hh:mm"."""

        timetable = ExactTimetable(schedules=["08:00"])
        assert timetable._parsed == [("daily", 8, 0)]  # noqa: S101

    def test_multiple_daily_schedules(self) -> None:
        """Parse multiple daily schedules, sorted by time."""

        timetable = ExactTimetable(schedules=["22:00", "08:00", "14:00"])
        assert timetable._parsed == [  # noqa: S101
            ("daily", 8, 0),
            ("daily", 14, 0),
            ("daily", 22, 0),
        ]

    def test_monthly_schedule(self) -> None:
        """Parse monthly format: "dd hh:mm"."""

        timetable = ExactTimetable(schedules=["15 10:30"])
        assert timetable._parsed == [("monthly", 15, 10, 30)]  # noqa: S101

    def test_yearly_schedule(self) -> None:
        """Parse yearly format: "mm.dd hh:mm"."""

        timetable = ExactTimetable(schedules=["12.31 23:59"])
        assert timetable._parsed == [("yearly", 12, 31, 23, 59)]  # noqa: S101

    def test_mixed_schedules(self) -> None:
        """Parse mixed schedule formats."""

        timetable = ExactTimetable(
            schedules=["08:00", "15 12:00", "01.01 00:00"]
        )
        assert len(timetable._parsed) == 3  # noqa: S101
        assert ("daily", 8, 0) in timetable._parsed  # noqa: S101
        assert ("monthly", 15, 12, 0) in timetable._parsed  # noqa: S101
        assert ("yearly", 1, 1, 0, 0) in timetable._parsed  # noqa: S101

    def test_empty_schedules(self) -> None:
        """Parse empty schedule list."""

        timetable = ExactTimetable(schedules=[])
        assert timetable._parsed == []  # noqa: S101

    def test_schedules_with_spaces(self) -> None:
        """Parse schedules with leading/trailing spaces."""

        timetable = ExactTimetable(schedules=["  08:00  "])
        assert timetable._parsed == [("daily", 8, 0)]  # noqa: S101


class TestNextMatch:
    """Tests for _next_match method."""

    def test_daily_future_today(self) -> None:
        """Next match is later today."""

        timetable = ExactTimetable(schedules=["18:00"])
        current = datetime(2024, 6, 15, 12, 0, tz=UTC)
        result = timetable._next_match(current)
        assert result == datetime(2024, 6, 15, 18, 0, tz=UTC)  # noqa: S101

    def test_daily_past_today(self) -> None:
        """Next match is tomorrow when time already passed."""

        timetable = ExactTimetable(schedules=["08:00"])
        current = datetime(2024, 6, 15, 12, 0, tz=UTC)
        result = timetable._next_match(current)
        assert result == datetime(2024, 6, 16, 8, 0, tz=UTC)  # noqa: S101

    def test_daily_exact_time(self) -> None:
        """When current time equals schedule, return tomorrow."""

        timetable = ExactTimetable(schedules=["12:00"])
        current = datetime(2024, 6, 15, 12, 0, tz=UTC)
        result = timetable._next_match(current)
        assert result == datetime(2024, 6, 16, 12, 0, tz=UTC)  # noqa: S101

    def test_monthly_future_this_month(self) -> None:
        """Next monthly match is later this month."""

        timetable = ExactTimetable(schedules=["20 10:00"])
        current = datetime(2024, 6, 15, 12, 0, tz=UTC)
        result = timetable._next_match(current)
        assert result == datetime(2024, 6, 20, 10, 0, tz=UTC)  # noqa: S101

    def test_monthly_past_this_month(self) -> None:
        """Next monthly match is next month when day already passed."""

        timetable = ExactTimetable(schedules=["10 10:00"])
        current = datetime(2024, 6, 15, 12, 0, tz=UTC)
        result = timetable._next_match(current)
        assert result == datetime(2024, 7, 10, 10, 0, tz=UTC)  # noqa: S101

    def test_yearly_future_this_year(self) -> None:
        """Next yearly match is later this year."""

        timetable = ExactTimetable(schedules=["12.31 23:59"])
        current = datetime(2024, 6, 15, 12, 0, tz=UTC)
        result = timetable._next_match(current)
        assert result == datetime(2024, 12, 31, 23, 59, tz=UTC)  # noqa: S101

    def test_yearly_past_this_year(self) -> None:
        """Next yearly match is next year when date already passed."""

        timetable = ExactTimetable(schedules=["01.01 00:00"])
        current = datetime(2024, 6, 15, 12, 0, tz=UTC)
        result = timetable._next_match(current)
        assert result == datetime(2025, 1, 1, 0, 0, tz=UTC)  # noqa: S101

    def test_multiple_candidates_earliest_wins(self) -> None:
        """Multiple schedules: return the earliest future match."""

        timetable = ExactTimetable(schedules=["14:00", "10:00"])
        current = datetime(2024, 6, 15, 9, 0, tz=UTC)
        result = timetable._next_match(current)
        assert result == datetime(2024, 6, 15, 10, 0, tz=UTC)  # noqa: S101

    def test_empty_returns_none(self) -> None:
        """Empty schedules return None."""

        timetable = ExactTimetable(schedules=[])
        current = datetime(2024, 6, 15, 12, 0, tz=UTC)
        result = timetable._next_match(current)
        assert result is None  # noqa: S101


class TestNextDagrunInfo:
    """Tests for next_dagrun_info method."""

    @patch("airflow_custom_timetable_plugin.extract_timetable.DateTime")
    def test_first_run_no_previous_interval(self, mock_datetime) -> None:
        """First DAG run: data_interval start equals end."""

        mock_datetime.now.return_value = datetime(
            2024, 6, 15, 9, 0, tz=UTC
        )
        timetable = ExactTimetable(schedules=["12:00"])
        result = timetable.next_dagrun_info()
        expected = datetime(2024, 6, 15, 12, 0, tz=UTC)
        assert result is not None  # noqa: S101
        assert result.run_after == expected  # noqa: S101
        assert result.data_interval.start == expected  # noqa: S101
        assert result.data_interval.end == expected  # noqa: S101

    @patch("airflow_custom_timetable_plugin.extract_timetable.DateTime")
    def test_subsequent_run_continuous_interval(self, mock_datetime) -> None:
        """Subsequent run: should use DagRunInfo.exact()."""

        mock_datetime.now.return_value = datetime(
            2024, 6, 15, 13, 0, tz=UTC
        )
        timetable = ExactTimetable(schedules=["12:00", "18:00"])

        expected_end = datetime(2024, 6, 15, 18, 0, tz=UTC)
        result = timetable.next_dagrun_info()

        assert result is not None  # noqa: S101
        assert result.run_after == expected_end  # noqa: S101
        assert result.data_interval.start == expected_end  # noqa: S101
        assert result.data_interval.end == expected_end  # noqa: S101

    @patch("airflow_custom_timetable_plugin.extract_timetable.DateTime")
    def test_no_future_match_returns_none(self, mock_datetime) -> None:
        """Empty schedules: next_dagrun_info returns None."""

        mock_datetime.now.return_value = datetime(
            2024, 6, 15, 12, 0, tz=UTC
        )
        timetable = ExactTimetable(schedules=[])
        result = timetable.next_dagrun_info()
        assert result is None  # noqa: S101


class TestSerialize:
    """Tests for serialization."""

    def test_serialize_roundtrip(self) -> None:
        """Serialize and deserialize should produce equivalent timetable."""

        original = ExactTimetable(
            schedules=["08:00", "15 12:00", "12.31 23:59"]
        )
        data = original.serialize()
        restored = ExactTimetable.deserialize(data)
        assert restored.schedules == original.schedules  # noqa: S101
        assert restored._parsed == original._parsed  # noqa: S101

    def test_serialize_empty(self) -> None:
        """Serialize empty schedules."""

        timetable = ExactTimetable(schedules=[])
        data = timetable.serialize()
        assert data == {"schedules": []}  # noqa: S101

    def test_deserialize_empty(self) -> None:
        """Deserialize empty data."""

        timetable = ExactTimetable.deserialize({})
        assert timetable.schedules == []  # noqa: S101
        assert timetable._parsed == []  # noqa: S101


class TestManualTrigger:
    """Tests for manual trigger."""

    def test_infer_manual_data_interval(self) -> None:
        """Manual trigger returns same start and end."""

        timetable = ExactTimetable(schedules=["08:00"])
        run_after = datetime(2024, 6, 15, 14, 30, tz=UTC)
        result = timetable.infer_manual_data_interval(run_after)
        assert result.start == run_after  # noqa: S101
        assert result.end == run_after  # noqa: S101
