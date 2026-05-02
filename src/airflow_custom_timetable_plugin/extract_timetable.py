from airflow.timetables.base import (
    DagRunInfo,
    DataInterval,
    TimeRestriction,
    Timetable,
)
from pendulum import (
    DateTime,
    UTC,
)


class ExactTimetable(Timetable):
    """Universal custom timetable.

    Supported schedule formats:
        "hh:mm"       - daily at specified time
        "dd hh:mm"    - monthly on specified day and time
        "mm.dd hh:mm" - yearly on specified month, day and time
    """

    def __init__(self, schedules: list[str]) -> None:
        """Class initialization."""

        self.schedules = schedules
        self._parsed = self._parse_schedules(schedules)

    @staticmethod
    def _parse_schedules(schedules: list[str]) -> list[tuple]:
        """Parse schedule strings into structured tuples.

        Supports three formats:
            - "hh:mm" -> ("daily", hour, minute)
            - "dd hh:mm" -> ("monthly", day, hour, minute)
            - "mm.dd hh:mm" -> ("yearly", month, day, hour, minute)

        Args:
            schedules: List of schedule strings to parse.

        Returns:
            List of parsed tuples sorted by time components."""

        parsed = []

        for s in schedules:
            s = s.strip()
            parts = s.split()

            if len(parts) == 1:
                h, m = map(int, parts[0].split(":"))
                parsed.append(("daily", h, m))

            elif len(parts) == 2:
                first, second = parts
                h, m = map(int, second.split(":"))

                if "." in first:
                    month, day = map(int, first.split("."))
                    parsed.append(("yearly", month, day, h, m))
                else:
                    d = int(first)
                    parsed.append(("monthly", d, h, m))

        return sorted(parsed, key=lambda x: x[1:])

    def _next_match(self, current: DateTime) -> DateTime | None:
        """Find the nearest future match for the given time.

        Iterates through all parsed schedules and computes the next valid
        run time for each entry, returning the earliest one.

        Args:
            current: The current datetime to find the next match from.

        Returns:
            The earliest future DateTime matching any schedule,
            or None if no schedules are configured."""

        candidates = []

        for entry in self._parsed:
            kind = entry[0]

            if kind == "daily":
                _, h, m = entry
                candidate = current.set(
                    hour=h, minute=m, second=0, microsecond=0
                )

                if candidate <= current:
                    candidate = candidate.add(days=1)

                candidates.append(candidate)

            elif kind == "monthly":
                _, d, h, m = entry
                candidate = current.set(
                    day=d, hour=h, minute=m, second=0, microsecond=0
                )

                if candidate <= current:
                    candidate = candidate.add(months=1)

                candidates.append(candidate)

            elif kind == "yearly":
                _, month, d, h, m = entry
                candidate = current.set(
                    month=month,
                    day=d,
                    hour=h,
                    minute=m,
                    second=0,
                    microsecond=0,
                )

                if candidate <= current:
                    candidate = candidate.add(years=1)

                candidates.append(candidate)

        return min(candidates) if candidates else None

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        """Return the data interval for a manual trigger.

        Args:
            run_after: The datetime the DAG run was manually triggered.

        Returns:
            A DataInterval with both start and end set to run_after."""

        return DataInterval(start=run_after, end=run_after)

    def next_dagrun_info(
        self,
        last_automated_data_interval: DataInterval | None = None,
        restriction: TimeRestriction = None,
    ) -> DagRunInfo | None:
        """Determine the next automated DAG run interval.

        Computes the next run time based on the configured schedules.
        If a previous interval exists, the new interval starts from its end,
        ensuring continuous data intervals. For the first run, the interval
        starts and ends at the same time.

        Returns:
            DagRunInfo with the computed start and end of the next interval,
            or None if no future schedule is found."""

        _ = last_automated_data_interval, restriction
        current_time = DateTime.now(UTC)
        end = self._next_match(current_time)

        if end:
            return DagRunInfo.exact(end)

    def serialize(self) -> dict:
        """Serialize timetable for transport between scheduler and webserver.

        Returns:
            A dictionary containing the schedules list for deserialization."""

        return {"schedules": self.schedules}

    @classmethod
    def deserialize(cls, data: dict) -> "ExactTimetable":
        """Deserialize a timetable from a dictionary.

        Args:
            data: Dictionary containing the serialized schedules.

        Returns:
            A new ExactTimetable instance with the restored schedules."""

        return cls(data.get("schedules", []))
