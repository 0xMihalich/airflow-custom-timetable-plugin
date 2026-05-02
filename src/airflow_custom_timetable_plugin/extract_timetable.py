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

    @staticmethod
    def _daily_candidate(current: DateTime, h: int, m: int) -> DateTime:
        """Compute next daily candidate."""
        candidate = current.set(hour=h, minute=m, second=0, microsecond=0)
        if candidate <= current:
            candidate = candidate.add(days=1)
        return candidate

    @staticmethod
    def _monthly_candidate(
        current: DateTime, d: int, h: int, m: int, max_iterations: int = 1000
    ) -> DateTime | None:
        """Compute next monthly candidate with retry for invalid dates."""
        for _ in range(max_iterations):
            try:
                candidate = current.set(
                    day=d, hour=h, minute=m, second=0, microsecond=0
                )
                if candidate <= current:
                    candidate = candidate.add(months=1)
                return candidate
            except ValueError:
                current = current.add(months=1)
        return None

    @staticmethod
    def _yearly_candidate(
        current: DateTime,
        month: int,
        d: int,
        h: int,
        m: int,
        max_iterations: int = 1000,
    ) -> DateTime | None:
        """Compute next yearly candidate with retry for invalid dates."""
        for _ in range(max_iterations):
            try:
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
                return candidate
            except ValueError:
                current = current.add(years=1)
        return None

    def _next_match(
        self,
        current: DateTime,
        max_iterations: int = 1000,
    ) -> DateTime | None:
        """Find the nearest future match with iteration
        limit to prevent infinite loops."""

        candidates = []

        for entry in self._parsed:
            kind = entry[0]

            if kind == "daily":
                candidate = self._daily_candidate(current, entry[1], entry[2])
                candidates.append(candidate)

            elif kind == "monthly":
                candidate = self._monthly_candidate(
                    current, entry[1], entry[2], entry[3], max_iterations
                )
                if candidate:
                    candidates.append(candidate)

            elif kind == "yearly":
                candidate = self._yearly_candidate(
                    current,
                    entry[1],
                    entry[2],
                    entry[3],
                    entry[4],
                    max_iterations,
                )
                if candidate:
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
        """Determine the next automated DAG run interval."""

        if last_automated_data_interval is not None:
            current_time = last_automated_data_interval.end
        else:
            current_time = DateTime.now(UTC)

        if (
            restriction
            and restriction.earliest
            and restriction.earliest > current_time
        ):
            current_time = restriction.earliest

        end = self._next_match(current_time, max_iterations=1000)

        if end is None:
            return None

        if restriction and restriction.latest and end > restriction.latest:
            return None

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

    @property
    def description(self) -> str:
        """Human-readable description of the schedule."""

        if not self.schedules:
            return "No schedule"

        return f"Fixed times: {', '.join(self.schedules)}"
