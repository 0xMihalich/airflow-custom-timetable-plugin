"""Universal Apache Airflow plugin with custom timetable support."""

from airflow.plugins_manager import AirflowPlugin

from .extract_timetable import ExactTimetable


class ExactTimetablePlugin(AirflowPlugin):
    """Plugin for registering the custom timetable."""

    name = "exact_timetable_plugin"
    timetables = [ExactTimetable]


__all__ = (
    "ExactTimetable",
    "ExactTimetablePlugin",
)
__author__ = "0xMihalich"
__version__ = "0.1.1"
