# Airflow Custom Timetable Plugin

Universal custom timetable plugin for Apache Airflow with support for multiple schedule formats.

![Tests](https://github.com/0xMihalich/airflow-custom-timetable-plugin/actions/workflows/run_tests.yml/badge.svg)
![PyPI](https://img.shields.io/pypi/v/airflow-custom-timetable-plugin?cacheSeconds=1)

## Installation

```bash
pip install airflow-custom-timetable-plugin
```

From git

```bash
pip install git+https://github.com/0xMihalich/airflow-custom-timetable-plugin.git
```

## Supported Schedule Formats

| Format | Example | Description |
|--------|---------|-------------|
| `hh:mm` | `"08:00"` | Daily at specified time |
| `dd hh:mm` | `"15 08:00"` | Monthly on specified day and time |
| `mm.dd hh:mm` | `"01.01 00:00"` | Yearly on specified month, day and time |

## Usage

```python
from airflow import DAG
from airflow_custom_timetable_plugin import ExactTimetable

with DAG(
    dag_id="my_dag",
    schedule=ExactTimetable(
        schedules=[
            "08:00",         # Every day at 08:00
            "20:15",         # Every day at 20:00
            "1 12:00",       # 1st day of every month at 12:00
            "01.01 00:00",   # January 1st at 00:00
        ]
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    ...
```

## Examples

### Daily schedules only

```python
schedule = ExactTimetable(
    schedules=["02:00", "06:00", "10:00", "14:00", "18:00", "22:00"]
)
```

### Mixed schedules

```python
schedule = ExactTimetable(
    schedules=[
        "08:00",         # Daily at 8 AM
        "1 10:05",       # 1st of each month at 10 AM
        "15 14:00",      # 15th of each month at 2 PM
        "12.31 23:59",   # December 31st at 23:59
    ]
)
```

## How It Works

The plugin parses schedule strings into structured entries and computes the next valid run time by finding the earliest future match across all configured schedules.

For the first automated run, the data interval starts and ends at the same time. For subsequent runs, the interval starts from the end of the previous interval, ensuring continuous data coverage.

## Requirements

- Python >= 3.10
- apache-airflow >= 2.4.3
- pendulum >= 3.0.0

## License

See the [LICENSE](LICENSE) file for details.

## Author

0xMihalich <bayanmobile87@gmail.com>
