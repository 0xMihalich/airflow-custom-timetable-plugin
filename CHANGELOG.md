# Version History

## 1.0.0

- Update README.md
- Update pytests

### Added
- Added `description` property to `ExactTimetable` for human-readable schedule display in UI.

### Changed
- Changed `max_active_runs` recommendation to 1 for fixed-time schedules to prevent unexpected parallel runs.
- Promoted plugin from beta to stable release.

### Fixed
- Fixed calendar view not showing scheduled DAG runs by using `last_automated_data_interval.end` as the starting point for computing the next run.
- Fixed infinite loop in UI when manually changing task status by adding `restriction` support in `next_dagrun_info()`.
- Fixed potential infinite loop in `_next_match()` for `monthly` and `yearly` schedules by adding `max_iterations` limit.
- Fixed invalid date handling for `monthly` and `yearly` schedules (e.g., February 31).
- Fixed zero-length data intervals by replacing `DagRunInfo.interval()` with `DagRunInfo.exact()`.

### Removed
- Removed beta status from classifiers and documentation.

## 0.1.3

- Update pytests

### Fixed
- Fixed calendar view not showing scheduled DAG runs by using `last_automated_data_interval.end`
as the starting point for computing the next run instead of `DateTime.now(UTC)`.

### Changed
- `next_dagrun_info()` now correctly chains future runs when `last_automated_data_interval`
is provided, enabling Airflow to build the calendar view across all supported versions (2.4+).

## 0.1.2

- Refactor ExactTimetable._next_match() method

### Fixed
- Fixed infinite loop in UI when manually changing task status by adding `restriction` support in `next_dagrun_info()`.
- Fixed potential infinite loop in `_next_match()` for `monthly` and `yearly` schedules by adding `max_iterations` limit.
- Fixed `monthly` schedule parsing where invalid dates (e.g., February 31) were not properly handled.

### Changed
- Moved `candidate` creation inside retry loop for `monthly` and `yearly` schedules to correctly handle invalid dates.

## 0.1.1

* Improve pytests
* Improve docstrings
* Fixed DAG run creation for exact time schedules.

### Fixed
- Replaced `DagRunInfo.interval()` with `DagRunInfo.exact()` in `ExactTimetable.next_dagrun_info()`
to prevent zero-length data intervals that caused UI hangs when marking tasks as success or failed.

## 0.1.0

First version of base_dumper
