# Version History

## 0.1.2

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
