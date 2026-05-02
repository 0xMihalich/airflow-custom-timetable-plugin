# Version History

## 0.1.1

* Improve pytests
* Improve docstrings
* Fixed DAG run creation for exact time schedules.

### Fixed
- Replaced `DagRunInfo.interval()` with `DagRunInfo.exact()` in `ExactTimetable.next_dagrun_info()`
to prevent zero-length data intervals that caused UI hangs when marking tasks as success or failed.

## 0.1.0

First version of base_dumper
