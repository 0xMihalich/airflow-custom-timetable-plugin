from pathlib import Path
from setuptools import setup


readme = Path(__file__).parent / "README.md"

setup(
    name="airflow-custom-timetable-plugin",
    version="0.1.0",
    description=(
        "Universal Apache Airflow plugin with custom timetable support"
    ),
    long_description=readme.read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    url="https://github.com/0xmihalich/airflow-custom-timetable-plugin",
    author="0xMihalich",
    author_email="bayanmobile87@gmail.com",
    package_dir={"": "src"},
    packages=[
        "airflow_custom_timetable_plugin",
    ],
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.10",
    install_requires=[
        "apache-airflow>=2.4.3",
        "pendulum>=3.0.0",
    ],
    entry_points={
        "airflow.plugins": [
            "exact_timetable = airflow_custom_timetable_plugin:"
            "ExactTimetablePlugin",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    ],
)
