# 🔍 Eltűnt Személy Kereső

A Streamlit application for tracking and analyzing missing persons data from the Hungarian Police website.


[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://missing.streamlit.app/)

## Features

### 1. Search for Missing Persons
- Filter by name, birth place, and birthdate range
- View detailed information about missing persons
- Export results to Excel

### 2. Database Comparison and Tracking
- Merge existing tracking database with new data
- Track changes in missing person status over time
- Identify newly missing persons and those who are no longer listed

## How to run it on your own machine

1. Install the requirements

```shell
$ pip install -r requirements.txt
```


2. Run the app

```shell
$ streamlit run streamlit_app.py
```

## Data Source

This application scrapes data from the [Hungarian Police website](https://www.police.hu/hu/koral/eltunt-szemelyek) to provide up-to-date information on missing persons.

## Usage Notes

- The search function allows you to find current missing persons based on various criteria
- The comparison function helps track changes in missing persons data over time
- All data is presented in Hungarian