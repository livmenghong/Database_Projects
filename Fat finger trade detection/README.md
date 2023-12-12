# Coursework Two - Big Data in Quantitave Finance 

## Introduction

The script "Main.py" can be used to retrieve all trades for a given date from MongoDB, aggregate it and insert the aggregation into SQLite. On top of that, this script will also perform an incorrect trade detection analysis on the the trades retrieved and insert suspicious trades into a table in SQLite.

Input MongoDB database to SQLite database specifications:

- mongodb: Database = Equity; Collection = CourseworkTwo
- SQLite: 

  Table = trades_suspects (create if not exist from SQLite client class in modules/db/sqlite_db.py)
  
  TabletoAdd = portfolio_positions (create if not exist from SQLite client class in modules/db/sqlite_db.py)
  
  TabletoGet = equity_prices
  

## Configuration

The configuration file for this script is stored in config/conf.yaml. The configurations can be amended without affecting code logic.

## Script Trigger

In order to trigger this script, the operator needs to run these command lines in their terminal:

```
cd ./Student_22232385/2.CourseworkTwo
python Main.py --date_run 2021-11-11

```