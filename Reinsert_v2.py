import pandas as pd
import os
import datetime as dt
from sqlalchemy import create_engine
import json
from dateutil.relativedelta import relativedelta
import numpy as np
import re
import dateutil.parser as dparser
pd.options.mode.chained_assignment = None


def DB_Account():
    with open("config.json", mode="r") as file:
        db_info = json.load(file)
    return db_info


    engine = create_engine(
        'mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}?driver=SQL+Server'.format(user=user, pwd=pwd, host=host,
                                                                                  port=port, db=db))
    con = engine.raw_connection()
    cursor = con.cursor()
    return engine, con, cursor
