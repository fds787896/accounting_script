import pandas as pd
import os
import datetime as dt
from datetime import datetime
import concurrent.futures
from sqlalchemy import create_engine
import json
import dateutil.parser as dparser
import re


def db_info():
    with open("config.json", mode="r") as file:
        db_info = json.load(file)
    return db_info


def connection(user, pwd, host, port, db):
    engine = create_engine(
        'mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}?driver=SQL+Server'.format(user=user, pwd=pwd, host=host,
                                                                                  port=port, db=db))
    con = engine.raw_connection()
    cursor = con.cursor()
    return engine, con, cursor


# date
def point():
    today = datetime.today()
    pointdate = (today.replace(day=1) - dt.timedelta(days=1)).replace(day=1) - dt.timedelta(days=1)
    point_date = str(pointdate.year) + '-' + str(pointdate.month)
    return point_date


# input data and preprocess data
def insert_into(co):
    if co[-4:] == "xlsx" and "~$" not in co:
        dic = pd.read_excel(path + "\\" + point_date + "\\" + co,
                            sheet_name=["日报", "充值提现", "收支调整", "费用", "冻结", "借入借出", "借出台湾", "余额表-银行", "余额表-三方", "运营信息"]
                            , skiprows=[0])
        for key, values in dic.items():
            # 报表细錄
            if key == "充值提现" or key == "收支调整" or key == "费用" or key == "冻结" or key == "借入借出" or key == "借出台湾":
                try:
                    values = values.loc[:, ~values.columns.str.contains("^Unnamed")]
                    values = values[pd.to_numeric(values.金额, errors="coerce", downcast="float").notnull()]
                    values['盘口名称'] = co.split('.')[0]
                    values['其'] = dt.datetime.strptime(point_date, "%Y-%m")
                    values['日期'] = values['日期'].astype(str).map(lambda x: x.split(" ")[0])
                    values["日期"] = values["日期"].replace(["NaT", "nan"], [None, None])
                    values = values.rename(columns={"日期": "日其", "三方名/银行名": "三方银行名"})
                    values.to_sql('p_报表细錄', con=engine, if_exists='append', index=False, chunksize=1000)
                except:
                    log_fail.append(co.split('.')[0])
            elif key == "余额表-银行" or key == "余额表-三方":
                try:
                    values = values.loc[:, ~values.columns.str.contains("^Unnamed")]
                    values = values.loc[:, :"摘要"]
                    if key == "余额表-银行":
                        values['表名'] = str("余额表-银行")
                    else:
                        values['表名'] = str("余额表-三方")
                    values = values[~values['日期'].isnull()]
                    values['公司名'] = co.split('.')[0]
                    values['其'] = dt.datetime.strptime(point_date, "%Y-%m")
                    for column in ['余额差', '后台收入', '掉补单', '支出手续费', '支出笔数', '支出金额', '收入手续费', '收入笔数', '收入金额', '期初金额',
                                   '期末金额']:
                        try:
                            values[column] = values[column].mask(values[column].str.contains('.', na=False), None)
                        except:
                            pass
                    values = values.rename(
                        columns={"日期": "日其", "期末金额": "其末金额", "期初金额": "其初金额", "三方名/银行名": "三方银行名", "掉补单": "调补单"})
                    values = values.applymap(str).replace({"nan": None, "NaT": None, "None": None}, regex=True)
                    values.to_sql("p_余额表", con=engine, if_exists='append', index=False, chunksize=1000)
                except:
                    bal_fail.append(co.split('.')[0])
            elif key == "日报":
                try:
                    values = values.loc[:, ~values.columns.str.contains("^Unnamed")]
                    values = values.drop(values.index[values[values["项   目"] == "余额表"].index[0]:])
                    values = values[~values["项   目"].isnull()]
                    sub = pd.concat([values["项   目"]] * len(values.columns[1:]), ignore_index=True).rename('项目')
                    dd = pd.Series(list(values.columns[1:].repeat(len(values['项   目']))), name='日其')
                    amount = pd.Series([j for i in values.columns[1:] for j in values[i]], name='金额')
                    merge = pd.concat([sub, dd, amount], axis=1)
                    merge['盘口名称'] = co.split('.')[0]
                    merge['其'] = dt.datetime.strptime(point_date, "%Y-%m")
                    merge.to_sql("p_日报", con=engine, if_exists="append", index=False, chunksize=1000)
                except:
                    day_fail.append(co.split('.')[0])
            else:
                try:
                    values = values.loc[:, ~values.columns.str.contains("^Unnamed")]
                    values = values[pd.to_numeric(values.金额, errors="coerce", downcast="float").notnull()]
                    values["日期"] = values["日期"].map(
                        lambda x: dparser.parser(x, fuzzy=True) if type(x) == str else x)
                    for column in ["单量", "投注额", "金额"]:
                        values[column] = values[column].map(lambda x: x.replace(",", "") if type(x) == str else x)
                        values[column] = values[column].map(
                            lambda x: re.findall("\d+\.\d+", x)[0] if type(x) == str else x)
                        values[column] = pd.to_numeric(values[column], errors="coerce")
                    values['盘口名称'] = co.split('.')[0]
                    values['其'] = dt.datetime.strptime(point_date, "%Y-%m")
                    values.to_sql('t_OperationInfo', con=engine, if_exists='append', index=False,
                                  chunksize=1000)
                except:
                    operation_fail.append(co.split('.')[0])


def main():
    global path, db_info, engine, con, cursor, point_date, log_fail, bal_fail, day_fail, operation_fail
    path = r"Z:\02-帳務"
    db_info = db_info()
    engine, con, cursor = connection(db_info["user"], db_info["password"], "localhost", 1433, "testdb")
    point_date = point()
    log_fail = []
    bal_fail = []
    day_fail = []
    operation_fail = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(insert_into, os.listdir(path + "\\" + point_date))
    con.close()


main()

if __name__ == "__main__":
    main()
    print("The work is done")
