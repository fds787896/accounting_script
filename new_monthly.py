import pandas as pd
import os
import datetime as dt
from datetime import datetime
import concurrent.futures
from sqlalchemy import create_engine
import json
import dateutil.parser as dparser
import re
import numpy as np


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
    # 费用处理(分割column)
    def SplitColumn(df, outputColumn, number):
        for index, value in enumerate(df["临时摘要"]):
            try:
                df[outputColumn][index] = value[number]
            except:
                df[outputColumn][index] = np.nan

    if co[-4:] == "xlsx" and "~$" not in co:
        dic = pd.read_excel(path + "\\" + point_date + "\\" + co,
                            sheet_name=["日报", "充值提现", "收支调整", "费用", "冻结", "借入借出", "借出台湾", "余额表-银行", "余额表-三方", "运营信息"]
                            , skiprows=[0])
        for key, values in dic.items():
            # 报表细錄
            if key == "充值提现" or key == "收支调整" or key == "冻结" or key == "借入借出" or key == "借出台湾":
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
            elif key == "费用":
                try:
                    # the cost df before "2022-03-09"
                    oldDf = values[values["日期"] < "2022-03-09"]
                    oldDf = oldDf.loc[:, ~oldDf.columns.str.contains("^Unnamed")]
                    oldDf = oldDf[pd.to_numeric(oldDf.金额, errors="coerce", downcast="float").notnull()]
                    # the cost df after  "2022-03-09"
                    newDf = values[values["日期"] >= "2022-03-09"]
                    newDf = newDf.loc[:, ~newDf.columns.str.contains("^Unnamed")]
                    newDf = newDf[pd.to_numeric(newDf.金额, errors="coerce", downcast="float").notnull()]
                    newDf["摘要"] = newDf["摘要"].map(lambda x: x.upper() if type(x) == str else x)
                    dfCost = newDf[~newDf["三级科目"].str.contains("系统佣金|被盗刷|保证金")]
                    dfCost["临时摘要"] = dfCost["摘要"].map(lambda x: x.split("/"))
                    dfCost[["四级科目", "数量", "单价", "期间", "对象", "备注", "预付", "套餐"]] = np.nan
                    dfCost = dfCost.reset_index(drop=True)
                    for number, column in enumerate(["四级科目", "数量", "单价", "期间", "对象", "备注"]):
                        SplitColumn(dfCost, column, number)
                    dfCost["预付"] = ["Y" if type(value) == str and "预付" in value else np.nan for value in
                                    dfCost["备注"]]
                    dfCost["套餐"] = ["Y" if type(value) == str and "套餐" in value else np.nan for value in
                                    dfCost["备注"]]
                    dfCost["外汇"] = [value.split("*") if type(value) == str and "*" in value else np.nan
                                    for value in dfCost["备注"]]
                    dfCost["币别"] = dfCost["外汇"].map(
                        lambda x: "".join(re.findall(r'[^(\d+(?:\.\d+)?)]', x[1])) if type(
                            x) == list else np.nan)
                    dfCost["外币金额"] = dfCost["外汇"].map(
                        lambda x: re.findall(r'(\d+(?:\.\d+)?)', x[1])[0] if type(
                            x) == list else np.nan)
                    dfCost["汇率"] = dfCost["外汇"].map(
                        lambda x: re.findall(r'(\d+(?:\.\d+)?)', x[2])[0] if type(
                            x) == list else np.nan)
                    ###
                    dfOther = newDf[newDf["三级科目"].str.contains("系统佣金|被盗刷|保证金")]
                    dfOther["临时摘要"] = dfOther["摘要"].map(lambda x: x.split("/"))
                    dfOther = dfOther.reset_index(drop=True)
                    dfOther[["四级科目", "对象", "备注"]] = np.nan
                    for number, column in enumerate(["四级科目", "对象", "备注"]):
                        SplitColumn(dfOther, column, number)
                    ###
                    df = pd.concat([dfCost, dfOther, oldDf], axis=0)
                    df = df.drop(["临时摘要", "外汇"], axis=1)
                    for column in ["数量", "单价", "期间", "外币金额", "汇率"]:
                        df[column] = df[column].map(lambda x: x.replace(",", "") if type(x) == str else x)
                        df[column] = pd.to_numeric(df[column], errors="coerce")
                    df['盘口名称'] = co.split('.')[0]
                    df['其'] = dt.datetime.strptime(point_date, "%Y-%m")
                    df = df.rename(columns={"日期": "日其", "三方名/银行名": "三方银行名"})
                    df.to_sql('p_报表细錄', con=engine, if_exists='append', index=False, chunksize=1000)
                except Exception as ex:
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
                    values = values.drop(values.index[values[values["项   目"] == "九、可分配现金"].index[0]+1:values[values["项   目"] == "坏帐"].index[0]])
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
                    values.to_sql('p_OperationInfo', con=engine, if_exists='append', index=False,
                                  chunksize=1000)
                except:
                    operation_fail.append(co.split('.')[0])


def RunProcedure():
    lst = ["UpdateNewAmountP", "UpdateNewAmountPlog", "UpdateNewAmountPbal", "UpdateNewAmountPop"]
    for procedure in lst:
        cursor.execute("EXEC {procedure}".format(procedure=procedure))
    con.commit()


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
    RunProcedure()
    con.close()


main()

if __name__ == "__main__":
    main()
    print("The work is done")
