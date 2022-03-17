import pandas as pd
import os
import datetime as dt
from sqlalchemy import create_engine
import json
import requests
import time
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


def month_lst():
    today = dt.datetime.today()
    first = today.replace(day=1)
    month = "".join((str(first.year), "-", str(first.month)))
    last = first - dt.timedelta(days=1)
    last_month = "".join((str(last.year), "-", str(last.month)))
    return [month, last_month]


def point_day():
    point = dt.datetime.today() - dt.timedelta(days=2)
    point = "".join((str(point.year), "/", str(point.month), "/", str(point.day)))
    yesterday = dt.datetime.today() - dt.timedelta(days=3)
    yesterday = "".join((str(yesterday.year), "/", str(yesterday.month), "/", str(yesterday.day)))
    return [point, yesterday]


def truncate_table(tb_name):
    try:
        cursor.execute("truncate table {}".format(tb_name))
        con.commit()
    except Exception as ex:
        print(ex)


def telegram_bot_sendtext(bot_message):
    bot_token = "1912198854:AAE_1GcC2lex037C5X2vQ-FIoqfN9SMc4dw"
    bot_chatID = "-569751154"
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    requests.get(send_text)


def insert_into_sql():
    def third_bank_into_sql(df, file):
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        df = df[~df["简称"].isnull()]
        df["账号"] = df["账号"].map(lambda x: x.strip(" \t\n\r") if type(x) == str else x)
        df["绑定电话"] = df["绑定电话"].map(lambda x: x.strip(" \t\n\r") if type(x) == str else x)
        df["盘口名称"] = file.split('.')[0]
        try:
            df["价格"] = df["价格"].mask(df["价格"].str.contains('.', na=False), None)
            df["序号"] = df["序号"].mask(df["序号"].str.contains('.', na=False), None)
        except:
            pass
        df = df.applymap(str).replace({"nan": None, "NaT": None, "None": None}, regex=True)
        df.columns = ['序号', '类型', '简称', '三方银行名', '户名', '账号', '登入账号', '绑定电话', '开户省', '开户市',
                      '信用等级',
                      '价格', '购买日其', '状态', '租用卡银行', '经手人', '摘要', '盘口名称']
        df["购买日其"] = df["购买日其"].map(lambda x: dparser.parse(x, fuzzy=True) if type(x) == str else x)
        df.to_sql("third_bank", con=engine, if_exists='append', index=False, chunksize=1000)

    def SplitColumn(df, outputColumn, number):
        for index, value in enumerate(df["临时摘要"]):
            try:
                df[outputColumn][index] = value[number]
            except:
                df[outputColumn][index] = np.nan

    for month in month_lst():
        for file in os.listdir(r"Z:\02-帳務\{month}".format(month=month)):
            if file[-4:] == "xlsx" and "~$" not in file:
                try:
                    dic = pd.read_excel(r"Z:\02-帳務\{month}\{file}".format(month=month, file=file)
                                        , sheet_name=["日报", "充值提现", "收支调整", "费用", "冻结", "借入借出", "借出台湾", "余额表-银行",
                                                      "余额表-三方", "三方与银行资料", "运营信息"],
                                        skiprows=[0], dtype={"账号": str, "绑定电话": str})
                    for key, df in dic.items():
                        if key == "充值提现" or key == "收支调整" or key == "冻结" or key == "借入借出" or key == "借出台湾":
                            try:
                                df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
                                df = df[pd.to_numeric(df.金额, errors="coerce", downcast="float").notnull()]
                                df['盘口名称'] = file.split('.')[0]
                                df['其'] = dt.datetime.strptime(month, "%Y-%m")
                                df = df.rename(columns={"日期": "日其", "三方名/银行名": "三方银行名"})
                                df.to_sql('t_报表细錄', con=engine, if_exists='append', index=False, chunksize=1000)
                            except Exception as ex:
                                telegram_bot_sendtext("".join((file, key, month)))
                                telegram_bot_sendtext(str(ex))
                        elif key == "费用":
                            try:
                                #the cost df before "2022-03-09"
                                oldDf = df[df["日期"] < "2022-03-09"]
                                oldDf = oldDf.loc[:, ~oldDf.columns.str.contains("^Unnamed")]
                                oldDf = oldDf[pd.to_numeric(oldDf.金额, errors="coerce", downcast="float").notnull()]
                                #the cost df after  "2022-03-09"
                                newDf = df[df["日期"] >= "2022-03-09"]
                                newDf = newDf.loc[:, ~newDf.columns.str.contains("^Unnamed")]
                                newDf = newDf[pd.to_numeric(newDf.金额, errors="coerce", downcast="float").notnull()]
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
                                    df[column] = pd.to_numeric(df[column], errors="coerce")
                                df['盘口名称'] = file.split('.')[0]
                                df['其'] = dt.datetime.strptime(month, "%Y-%m")
                                df = df.rename(columns={"日期": "日其", "三方名/银行名": "三方银行名"})
                                df.to_sql('t_报表细錄', con=engine, if_exists='append', index=False, chunksize=1000)
                            except Exception as ex:
                                telegram_bot_sendtext("".join((file, key, month)))
                                telegram_bot_sendtext(str(ex))
                        elif key == "余额表-银行" or key == "余额表-三方":
                            try:
                                df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
                                df = df.loc[:, :"摘要"]
                                if key == "余额表-银行":
                                    df['表名'] = str("余额表-银行")
                                else:
                                    df['表名'] = str("余额表-三方")
                                df = df[~df['日期'].isnull()]
                                df['公司名'] = file.split('.')[0]
                                df['其'] = dt.datetime.strptime(month, "%Y-%m")
                                for column in ['余额差', '后台收入', '掉补单', '支出手续费', '支出笔数', '支出金额', '收入手续费', '收入笔数', '收入金额',
                                               '期初金额', '期末金额']:
                                    try:
                                        df[column] = df[column].mask(df[column].str.contains('.', na=False), None)
                                    except:
                                        pass
                                df = df.rename(
                                    columns={"日期": "日其", "期末金额": "其末金额", "期初金额": "其初金额", "三方名/银行名": "三方银行名",
                                             "掉补单": "调补单"})
                                df.to_sql("t_余额表", con=engine, if_exists='append', index=False, chunksize=1000)
                            except Exception as ex:
                                telegram_bot_sendtext("".join((file, key, month)))
                                telegram_bot_sendtext(str(ex))
                        elif key == "日报":
                            try:
                                df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
                                df = df.drop(df.index[df[df["项   目"] == "余额表"].index[0]:])
                                for day in df.columns:
                                    if day == point_day()[0]:
                                        df = df.loc[:, :day]
                                    else:
                                        df = df
                                df = df[~df["项   目"].isnull()]
                                sub = pd.concat([df["项   目"]] * len(df.columns[1:]), ignore_index=True).rename('项目')
                                dd = pd.Series(list(df.columns[1:].repeat(len(df['项   目']))), name='日其')
                                amount = pd.Series([j for i in df.columns[1:] for j in df[i]], name='金额')
                                merge = pd.concat([sub, dd, amount], axis=1)
                                merge['盘口名称'] = file.split('.')[0]
                                merge['其'] = dt.datetime.strptime(month, "%Y-%m")
                                try:
                                    if "".join((str(merge["其"][0].year), "-", str(merge["其"][0].month))) == "-".join(
                                            point_day()[0].replace("/", "-").split("-")[0:2]) and \
                                            merge["金额"][merge["日其"] == point_day()[0]][merge["项目"] == "三、利润"].values[
                                                0] == 0:
                                        telegram_bot_sendtext("".join((file, month, "未到帐")))
                                except:
                                    pass
                                merge.to_sql("t_日报", con=engine, if_exists='append', index=False, chunksize=1000)
                            except Exception as ex:
                                telegram_bot_sendtext("".join((file, key, month)))
                                telegram_bot_sendtext(str(ex))
                        elif key == "三方与银行资料":
                            if month == currentMonth:
                                try:
                                    third_bank_into_sql(df, file)
                                except Exception as ex:
                                    telegram_bot_sendtext("".join((file, key, month)))
                                    telegram_bot_sendtext(str(ex))
                            else:
                                if file in os.listdir(r"Z:\02-帳務\{currentMonth}".format(currentMonth=currentMonth)):
                                    pass
                                else:
                                    try:
                                        third_bank_into_sql(df, file)
                                    except Exception as ex:
                                        telegram_bot_sendtext("".join((file, key, month)))
                                        telegram_bot_sendtext(str(ex))
                        else:
                            try:
                                df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
                                df = df[pd.to_numeric(df.金额, errors="coerce", downcast="float").notnull()]
                                df["日期"] = df["日期"].map(
                                    lambda x: dparser.parser(x, fuzzy=True) if type(x) == str else x)
                                for column in ["单量", "投注额", "金额"]:
                                    df[column] = df[column].map(lambda x: x.replace(",", "") if type(x) == str else x)
                                    df[column] = df[column].map(
                                        lambda x: re.findall("\d+\.\d+", x)[0] if type(x) == str else x)
                                    df[column] = pd.to_numeric(df[column], errors="coerce")
                                df['盘口名称'] = file.split('.')[0]
                                df['其'] = dt.datetime.strptime(month, "%Y-%m")
                                df.to_sql('t_OperationInfo', con=engine, if_exists='append', index=False,
                                          chunksize=1000)
                            except Exception as ex:
                                telegram_bot_sendtext(file)
                                telegram_bot_sendtext(str(ex))
                except Exception as ex:
                    telegram_bot_sendtext("".join((file, month)))
                    telegram_bot_sendtext(str(ex))


def main():
    global db_info, engine, con, cursor, month_lst, currentMonth
    db_info = db_info()
    engine, con, cursor = connection(db_info["user"], db_info["password"], "localhost", 1433, "testdb")
    currentMonth = "{year}-{month}".format(year=dt.datetime.today().year, month=dt.datetime.today().month)
    for tablename in ["t_报表细錄", "t_余额表", "t_日报", "third_bank", "t_OperationInfo"]:
        truncate_table(tablename)
    insert_into_sql()
    # InputOperationData(month_lst)
    con.close()
    telegram_bot_sendtext("Done")


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))