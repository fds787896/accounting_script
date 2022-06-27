import pandas as pd
import os
import datetime as dt
from sqlalchemy import create_engine
import json
from multiprocessing import cpu_count
import concurrent.futures
import time

pd.options.mode.chained_assignment = None


class DBConnection:
    @staticmethod
    def DBInfo():
        with open("config.json", mode="r") as file:
            info = json.load(file)
        return info

    def __init__(self, path, month):
        self.host = "localhost"
        self.port = 1433
        self.user = self.DBInfo()["user"]
        self.password = self.DBInfo()["password"]
        self.database = "testdb"
        self.path = path
        self.month = month

    def CreateConnection(self):
        engine = create_engine(
            'mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=SQL+Server'.format(user=self.user,
                                                                                                 password=self.password,
                                                                                                 host=self.host,
                                                                                                 port=self.port,
                                                                                                 database=self.database))
        con = engine.raw_connection()
        cursor = con.cursor()
        return engine, con, cursor


class DeleteTable(DBConnection):

    @staticmethod
    def DateList(start, end):
        List = pd.date_range(start, end, freq='MS').strftime("%Y-%m").tolist()
        List = [element.replace("-0", "-") for element in List]
        return List

    def DeleteData(self, start, end):
        engine, con, cursor = self.CreateConnection()
        for table in ["p_余额表", "p_日报", "p_报表细錄"]:
            for month in self.DateList(start, end):
                cursor.execute(
                    "delete from {table} where 其 = '{month}-1'".format(table=table, month=month))
                con.commit()


class Insert_to_SQL(DBConnection):

    @staticmethod
    def DateList(start, end):
        List = pd.date_range(start, end, freq='MS').strftime("%Y-%m").tolist()
        List = [element.replace("-0", "-") for element in List]
        return List

    def DailytoSQL(self, file):
        if file[-4:] == "xlsx" and "~$" not in file:
            try:
                df = pd.read_excel("".join((self.path, "\\", self.month, "\\", file))
                                   , sheet_name="日报", skiprows=[0])
                df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
                df = df.drop(df.index[df[df["项   目"] == "余额表"].index[0]:])
                df = df[~df["项   目"].isnull()]
                sub = pd.concat([df["项   目"]] * len(df.columns[1:]), ignore_index=True).rename(
                    '项目')
                dd = pd.Series(list(df.columns[1:].repeat(len(df['项   目']))), name='日其')
                amount = pd.Series([j for i in df.columns[1:] for j in df[i]], name='金额')
                merge = pd.concat([sub, dd, amount], axis=1)
                merge['盘口名称'] = file.split('.')[0]
                merge['其'] = dt.datetime.strptime(self.month, "%Y-%m")
                merge.to_sql("p_日报", con=self.CreateConnection()[0], if_exists="append", index=False,
                             chunksize=1000)
            except Exception as ex:
                print("".join((self.month, "-", file)))
                print(ex)

    def LogtoSQL(self, file):
        if file[-4:] == "xlsx" and "~$" not in file:
            try:
                dic = pd.read_excel("".join((self.path, "\\", self.month, "\\", file))
                                    , sheet_name=["充值提现", "收支调整", "费用", "冻结", "借入借出", "借出台湾"]
                                    , skiprows=[0])
                for key, df in dic.items():
                    if key == "充值提现" or key == "收支调整" or key == "费用" or key == "冻结" or key == "借入借出" or key == "借出台湾":
                        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
                        df = df[pd.to_numeric(df.金额, errors="coerce", downcast="float").notnull()]
                        df['盘口名称'] = file.split('.')[0]
                        df['其'] = dt.datetime.strptime(self.month, "%Y-%m")
                        df = df.rename(columns={"日期": "日其", "三方名/银行名": "三方银行名"})
                        df.to_sql("p_报表细錄", con=self.CreateConnection()[0], if_exists='append', index=False,
                                  chunksize=1000)
            except Exception as ex:
                print("".join((self.month, "-", file)))
                print(ex)

    def BalancetoSQL(self, file):
        if file[-4:] == "xlsx" and "~$" not in file:
            try:
                dic = pd.read_excel("".join((self.path, "\\", self.month, "\\", file))
                                    , sheet_name=["余额表-银行", "余额表-三方"]
                                    , skiprows=[0])
                for key, df in dic.items():
                    if key == "余额表-银行" or key == "余额表-三方":
                        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
                        df = df.loc[:, :"摘要"]
                        if key == "余额表-银行":
                            df['表名'] = str("余额表-银行")
                        else:
                            df['表名'] = str("余额表-三方")
                        df = df[~df['日期'].isnull()]
                        df['公司名'] = file.split('.')[0]
                        df['其'] = dt.datetime.strptime(self.month, "%Y-%m")
                        for column in ['余额差', '后台收入', '掉补单', '支出手续费', '支出笔数', '支出金额', '收入手续费', '收入笔数', '收入金额',
                                       '期初金额', '期末金额']:
                            try:
                                df[column] = df[column].mask(df[column].str.contains('.', na=False), None)
                            except (Exception,):
                                pass
                        df = df.rename(columns={"日期": "日其", "期末金额": "其末金额", "期初金额": "其初金额", "三方名/银行名": "三方银行名",
                                                "掉补单": "调补单"})
                        df.to_sql("p_余额表", con=self.CreateConnection()[0], if_exists='append', index=False,
                                  chunksize=1000)
            except Exception as ex:
                print("".join((self.month, "-", file)))
                print(ex)

    def UpdateNewAmount(self, ratelist, updatelist):
        engine, con, cursor = self.CreateConnection()
        procList = ["ReputNewAmountP", "ReputNewAmountPlog", "ReputNewAmountPbal"]
        for procedure in procList:
            for ratemonth, updatemonth in zip(ratelist, updatelist):
                cursor.execute(
                    "EXEC {procedure} '{ratemonth}-1','{month}-1'".format(procedure=procedure, ratemonth=ratemonth,
                                                                          month=updatemonth))
        con.commit()


def main():
    path = r"Z:\02-帳務"
    month = "2020-6"
    deleteObj = DeleteTable(path, month)
    # deleteObj.DeleteData("2020-6", "2020-6")
    executelist = deleteObj.DateList("2020-6", "2020-6")
    for insertmonth in executelist:
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count() * 5) as executor:
            executor.map(Insert_to_SQL(path, insertmonth).DailytoSQL, os.listdir("".join((path, "\\", insertmonth))))
            executor.map(Insert_to_SQL(path, insertmonth).LogtoSQL, os.listdir("".join((path, "\\", insertmonth))))
            executor.map(Insert_to_SQL(path, insertmonth).BalancetoSQL, os.listdir("".join((path, "\\", insertmonth))))
    ratelist = deleteObj.DateList("2020-5", "2020-5")
    Insert_to_SQL(path, month).UpdateNewAmount(ratelist, executelist)


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))