import pandas as pd
import os
import datetime as dt
from sqlalchemy import create_engine
import json
from dateutil.relativedelta import relativedelta


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


def point_month_list(start_month, end_month):
    today = dt.datetime.today()
    first = today.replace(day=1)
    month_list = [first - relativedelta(month=month) for month in range(start_month, end_month + 1)]
    month_list_str = ["".join((str(month_str.year), "-", str(month_str.month))) for month_str in month_list]
    return month_list_str


def delete_point_date_data(point_list):
    delete_table_list = ["p_余额表", "p_日报", "p_报表细錄"]
    for delete_table in delete_table_list:
        for point_month in point_list:
            cursor.execute("delete from {table} where 其 = '{month}-1'".format(table=delete_table,month=point_month))
            con.commit()


def insert_data_to_sql(point_list):
    for month in point_list:
        for file in os.listdir(r"Z:\02-帳務\{month}".format(month=month)):
            if file[-4:] == "xlsx" and "~$" not in file:
                try:
                    dic = pd.read_excel(r"Z:\02-帳務\{month}\{file}".format(month=month, file=file)
                                        , sheet_name=["日报", "充值提现", "收支调整", "费用", "冻结", "借入借出", "借出台湾", "余额表-银行",
                                                      "余额表-三方"],
                                        skiprows=[0])
                    for key, df in dic.items():
                        if key == "充值提现" or key == "收支调整" or key == "费用" or key == "冻结" or key == "借入借出" or key == "借出台湾":
                            try:
                                df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
                                df = df[pd.to_numeric(df.金额, errors="coerce", downcast="float").notnull()]
                                df['盘口名称'] = file.split('.')[0]
                                df['其'] = dt.datetime.strptime(month, "%Y-%m")
                                df = df.rename(columns={"日期": "日其", "三方名/银行名": "三方银行名"})
                                df.to_sql('p_报表细錄', con=engine, if_exists='append', index=False, chunksize=1000)
                            except Exception as ex:
                                print(ex)
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
                                df.to_sql("p_余额表", con=engine, if_exists='append', index=False, chunksize=1000)
                            except Exception as ex:
                                print(ex)
                        else:
                            try:
                                df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
                                df = df.drop(df.index[df[df["项   目"] == "余额表"].index[0]:])
                                df = df[~df["项   目"].isnull()]
                                sub = pd.concat([df["项   目"]] * len(df.columns[1:]), ignore_index=True).rename(
                                    '项目')
                                dd = pd.Series(list(df.columns[1:].repeat(len(df['项   目']))), name='日其')
                                amount = pd.Series([j for i in df.columns[1:] for j in df[i]], name='金额')
                                merge = pd.concat([sub, dd, amount], axis=1)
                                merge['盘口名称'] = file.split('.')[0]
                                merge['其'] = dt.datetime.strptime(month, "%Y-%m")
                                merge.to_sql("p_日报", con=engine, if_exists="append", index=False, chunksize=1000)
                            except Exception as ex:
                                print(ex)
                except Exception as ex:
                    print(ex)
                finally:
                    con.close()


def main():
    global db_info, engine, con, cursor, month_list_str
    db_info=db_info()
    engine, con, cursor = connection(db_info["user"], db_info["password"], "localhost", 1433, "testdb")
    month_list_str=point_month_list(4,4)
    delete_point_date_data(point_list=month_list_str)
    insert_data_to_sql(month_list_str)


if __name__ == "__main__":
    main()