import pandas as pd
import os
import datetime as dt
from datetime import datetime
from sqlalchemy import create_engine
import json


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


# define the time
def timefunc():
    today = datetime.today()
    month = str(today.year) + '-' + str(today.month)
    lastmonth = today.replace(day=1) - dt.timedelta(days=1)
    last_month = str(lastmonth.year) + '-' + str(lastmonth.month)
    dt_lst = [month, last_month]
    return dt_lst


# find the path of list
def main_lst(path, lst):
    cur_mon = os.listdir(''.join((path, '\\', lst[0])))
    last_mon = os.listdir(''.join((path, '\\', lst[1])))
    diff_co = list(set(last_mon).difference(cur_mon))
    main_lst = [cur_mon, diff_co]
    return main_lst


# truncate table
def truncate_table(tbname, cursor, con):
    cursor.execute("truncate table {tbname}".format(tbname=tbname))
    con.commit()


# put data into sql
def put_data(main_lst, dt_lst, con, path, engine):
    fail = []
    for cos, month in zip(main_lst, dt_lst):
        for co in cos:
            if co[-4:] == 'xlsx' and "~$" not in co:
                try:
                    tb = pd.read_excel(''.join((path, "\\", month, "\\", co)), sheet_name='三方与银行资料', skiprows=[0])
                    tb = tb.loc[:, ~tb.columns.str.contains("^Unnamed")]
                    tb = tb[~tb["简称"].isnull()]
                    tb["盘口名称"] = co.split('.')[0]
                    try:
                        tb["价格"] = tb["价格"].mask(tb["价格"].str.contains('.', na=False), None)
                        tb["序号"] = tb["序号"].mask(tb["序号"].str.contains('.', na=False), None)
                    except:
                        pass
                    tb = tb.applymap(str).replace({"nan": None, "NaT": None, "None": None}, regex=True)
                    tb.columns = ['序号', '类型', '简称', '三方银行名', '户名', '账号', '登入账号', '绑定电话', '开户省', '开户市', '信用等级',
                                  '价格', '购买日其', '状态', '租用卡银行', '经手人', '摘要', '盘口名称']
                    tb.to_sql("third_bank", con=engine, if_exists='append', index=False, chunksize=1000)
                except:
                    fail.append("".join((month, '  ', co)))
                finally:
                    con.close()
    return fail


# main function
def main():
    global db_info
    path = r"Z:\02-帳務"
    db_info = db_info()
    engine, con, cursor = connection(db_info["user"], db_info["password"], 'localhost', '1433', 'testdb')
    dt_lst = timefunc()
    lst = main_lst(path, dt_lst)
    truncate_table('third_bank', cursor, con)
    fail = put_data(lst, dt_lst, con, path, engine)
    print("Done")


# execute main function
if __name__ == "__main__":
    main()
