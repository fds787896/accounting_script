USE [david_test]
GO

/****** Object:  View [dbo].[view_报表细錄]    Script Date: 5/13/2022 4:14:32 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[view_报表细錄]
as 
select 一级科目,二级科目,三级科目,UPPER(四级科目) '四级科目',日其,其,金额 as '原金额',tg.盘口名称,充提笔数,upper(tg.简称) 简称,upper(thd.三方银行名) 三方银行名,upper(tg.摘要) 细錄摘要,tg.经手人,tg.类型,本方卡号,对方卡号,对象,
upper(thd.摘要) 三方摘要,群组名称,三方银行编码,银行层级,co.划分号,co.结帐时间,co.结帐周其,co.盘口编码,co.群组编号,tg.序号,tg.充提人数,tg.数量,tg.单价,tg.期间,tg.备注,tg.预付,tg.套餐,tg.币别,tg.外币金额,tg.汇率,
tg.新金额 as '金额',co.公司
from t_报表细錄 tg
left join third_bank thd
on upper(tg.简称) = upper(thd.简称)
and upper(tg.盘口名称) = upper(thd.盘口名称)
left join third_mapping tm
on upper(thd.三方银行名) = upper(tm.三方银行名)
left join co_mapping co
on upper(tg.盘口名称) = upper(co.盘口名称)
union all
select 一级科目,二级科目,三级科目,UPPER(四级科目) '四级科目',日其,其,金额 as '原金额',pg.盘口名称,充提笔数,upper(pg.简称) 简称,upper(thd.三方银行名) 三方银行名,upper(pg.摘要) 细錄摘要,pg.经手人,pg.类型,本方卡号,对方卡号,对象,
upper(thd.摘要) 三方摘要,群组名称,三方银行编码,银行层级,co.划分号,co.结帐时间,co.结帐周其,co.盘口编码,co.群组编号,pg.序号,pg.充提人数,pg.数量,pg.单价,pg.期间,pg.备注,pg.预付,pg.套餐,pg.币别,pg.外币金额,pg.汇率,
pg.新金额 as '金额',co.公司
from p_报表细錄 pg
left join third_bank thd
on upper(pg.简称) = upper(thd.简称)
and upper(pg.盘口名称) = upper(thd.盘口名称)
left join third_mapping tm
on upper(thd.三方银行名) = upper(tm.三方银行名)
left join co_mapping co
on upper(pg.盘口名称) = upper(co.盘口名称)
GO

