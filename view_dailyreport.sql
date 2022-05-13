USE [david_test]
GO

/****** Object:  View [dbo].[view_日报]    Script Date: 5/13/2022 4:14:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[view_日报] as
select d.项目,d.日其,d.金额 as '原金额',d.盘口名称,d.其,d.新金额 as '金额',c.划分号,c.生活费用办公室号,s.一级科目,s.二级科目,s.三级科目,s.会科编号,c.结帐周其,c.群组编号,c.地区,c.公司,w.*
from t_日报 d
left join co_mapping c
on d.盘口名称 = c.盘口名称
left join subject_mapping s
on d.项目 = s.项目
left join (
SELECT datepart(wk, CONVERT(date, 日其)) as '周数', MIN(CONVERT(date, 日其)) AS '开始', MAX(CONVERT(date, 日其)) AS '结束'
FROM t_日报
GROUP BY datepart(wk, CONVERT(date, 日其))
) w
on datepart(week,convert(date,d.日其)) = w.周数
union all
select p.项目,p.日其,p.金额 as '原金额',p.盘口名称,p.其,p.新金额 as '金额',c.划分号,c.生活费用办公室号,s.一级科目,s.二级科目,s.三级科目,s.会科编号,c.结帐周其,c.群组编号,c.地区,c.公司,w.*
from p_日报 p
left join co_mapping c
on p.盘口名称 = c.盘口名称
left join subject_mapping s
on p.项目 = s.项目 
left join (
SELECT datepart(wk, CONVERT(date, 日其)) as '周数', MIN(CONVERT(date, 日其)) AS '开始', MAX(CONVERT(date, 日其)) AS '结束'
FROM p_日报
GROUP BY datepart(wk, CONVERT(date, 日其))
) w
on datepart(week,convert(date,p.日其)) = w.周数
GO

