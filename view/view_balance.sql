USE [david_test]
GO

/****** Object:  View [dbo].[view_余额表]    Script Date: 5/13/2022 4:15:10 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE view [dbo].[view_余额表] as
SELECT  upper(thd.简称) 简称, 公司名, upper(thd.三方银行名) '三方名/银行名', 表名, 其, 日其 日期, 余额差, 后台收入, upper(tb.摘要) 余额表摘要, 调补单, 
支出手续费, 支出笔数, 支出金额, 收入手续费, 收入笔数, 收入金额, 其初金额, 其末金额, 用途, upper(thd.摘要) 三方摘要,群组名称,三方银行编码,银行层级,
co.划分号,co.盘口编码,co.群组编号,co.结帐时间,co.结帐周其,thd.租用卡银行,co.公司
FROM  t_余额表 tb
left join third_bank thd
on upper(tb.简称) = upper(thd.简称)
and upper(tb.公司名) = upper(thd.盘口名称)
left join third_mapping tm
on upper(thd.三方银行名) = upper(tm.三方银行名)
left join co_mapping co
on upper(tb.公司名) = upper(co.盘口名称)
union all
SELECT  upper(thd.简称) 简称, 公司名, upper(thd.三方银行名) '三方名/银行名', 表名, 其, 日其 日期, 余额差, 后台收入, upper(pb.摘要) 余额表摘要, 调补单, 
支出手续费, 支出笔数, 支出金额, 收入手续费, 收入笔数, 收入金额, 其初金额, 其末金额, 用途, upper(thd.摘要) 三方摘要,群组名称,三方银行编码,银行层级,
co.划分号,co.盘口编码,co.群组编号,co.结帐时间,co.结帐周其,thd.租用卡银行,co.公司
FROM  p_余额表 pb
left join third_bank thd
on upper(pb.简称) = upper(thd.简称)
and upper(pb.公司名) = upper(thd.盘口名称)
left join third_mapping tm
on upper(thd.三方银行名) = upper(tm.三方银行名)
left join co_mapping co
on upper(pb.公司名) = upper(co.盘口名称)
GO

