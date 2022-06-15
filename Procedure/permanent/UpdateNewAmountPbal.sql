USE [david_test]
GO

/****** Object:  StoredProcedure [dbo].[UpdateNewAmountPbal]    Script Date: 6/15/2022 12:23:36 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[UpdateNewAmountPbal] as 
set nocount on 
begin
declare @lasttwomonth date,@lastthreemonth date,@lastthreerate decimal(16,4)
set @lasttwomonth = convert(date,(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-2, 0)))
set @lastthreemonth = convert(date,(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-3, 0)))
set @lastthreerate = (select 人民币汇率 from exchange_rate where 日其 = @lastthreemonth and 账户 = '林吉特')
	update p_余额表 set 
	其末金额 = 其末金额*@lastthreerate,
	收入金额 = 收入金额*@lastthreerate,
	支出金额 = 支出金额*@lastthreerate,
	支出手续费 = 支出手续费*@lastthreerate,
	其初金额 = 其初金额*@lastthreerate,
	收入手续费 = 收入手续费*@lastthreerate,
	余额差 = 余额差*@lastthreerate,
	后台收入 = 后台收入*@lastthreerate,
	调补单 = 调补单*@lastthreerate
	where 公司名 = 'We1win'
	and 其 = @lasttwomonth
	end;
GO

