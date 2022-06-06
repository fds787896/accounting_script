USE [testdb]
GO

/****** Object:  StoredProcedure [dbo].[UpdateNewAmountP]    Script Date: 6/6/2022 12:28:47 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


ALTER procedure [dbo].[UpdateNewAmountP]
as 
SET NOCOUNT ON
begin 
declare @lasttwomonth date,@lastthreemonth date,@lastthreerate decimal(16,4)
set @lasttwomonth = convert(date,(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-2, 0)))
set @lastthreemonth = convert(date,(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-3, 0)))
set @lastthreerate = (select 人民币汇率 from exchange_rate where 日其 = @lastthreemonth and 账户 = '林吉特')
update p_日报 set 新金额 = 金额*@lastthreerate
where 盘口名称 = 'We1win'
and 其 = @lasttwomonth
end ;
GO

