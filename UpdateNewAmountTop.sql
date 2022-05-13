USE [david_test]
GO

/****** Object:  StoredProcedure [dbo].[UpdateNewAmountTop]    Script Date: 5/13/2022 3:33:35 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


ALTER procedure [dbo].[UpdateNewAmountTop]
as 
set nocount on
begin
declare @lastonerate decimal(16,4),@lasttworate decimal(16,4),@lasttwomonth date, @lastmonth date, @month date
set @lasttwomonth = convert(date,(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-2, 0)))
set @lastmonth = convert(date,(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)))
set @month  = convert(date,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))
set @lastonerate = (select 人民币汇率 from exchange_rate where 日其 = @lastmonth and 账户 = '林吉特')
set @lasttworate = (select 人民币汇率 from exchange_rate where 日其 = @lasttwomonth and 账户 = '林吉特')
	update t_OperationInfo set 
	新金额 = (
	case
	when 盘口名称 = 'We1win' and 其 = @month then 金额*@lastonerate
	when 盘口名称 = 'We1win' and 其 = @lastmonth then 金额*@lasttworate
	else 金额 end),
	
	新投注额 = (
	case 
	when 盘口名称 = 'We1win' and 其 = @month then 投注额*@lastonerate
	when 盘口名称 = 'We1win' and 其 = @lastmonth then 投注额*@lasttworate 
	else 投注额 end)
end;
GO

