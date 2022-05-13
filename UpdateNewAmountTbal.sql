USE [david_test]
GO

/****** Object:  StoredProcedure [dbo].[UpdateNewAmountTbal]    Script Date: 5/13/2022 3:32:56 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





create procedure [dbo].[UpdateNewAmountTbal] as
SET NOCOUNT ON
begin 
declare @lastonerate decimal(16,4),@lasttworate decimal(16,4),@lasttwomonth date, @lastmonth date, @month date
set @lasttwomonth = convert(date,(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-2, 0)))
set @lastmonth = convert(date,(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)))
set @month  = convert(date,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))
set @lastonerate = (select 人民币汇率 from exchange_rate where 日其 = @lastmonth and 账户 = '林吉特')
set @lasttworate = (select 人民币汇率 from exchange_rate where 日其 = @lasttwomonth and 账户 = '林吉特')
	update t_余额表 set 其末金额 = (
	case 
	when 公司名 = 'We1win' and 其 = @month then 其末金额*@lastonerate 
	when 公司名 = 'We1win' and 其 = @lastmonth then 其末金额*@lasttworate 
	else 其末金额 end),
	
	收入金额 = (
	case 
	when 公司名 = 'We1win' and 其 = @month then 收入金额*@lastonerate 
	when 公司名 = 'We1win' and 其 = @lastmonth then 收入金额*@lasttworate 
	else 收入金额 end),
	
	支出金额 = (
	case 
	when 公司名 = 'We1win' and 其 = @month then 支出金额*@lastonerate 
	when 公司名 = 'We1win' and 其 = @lastmonth then 支出金额*@lasttworate 
	else 支出金额 end),
	
	支出手续费 = (
	case 
	when 公司名 = 'We1win' and 其 = @month then 支出手续费*@lastonerate 
	when 公司名 = 'We1win' and 其 = @lastmonth then 支出手续费*@lasttworate 
	else 支出手续费 end),
	
	其初金额 = (
	case 
	when 公司名 = 'We1win' and 其 = @month then 其初金额*@lastonerate 
	when 公司名 = 'We1win' and 其 = @lastmonth then 其初金额*@lasttworate 
	else 其初金额 end),
	
	收入手续费 = (
	case 
	when 公司名 = 'We1win' and 其 = @month then 收入手续费*@lastonerate 
	when 公司名 = 'We1win' and 其 = @lastmonth then 收入手续费*@lasttworate 
	else 收入手续费 end),
	
	余额差 = (
	case 
	when 公司名 = 'We1win' and 其 = @month then 余额差*@lastonerate 
	when 公司名 = 'We1win' and 其 = @lastmonth then 余额差*@lasttworate 
	else 余额差 end),
	
	后台收入 = (
	case 
	when 公司名 = 'We1win' and 其 = @month then 后台收入*@lastonerate 
	when 公司名 = 'We1win' and 其 = @lastmonth then 后台收入*@lasttworate 
	else 后台收入 end),
	
	调补单 = (
	case 
	when 公司名 = 'We1win' and 其 = @month then 调补单*@lastonerate 
	when 公司名 = 'We1win' and 其 = @lastmonth then 调补单*@lasttworate 
	else 调补单 end)
	end;

GO

