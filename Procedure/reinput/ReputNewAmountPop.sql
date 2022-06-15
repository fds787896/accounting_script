USE [david_test]
GO

/****** Object:  StoredProcedure [dbo].[ReputNewAmountPop]    Script Date: 6/15/2022 11:49:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[ReputNewAmountPop]
@ratemonth date,
@month date
as
set nocount on 
begin 
declare @rate decimal(16,4)
set @rate = (select 人民币汇率 from exchange_rate where 账户 = '林吉特' and 日其 = @ratemonth)
update p_OperationInfo set 新金额 = (
case
when 盘口名称 = 'We1win' and 其 = @month then 金额*@rate
when 盘口名称 != 'We1win' and 其 = @month then 金额*1
else 新金额
end),
新投注额 = (
case 
when 盘口名称 = 'We1win' and 其 = @month then 投注额*@rate
when 盘口名称 != 'We1win' and 其 = @month then 投注额*1
else 新投注额
end)
end;
GO

