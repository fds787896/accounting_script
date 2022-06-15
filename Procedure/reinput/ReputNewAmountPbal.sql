USE [david_test]
GO

/****** Object:  StoredProcedure [dbo].[ReputNewAmountPbal]    Script Date: 6/15/2022 11:48:28 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


ALTER procedure [dbo].[ReputNewAmountPbal]
@ratemonth date,
@month date
as
set nocount on 
begin 
declare @rate decimal(16,4)
set @rate = (select 人民币汇率 from exchange_rate where 账户 = '林吉特' and 日其 = @ratemonth)
update p_余额表 set
其末金额 = 其末金额 * @rate,
收入金额 = 收入金额 * @rate,
支出金额 = 支出金额 * @rate,
支出手续费 = 支出手续费 * @rate,
其初金额 = 其初金额 * @rate,
收入手续费 = 收入手续费 * @rate,
余额差 = 余额差 * @rate,
后台收入 = 后台收入 * @rate,
调补单 = 调补单 * @rate
where 公司名 = 'We1win'
and 其 = @month
end;
GO

