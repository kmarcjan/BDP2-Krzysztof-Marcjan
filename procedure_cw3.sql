DECLARE @YearsAgo INT;  
SET @YearsAgo = 8;
SELECT * FROM DimCurrency INNER JOIN FactCurrencyRate ON DimCurrency.CurrencyKey=FactCurrencyRate.CurrencyKey
WHERE (DimCurrency.CurrencyAlternateKey='GBP' OR  DimCurrency.CurrencyAlternateKey='EUR') AND (DATEADD(Year, -@YearsAgo, GETUTCDATE()) > FactCurrencyRate.Date)
GO