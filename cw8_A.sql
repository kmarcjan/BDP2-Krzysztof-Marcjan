SELECT [OrderDate], SUM([OrderQuantity]) AS Orders_cnt 
FROM [dbo].[FactInternetSales]
GROUP BY [OrderDate]
HAVING COUNT([OrderQuantity]) < 100
ORDER BY Orders_cnt DESC