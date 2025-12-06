-- Q1: Calc Quantity of items, Sales value & Order quantity by each Subcategory in L12M
SELECT FORMAT_DATETIME('%b %Y', a.ModifiedDate) AS month
      ,c.Name
      ,SUM(a.OrderQty) AS qty_item
      ,SUM(a.LineTotal) AS total_sales
      ,COUNT(DISTINCT a.SalesOrderID) AS order_cnt
FROM `adventureworks2019.Sales.SalesOrderDetail` a 
LEFT JOIN `adventureworks2019.Production.Product` b
  ON a.ProductID = b.ProductID
LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c
  ON b.ProductSubcategoryID = CAST(c.ProductSubcategoryID AS STRING)
WHERE DATE(a.ModifiedDate) >= (SELECT DATE_SUB(DATE(MAX(a.ModifiedDate)), INTERVAL 12 MONTH)
                               FROM `adventureworks2019.Sales.SalesOrderDetail`)
GROUP BY 1,2
ORDER BY 2,PARSE_DATE('%b %Y', month);

-- Q2: Calc %YoY growth rate by SubCategory & release top 3 category with highest grow rate
WITH 
cal_qty_item AS (
  SELECT
    EXTRACT(YEAR FROM a.ModifiedDate) AS year
    ,c.Name
    ,SUM(OrderQty) AS qty_item
  FROM adventureworks2019.Sales.SalesOrderDetail a
  JOIN adventureworks2019.Production.Product b
    USING(ProductID)
  JOIN adventureworks2019.Production.ProductSubcategory c
    ON CAST(b.ProductSubcategoryID AS INT64) = c.ProductSubcategoryID
  GROUP BY 1,2
)

,cal_prv_qty AS (
  SELECT
    year 
    ,Name
    ,qty_item
    ,LAG(qty_item) OVER(PARTITION BY Name ORDER BY year) AS prv_qty
  FROM cal_qty_item
)

,cal_qty_diff AS (
  SELECT
    Name
    ,qty_item 
    ,prv_qty
    ,ROUND(qty_item/prv_qty - 1, 2) AS qty_diff
  FROM cal_prv_qty
  WHERE prv_qty IS NOT NULL
)

SELECT
  Name
  ,qty_item 
  ,prv_qty 
  ,qty_diff
FROM (
      SELECT
        Name
        ,qty_item 
        ,prv_qty 
        ,qty_diff
        ,DENSE_RANK() OVER(ORDER BY qty_diff DESC) AS ranking
      FROM cal_qty_diff
      )
WHERE ranking <= 3
ORDER BY 4 DESC;

-- Q3: Ranking Top 3 TeritoryID with biggest Order quantity of every year
WITH cal_order_qty AS (
  SELECT
    EXTRACT(YEAR FROM a.ModifiedDate) AS year
    ,TerritoryID
    ,SUM(OrderQty) AS order_cnt
  FROM adventureworks2019.Sales.SalesOrderDetail a
  JOIN adventureworks2019.Sales.SalesOrderHeader USING(SalesOrderID)
  GROUP BY 1,2
)

SELECT
  year
  ,TerritoryID 
  ,order_cnt
  ,ranking
FROM (
      SELECT
        year 
        ,TerritoryID
        ,order_cnt
        ,DENSE_RANK() OVER(PARTITION BY year ORDER BY order_cnt DESC) AS ranking
      FROM cal_order_qty
      )
WHERE ranking <= 3
ORDER BY 1,4;

-- Q4: Calc Total Discount Cost belongs to Seasonal Discount for each SubCategory
SELECT 
    EXTRACT(YEAR FROM ModifiedDate) AS year
    ,Name
    ,SUM(disc_cost) AS total_cost
FROM (
        SELECT 
          DISTINCT a.ModifiedDate
          ,c.Name
          ,d.DiscountPct, d.Type
          ,a.OrderQty * d.DiscountPct * UnitPrice AS disc_cost 
        FROM `adventureworks2019.Sales.SalesOrderDetail` a
        LEFT JOIN `adventureworks2019.Production.Product` b 
          ON a.ProductID = b.ProductID
        LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c 
          ON CAST(b.ProductSubcategoryID AS INT) = c.ProductSubcategoryID
        LEFT JOIN `adventureworks2019.Sales.SpecialOffer` d 
          ON a.SpecialOfferID = d.SpecialOfferID
        WHERE LOWER(d.Type) LIKE '%seasonal discount%' 
      )
GROUP BY 1,2;

-- Q5: Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
WITH 
info AS (
  SELECT  
    EXTRACT(MONTH FROM ModifiedDate) AS month_no
    ,EXTRACT(YEAR FROM ModifiedDate) AS year_no
    ,CustomerID
    ,COUNT(DISTINCT SalesOrderID) AS order_cnt
  FROM `adventureworks2019.Sales.SalesOrderHeader`
  WHERE EXTRACT(YEAR FROM ModifiedDate) = 2014
    AND Status = 5
  GROUP BY 1,2,3
  ORDER BY 3,1 
),

row_num AS (
  SELECT *
      ,ROW_NUMBER() OVER(PARTITION BY CustomerID ORDER BY month_no) AS row_numb
  FROM info 
), 

first_order AS (
  SELECT *
  FROM row_num
  WHERE row_numb = 1
), 

month_gap AS (
  SELECT 
    a.CustomerID
    ,b.month_no AS month_join
    ,a.month_no AS month_order
    ,a.order_cnt
    ,CONCAT('M - ',a.month_no - b.month_no) AS month_diff
  FROM info a 
  LEFT JOIN first_order b 
    ON a.CustomerID = b.CustomerID
  ORDER BY 1,3
)

SELECT month_join
      ,month_diff 
      ,COUNT(DISTINCT CustomerID) AS customer_cnt
FROM month_gap
GROUP BY 1,2
ORDER BY 1,2;

-- Q6: Trend of Stock level & MoM diff % by all product in 2011
WITH
cal_stock_qty AS (
  SELECT
    Name
    ,EXTRACT(MONTH FROM a.ModifiedDate) AS month
    ,EXTRACT(YEAR FROM a.ModifiedDate) AS year
    ,SUM(StockedQty) AS stock_qty
  FROM adventureworks2019.Production.Product
  LEFT JOIN adventureworks2019.Production.WorkOrder a
    USING(ProductID)
  WHERE EXTRACT(YEAR FROM a.ModifiedDate) = 2011
  GROUP BY 1,2,3
),

cal_stock_prv AS (
  SELECT
    Name
    ,month 
    ,year
    ,stock_qty
    ,LAG(stock_qty) OVER(PARTITION BY Name ORDER BY month) AS stock_prv
  FROM cal_stock_qty
)

SELECT
  Name
  ,month 
  ,year 
  ,stock_qty 
  ,stock_prv 
  ,COALESCE(ROUND(100.0*(stock_qty/stock_prv - 1), 1), 0) AS diff
FROM cal_stock_prv
ORDER BY 1, 2 DESC;

-- Q7: Calc Ratio of Stock/Sales in 2011 by product name, by month
WITH 
sale_info AS (
  SELECT 
    EXTRACT(MONTH FROM a.ModifiedDate) AS mth 
    ,EXTRACT(YEAR FROM a.ModifiedDate) AS yr 
    ,a.ProductId
    ,b.Name
    ,SUM(a.OrderQty) AS sales
  FROM `adventureworks2019.Sales.SalesOrderDetail` a 
  LEFT JOIN `adventureworks2019.Production.Product` b 
    ON a.ProductID = b.ProductID
  WHERE EXTRACT(YEAR FROM a.ModifiedDate) = 2011
  GROUP BY 1,2,3,4
), 

stock_info AS (
  SELECT
    EXTRACT(MONTH FROM ModifiedDate) AS mth 
    ,EXTRACT(YEAR FROM ModifiedDate) AS yr 
    ,ProductId
    ,SUM(StockedQty) AS stock_cnt
  FROM `adventureworks2019.Production.WorkOrder`
  WHERE EXTRACT(YEAR FROM ModifiedDate) = 2011
  GROUP BY 1,2,3
)

SELECT
  a.mth
  ,a.yr
  ,a.ProductId
  ,a.Name
  ,a.sales
  ,b.stock_cnt AS stock
  ,ROUND(COALESCE(b.stock_cnt,0) / sales,2) AS ratio
FROM sale_info a 
FULL JOIN stock_info b 
  ON a.ProductId = b.ProductId
    AND a.mth = b.mth 
    AND a.yr = b.yr
ORDER BY 1 DESC, 7 DESC;

-- Q8: No of order and value at Pending status in 2014
SELECT
  EXTRACT(YEAR FROM ModifiedDate) AS year
  ,Status
  ,COUNT(DISTINCT PurchaseOrderID) AS order_cnt
  ,SUM(TotalDue) AS value 
FROM adventureworks2019.Purchasing.PurchaseOrderHeader
WHERE EXTRACT(YEAR FROM ModifiedDate) = 2014
  AND Status = 1
GROUP BY 1,2;