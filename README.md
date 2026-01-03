# SQL | Bicycle Manufacturer Analysis

## **Table of Contents**
- Introduction
- SQL Queries & Results
- Insights & Recommendations

---

## **Introduction**
### Objectives
        
  The project is designed to simulate a real analytical workflow in a manufacturing and sales environment. The main objectives include:
        
  - Building practical SQL queries that mirror real business questions, such as revenue trends, customer purchasing behavior, and time-based comparisons.
  - Demonstrating how to transform raw transactional data into structured analytical outputs using date functions, window functions, and subqueries.
  - Enhancing analytical thinking by breaking down business problems into smaller SQL components that can be validated and reused.
  - Creating a portfolio-ready SQL project that showcases both technical skills and the ability to communicate analysis clearly.

### Dataset Description

This project uses selected tables from the **AdventureWorks2019** database. The analytical model is based on a simple star-schema structure including fact and dimension tables.

Fact Tables

  - Sales.SalesOrderDetail – Line-level sales transactions.
  - Sales.SalesOrderHeader – Order-level sales information.
  - Sales.SpecialOffer – Discount and promotional details.
  - Production.WorkOrder – Manufacturing work orders and stock movement.
  - Purchasing.PurchaseOrderHeader – Purchase order records.

Dimension Tables

  - Production.Product – Product attributes.
  - Production.ProductSubcategory – Product subcategory classification.

### Key Skills Demonstrated

This project highlights a range of SQL skills expected from a data analyst or BI analyst:
    
  - **Date and Time Manipulation:** Using FORMAT_DATE, PARSE_DATE, DATE_TRUNC, DATE_SUB, and similar functions to prepare data for monthly and yearly reporting.
  - **Window Functions:** Applying LAG, LEAD, and ranking functions to track customer behavior, detect sequential purchases, and identify first/last touchpoints.
  - **Data Cleaning and Filtering:** Isolating valid records, removing missing values, and preparing the dataset for reliable analysis.
  - **Aggregation and Grouping:** Summarizing metrics like total revenue, order count, and monthly performance.
  - **Analytical Query Design:** Writing queries that mimic real business logic, such as calculating customer first purchase month or comparing performance across time periods.
  - **Scalable Query Structure:** Using readable formatting, aliasing, and consistent layout to make each query maintainable and easy to review.

---

## **SQL Queries & Results**
### Q1. Calculate quantity of items, sales value & order quantity by each subcategory in last 12 months (L12M).
```sql
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
```
- Result
<img width="837" height="352" alt="image" src="https://github.com/user-attachments/assets/61e6d641-0df9-4402-9160-37c6dd62960b" />

### Q2. Calculate %YoY growth rate by subcategory & release top 3 category with highest grow rate.
```sql
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
```
- Result
<img width="636" height="106" alt="image" src="https://github.com/user-attachments/assets/02fd3364-64f6-4f0a-bc2e-ffff8eb8031b" />

### Q3. Ranking Top 3 TerritoryID with biggest order quantity of every year.
```sql
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
```
- Result
<img width="837" height="352" alt="image" src="https://github.com/user-attachments/assets/4154dd37-d872-454e-a352-9524048b09ca" />

### Q4. Calculate Total Discount Cost belongs to Seasonal Discount for each subcategory.
```sql
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
```
- Result
<img width="511" height="81" alt="image" src="https://github.com/user-attachments/assets/0e20be01-6b24-43ef-9883-901333f73ca6" />

### Q5. Retention rate of customer in 2014 with status of Successfully Shipped (Cohort Analysis).
```sql
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
```
- Result
<img width="513" height="377" alt="image" src="https://github.com/user-attachments/assets/f0633fe3-5755-440e-9d93-e8c9c5dd4b29" />

## Q6. Trend of stock level & MoM% by all product in 2011.
```sql
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
```
- Result
<img width="883" height="351" alt="image" src="https://github.com/user-attachments/assets/3d7d96af-5fd3-47e0-916a-3f372bbdad86" />

## Q7. Calculate Ratio of Stock/Sales in 2011 by product name and month.
```sql
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
```
- Result
<img width="1009" height="350" alt="image" src="https://github.com/user-attachments/assets/cdb09a91-e3a3-42d2-9e2e-54cb2d87134f" />

## Q8. Number of orders and values at Pending status in 2014.
```sql
SELECT
  EXTRACT(YEAR FROM ModifiedDate) AS year
  ,Status
  ,COUNT(DISTINCT PurchaseOrderID) AS order_cnt
  ,SUM(TotalDue) AS value 
FROM adventureworks2019.Purchasing.PurchaseOrderHeader
WHERE EXTRACT(YEAR FROM ModifiedDate) = 2014
  AND Status = 1
GROUP BY 1,2;
```
- Result
<img width="561" height="55" alt="image" src="https://github.com/user-attachments/assets/2a17bc85-32d4-4678-a811-bc15f1b7be29" />

---

## **Insights & Recommendations**

| **Queries** | **Insights** | **Recommendations** |
|----------|--------------|---------------------|
| **Q1 — Sales Performance by Subcategory (Last 12 Months)** | The 12-month view highlights which subcategories drive sales volume, revenue, and order counts. Some subcategories generate high volume but low revenue, indicating lower-priced items. Others produce strong revenue despite fewer orders, suggesting high-value products. | Strengthen investment in subcategories showing stable revenue and volume growth. Reevaluate pricing or introduce cross-sell strategies for high-volume, low-revenue groups. Review pricing sensitivity for high-value items with low order counts to optimize profitability. |
| **Q2 — YoY Growth Rate & Top 3 Subcategories with Highest Growth** | The top 3 fastest-growing subcategories are the primary contributors to overall business momentum. Their strong YoY performance may signal rising market demand or successful marketing activity. | Prioritize marketing and inventory allocation for these high-growth subcategories. Monitor their growth trajectory to detect early signs of slowdown. For low-growth or negative-growth groups, reassess competitive positioning, pricing, or product relevance. |
| **Q3 — Top 3 Territories by Order Quantity Each Year** | The leading territories demonstrate consistent demand and stable customer activity. Territories dropping out of the top tier may indicate shifting customer behavior or competitive pressure. | Maintain operational and sales support in the top-performing territories. Investigate performance declines in territories falling behind and adjust commercial strategies accordingly. Use the success patterns of top territories to expand into similar markets. |
| **Q4 — Total Discount Cost from Seasonal Discount by Subcategory** | The data shows which subcategories incur the highest discount costs. High discount spending often corresponds to heavy promotional strategies or underperforming products requiring demand stimulation. | Evaluate whether discount spending effectively translates into increased sales. Reduce or restructure promotions for subcategories with high discount cost but weak revenue impact. Prioritize discount programs for items demonstrating strong conversion when discounted. |
| **Q5 — Customer Retention Cohort (2014)** | The retention pattern reveals how quickly customers return after their first purchase. Strong retention in Month 1 and Month 2 reflects product-market fit and healthy customer experience. Sharp drop-offs indicate potential issues in post-purchase engagement or limited repeat purchasing. | Strengthen onboarding and follow-up engagement immediately after the first purchase. Introduce targeted incentives to encourage early repeat purchases. Analyze one-time customers to identify barriers preventing continued engagement. |
| **Q6 — Stock Trend & MoM Change (2011)** | Monthly stock movement highlights which products experience rising or declining inventory levels. Significant MoM increases point to overstocking or reduced demand, while MoM decreases reflect fast-moving items or insufficient replenishment. | Adjust purchase planning for items with consistently rising stock levels. Increase production or replenish faster for items showing sharp stock decreases. Use MoM patterns to identify seasonality and refine supply chain planning. |
| **Q7 — Stock/Sales Ratio by Product (2011)** | A high stock/sales ratio indicates excess inventory relative to sales velocity, increasing holding costs. A low ratio shows strong demand but potential stock-out risk. | Reduce production or apply promotional tactics for high-ratio products. Increase safety stock or production for low-ratio, fast-moving items. Review monthly ratio patterns to optimize inventory thresholds. |
| **Q8 — Pending Orders & Value (2014)** | Pending orders reflect bottlenecks in approval or fulfillment workflows. High pending counts may indicate internal delays or supplier-related issues. | Audit the order approval and fulfillment process to reduce backlog. Identify the root cause—supplier delays, material shortages, or internal inefficiencies. Define clear SLAs to improve order processing time and minimize pending accumulation. |
