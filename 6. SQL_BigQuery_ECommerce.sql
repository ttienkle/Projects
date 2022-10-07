-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT 
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) as month,
  SUM(totals.visits) as visits,
  SUM(totals.pageviews) as pageviews,
  SUM(totals.transactions) as transactions,
  SUM(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170101' AND '20170331'
GROUP BY month
ORDER BY 1;




-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT  
  trafficSource.source as source,
  SUM(totals.visits) as total_visits,
  SUM(totals.bounces) as total_no_of_bounces,
  ROUND(SUM(totals.bounces) / SUM(totals.visits)*100,2) as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170701' AND '20170731'
GROUP BY source
ORDER BY total_visits DESC;





-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
WITH revenue_by_source AS (
    SELECT 
      PARSE_DATE('%Y%m%d',date) as parsed_date,
      trafficSource.source as source,
      SUM(totals.totalTransactionRevenue)/1000000 as revenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _table_suffix BETWEEN '20170601' AND '20170630'
    GROUP BY source, parsed_date
    HAVING revenue IS NOT NULL
    ORDER BY 2)

SELECT *
FROM (
      SELECT
        'Week' as time_type,
        FORMAT_DATE('%Y%W',parsed_date) as time,
        source,
        ROUND(SUM(revenue),2) as revenue
      FROM revenue_by_source
      GROUP BY time, source
      ORDER BY source,time)

UNION ALL

SELECT *
FROM (
      SELECT
        'Month' as time_type,
        FORMAT_DATE('%Y%m',parsed_date) as time,
        source,
        ROUND(SUM(revenue),2) as revenue
      FROM revenue_by_source
      GROUP BY time, source
      ORDER BY source,time)
    
ORDER BY source,time;





--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH purchaser_data as(
  SELECT
      FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix BETWEEN '0601' AND '0731'
  AND totals.transactions>=1
  GROUP BY month
),

non_purchaser_data as(
  SELECT
      FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
      SUM(totals.pageviews)/COUNT(distinct fullvisitorid) as avg_pageviews_non_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix BETWEEN '0601' AND '0731'
  AND totals.transactions IS NULL
  GROUP BY month
)

SELECT
    pd.*,
    avg_pageviews_non_purchase
FROM purchaser_data pd
LEFT JOIN non_purchaser_data USING(month)
ORDER BY pd.month;




-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT  
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
  ROUND(SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId),9) as avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170701' AND '20170731'
AND totals.transactions IS NOT NULL
GROUP BY month;





-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
    ROUND(SUM(totals.totalTransactionRevenue)/SUM(totals.visits),2) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE _table_suffix BETWEEN '01' AND '31'
AND totals.transactions IS NOT NULL
GROUP BY month;





-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL
WITH unnested_data AS (
  SELECT *
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
  WHERE _table_suffix BETWEEN '20170701'AND '20170731')
  
SELECT 
  v2ProductName as other_purchased_products,
  SUM(productQuantity) as quantity
FROM unnested_data
WHERE productRevenue IS NOT NULL
AND fullvisitorid IN
    (SELECT 
      DISTINCT fullvisitorid
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
    WHERE _table_suffix BETWEEN '20170701'AND '20170731'
    AND v2ProductName="YouTube Men's Vintage Henley"
    AND productRevenue IS NOT NULL)
AND v2ProductName != "YouTube Men's Vintage Henley"
GROUP BY v2ProductName
ORDER BY quantity DESC;




--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH product_data as(
SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) as month,
    COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    COUNT(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
UNNEST(hits) as hits,
UNNEST (hits.product) as product
WHERE _table_suffix BETWEEN '20170101' AND '20170331'
AND eCommerceAction.action_type IN ('2','3','6')
GROUP BY month
ORDER BY month
)

SELECT
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
FROM product_data;









