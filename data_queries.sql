--- Task 1: How many stores does the business have and in what countries?

SELECT country_code, COUNT(country_code) AS country_count
FROM dim_store_details
GROUP BY country_code
ORDER BY country_count DESC;

--- Task 2: Which location has the most stores?

SELECT locality, COUNT(locality) AS locality_count
FROM dim_store_details
GROUP BY locality
ORDER BY locality_count DESC;

--- Task 3: Which month produced the largest amount of sales?

SELECT
	month,
	ROUND(CAST(SUM((product_quantity * product_price)) AS numeric),2) AS total_sales
FROM dim_date_details AS ddm
	LEFT JOIN
	orders_table AS o
	ON ddm.date_uuid = o.date_uuid
	LEFT JOIN
	dim_product_details AS dpd
	ON dpd.product_code = o.product_code
GROUP BY month
ORDER BY total_sales DESC;

--- Task 4: How many sales are coming from online?

ALTER TABLE orders_table
ADD web_or_not VARCHAR(7);

UPDATE orders_table
SET web_or_not = CASE
	WHEN store_code LIKE 'WEB%' THEN 'Web'
	ELSE 'Offline'
	END;

SELECT
	COUNT(product_quantity) AS number_of_sales,
	SUM(product_quantity) AS product_quantity_count,
	web_or_not
FROM dim_date_details AS ddm
	LEFT JOIN
	orders_table AS o
	ON ddm.date_uuid = o.date_uuid
	LEFT JOIN
	dim_product_details AS dpd
	ON dpd.product_code = o.product_code
	LEFT JOIN
	dim_store_details AS dsd
	ON dsd.store_code = o.store_code

GROUP BY web_or_not
ORDER BY product_quantity_count ASC;

--- Task 5: What percentage of sales come through each type of store?

WITH cte1 AS(
	SELECT
		SUM(product_quantity * product_price) AS total_sales
	FROM orders_table AS o
		LEFT JOIN
		dim_product_details AS dpd
		ON dpd.product_code = o.product_code
		LEFT JOIN
		dim_store_details AS dsd
		ON dsd.store_code = o.store_code)

	SELECT
		store_type,
		ROUND(CAST(SUM(product_quantity * product_price) AS numeric), 2) AS total_sales,
		ROUND(CAST((SUM(product_quantity * product_price)/(SELECT total_sales FROM cte1)) AS numeric)*100,2) AS percentage_totals
	FROM orders_table AS o
		LEFT JOIN
		dim_product_details AS dpd
		ON dpd.product_code = o.product_code
		LEFT JOIN
		dim_store_details AS dsd
		ON dsd.store_code = o.store_code
	GROUP BY store_type
	ORDER BY percentage_totals DESC;

--- Task 6: Which month in each year produced the highest cost of sales?

SELECT
	ROUND(CAST(SUM((product_quantity * product_price)) AS numeric),2) AS total_sales,
	year,
	month
FROM orders_table AS o
	LEFT JOIN
	dim_date_details AS ddm
	ON o.date_uuid = ddm.date_uuid
	LEFT JOIN
	dim_product_details AS dpd
	ON dpd.product_code = o.product_code
GROUP BY month, year
ORDER BY total_sales DESC
LIMIT 10;

--- Task 7: What is our staff headcount?

SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

--- Task 8: Which German store is selling the most?

SELECT
	ROUND(CAST(SUM((product_quantity * product_price)) AS numeric),2) AS total_sales,
	store_type,
	country_code
FROM orders_table AS o
	LEFT JOIN
	dim_store_details AS dsm
	ON o.store_code = dsm.store_code
	LEFT JOIN
	dim_product_details AS dpd
	ON dpd.product_code = o.product_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY total_sales ASC

--- Task 9: How quickly is the company making sales?

WITH cte1 AS(
	SELECT 
		year, 
		date_string 
	FROM dim_date_details
	ORDER BY date_string
), cte2 AS(
	SELECT
		year,
		date_string, 
		LEAD(date_string, 1, NULL) OVER (
				ORDER BY date_string) 
	AS next_purchase
	FROM cte1
), cte3 AS(
	SELECT
		year, 	
		AVG((next_purchase)-(date_string)) AS average_time_between_purchases
	 FROM cte2
	GROUP BY year
)
SELECT 
	year, 
	CONCAT('"hours": ', EXTRACT(HOUR FROM average_time_between_purchases), 
	', "minutes": ', EXTRACT(MINUTE FROM average_time_between_purchases),
	', "seconds": ', ROUND(EXTRACT(SECOND FROM average_time_between_purchases),0),
	', "milliseconds": ', LEFT(CAST((EXTRACT(MICROSECOND FROM average_time_between_purchases)*1000) AS CHAR(255)), 2)
	) AS actual_time_taken
FROM cte3
	GROUP BY year, average_time_between_purchases
	ORDER BY average_time_between_purchases DESC
LIMIT 5;



