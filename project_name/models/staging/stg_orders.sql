select

	ORDERNUMBER as order_id,
	ORDERLINENUMBER as order_sub_id,
	PRODUCTCODE as product_id,
	CUSTOMERID as customer_id

	PRODUCTLINE as product_name,
	SALES as order_sale,
	STATUS as order_status,
	
	CAST(STRPTIME(SPLIT_PART(ORDERDATE, ' ', 1), '%m/%d/%Y') AS DATE) AS order_date

from orders