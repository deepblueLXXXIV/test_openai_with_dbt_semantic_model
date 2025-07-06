### This is a test project for openai with dbt semantic model

dbt version: 1.10.3
python version: 3.11.5
os info: Windows-10-10.0.26100-SP0
adapter type: duckdb
adapter version: 1.9.4

ai model: gpt-4.1-mini

need to pip install langchain_core, langchain_openai, langchain_core.prompts along with dbt-core, duckdb, dbt-duckdb (if missed any dependencies, please install accordingly)

data is taken from https://www.kaggle.com/datasets/kyanyoga/sample-sales-data/data and split to 2 tables orders + customers into duckdb database file located under project_name/test/test.duckdb 

the base sql is stored under project_name/models/staging
model is stored under project_name/models/marts/oders.yml

to test run following
- dbt debug
- dbt deps
- dbt compile
- cd to the project_name folder and run python test.py

after compilation, the process will create manifest file under target folder
we will get all needed information from the manifest file for the AI to generate sql based on input question
the models and base sql should be loaded in a for loop, as a test project this part is hardcoded

following are some test runs:

dbt 语义层问答系统 | 输入 'exit' 退出

问题：2003年摩托车销量多少？

select
    count(*) as motorcycle_order_count
from (
    select
        ORDERNUMBER as order_id,
        ORDERLINENUMBER as order_sub_id,
        PRODUCTCODE as product_id,
        CUSTOMERID as customer_id,
        PRODUCTLINE as product_name,
        SALES as order_sale,
        STATUS as order_status,
        CAST(STRPTIME(SPLIT_PART(ORDERDATE, ' ', 1), '%m/%d/%Y') AS DATE) AS order_date
    from orders
) as orders_sub
where product_name = 'Motorcycles'
  and extract(year from order_date) = 2003

结果：
(109,)

问题：2003年摩托车收入多少？

select sum(order_sale) as motorcycle_revenue
from (
    select
        ORDERNUMBER as order_id,
        ORDERLINENUMBER as order_sub_id,
        PRODUCTCODE as product_id,
        CUSTOMERID as customer_id,
        PRODUCTLINE as product_name,
        SALES as order_sale,
        STATUS as order_status,
        cast(strptime(split_part(ORDERDATE, ' ', 1), '%m/%d/%Y') as date) as order_date
    from orders
) as source
where product_name = 'Motorcycles'
  and extract(year from order_date) = 2003;

结果：
(370895.58,)

问题：2004年巴士和火车收入占比多少？

select
    sum(case when product_name = 'Trucks and Buses' then order_sale else 0 end) * 1.0 / sum(order_sale) as truck_and_bus_revenue_pct
from (
    select

        ORDERNUMBER as order_id,
        ORDERLINENUMBER as order_sub_id,
        PRODUCTCODE as product_id,

        PRODUCTLINE as product_name,
        CUSTOMERNAME as customer_name,
        SALES as order_sale,
        STATUS as order_status,

        CAST(STRPTIME(SPLIT_PART(ORDERDATE, ' ', 1), '%m/%d/%Y') AS DATE) AS order_date

    from orders
) where extract(year from order_date) = 2004

结果：
(0.11014712487234204,)

问题：2003年美国用户销量多少？

select sum(order_sale) as total_sales_2003_usa
from (
    select
        ORDERNUMBER as order_id,
        ORDERLINENUMBER as order_sub_id,
        PRODUCTCODE as product_id,
        CUSTOMERID as customer_id,
        PRODUCTLINE as product_name,
        SALES as order_sale,
        STATUS as order_status,
        CAST(STRPTIME(SPLIT_PART(ORDERDATE, ' ', 1), '%m/%d/%Y') AS DATE) AS order_date
    from orders
) orders_sub
join (
    select
        CUSTOMERID as customer_id,
        CUSTOMERNAME as customer_name,
        COUNTRY as country,
        TERRITORY as territory
    from customers
) customers_sub
on orders_sub.customer_id = customers_sub.customer_id
where extract(year from order_date) = 2003
and country = 'USA'

结果：
(1305147.8799999997,)

问题：2003年美国用户销量占北美比例多少？

select
    sum(case when customers.country = 'USA' then orders.order_sale else 0 end) as usa_sales,
    sum(orders.order_sale) as na_sales,
    sum(case when customers.country = 'USA' then orders.order_sale else 0 end) * 1.0 / nullif(sum(orders.order_sale), 0) as usa_sales_pct
from
    (
        select
            ORDERNUMBER as order_id,
            ORDERLINENUMBER as order_sub_id,
            PRODUCTCODE as product_id,
            CUSTOMERID as customer_id,
            PRODUCTLINE as product_name,
            SALES as order_sale,
            STATUS as order_status,
            CAST(STRPTIME(SPLIT_PART(ORDERDATE, ' ', 1), '%m/%d/%Y') AS DATE) AS order_date
        from orders
    ) orders
join
    (
        select
            CUSTOMERID as customer_id,
            CUSTOMERNAME as customer_name,
            COUNTRY as country,
            TERRITORY as territory
        from customers
    ) customers
    on orders.customer_id = customers.customer_id
where
    extract(year from orders.order_date) = 2003
    and customers.territory = 'NA'

结果：
(1305147.8799999997, 1359757.3799999997, 0.9598387912408315)

问题：2003年美国销量最高的5个用户名和销量分别是多少？

select
  customer_name,
  sum(order_sale) as total_sales
from (
  select
    o.CUSTOMERID as customer_id,
    c.CUSTOMERNAME as customer_name,
    o.SALES as order_sale,
    cast(strptime(split_part(o.ORDERDATE, ' ', 1), '%m/%d/%Y') as date) as order_date,
    c.COUNTRY as country
  from orders o
  join customers c on o.CUSTOMERID = c.CUSTOMERID
) sub
where country = 'USA'
  and extract(year from order_date) = 2003
group by customer_name
order by total_sales desc
limit 5

结果：
('Mini Gifts Distributors Ltd.', 185128.12)
('Muscle Machine Inc', 132778.24000000002)
('Technics Stores Inc.', 104337.3)
('Mini Creations Ltd.', 97929.83000000002)
('Corporate Gift Ideas Co.', 95678.87999999999)


following are from the original dbt readme

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
