./bin/sql-client.sh

select 'hello world';  
show functions;  
select current_timestamp;  

WITH orders_with_total AS (
    SELECT order_id, price + tax AS total
    FROM Orders
)
SELECT order_id, SUM(total)
FROM orders_with_total
GROUP BY order_id;  
with像临时视图  

desc LeftTable； 窗口详情 

SELECT user, amount
FROM Orders
WHERE product EXISTS (
    SELECT product FROM NewProducts
)


TopN用sql实现很简单  
sql里面的模式检测，可以用sql的方式实现一个复杂模式匹配，类似cep功能  

statement set可以实现一个语句插入数据到多个表  

sql-client.verbose设置为true可以打印堆栈  