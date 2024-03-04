with stg_orders as 
(
    select
        OrderID,  
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey, 
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey, 
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey
    from {{source('northwind','Orders')}}
),
stg_order_details as
(
    select 
        orderid,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        sum(Quantity) as quantity, 
        sum(Quantity*UnitPrice) as extendedpriceamount,
        sum(Quantity*UnitPrice*Discount) as discountamount,
        sum(Quantity*UnitPrice*(1-Discount)) as soldamount
    from {{source('northwind','Order_Details')}}
    group by orderid, productid
)
select  
    o.OrderID,
    o.employeekey,
    o.customerkey,
    o.orderdatekey,
    od.productkey,
    od.quantity,
    od.extendedpriceamount, 
    od.discountamount,
    od.soldamount
from stg_order_details od
    join stg_orders o on od.orderid = o.orderid