SELECT *
FROM products
ORDER BY unitprice DESC;

SELECT *
FROM customers WHERE country = 'UK' OR country = 'Spain';

SELECT *
FROM products
WHERE unitsinstock > 100 AND unitprice >= 25;

select distinct shipcountry
from orders;

select *
from orders
where orderdate between '1996-10-01' and '1996-10-31';

select *
from orders
where shipcountry='Germany' and 
orderdate between '1996-01-01' and '1996-12-31' and
employeeid=1 and
freight >= 100 and 
shipregion is null;

select *
from orders
where shippeddate > requireddate;

select *
from orders
where orderdate between '1997-01-01' and '1997-04-30' and
shipcountry='Canada';

select *
from orders
where employeeid in (2,5,8) and
shipregion is not null and
shipvia in (1,3) 
order by employeeid ASC, shipvia ASC;

select *
from employees
where extract(year from birthdate) >= 1960 and
region is null;

