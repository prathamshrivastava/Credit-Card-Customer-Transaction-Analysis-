Create database credit_card_analysis;
use credit_card_analysis;

select * from customers_dirty limit 10;

select distinct(gender) 
from customers_dirty;

CREATE TABLE transactions_dirty (
    transaction_id INT,
    card_id INT,
    transaction_date VARCHAR(50),
    transaction_amount FLOAT,
    merchant_name VARCHAR(100),
    merchant_category VARCHAR(50),
    merchant_city VARCHAR(100),
    merchant_country VARCHAR(50),
    transaction_type VARCHAR(50),
    currency VARCHAR(10),
    fraud_flag INT,
    transaction_status VARCHAR(20)
);

LOAD DATA INFILE '"C:\Users\Pratham Shrivastava\Desktop\Data Analyst\Fintech Project\Synthetic_Data_Generation\data\transactions_dirty.csv"'
INTO TABLE transactions_dirty
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT customer_id, COUNT(*) as cnt
FROM customers_dirty
GROUP BY customer_id
HAVING cnt > 1;

select * 
from customers_dirty
where customer_id = 53;

use credit_card_analysis;

rename table cards_dirty to cards;

rename table customers_dirty to customers;
 
 -- Q1. Find the total number of customers.
select count(*) 
from customers;

-- Q2. Display the first 10 rows from the transactions table.
select * 
from transactions 
limit 10;

-- Q3. Return customer_id, first_name, last_name, and customer_age for customers older than 50.
select customer_id, first_name, last_name, customer_age
from customers
where customer_age > 50;


-- Q4. Find the total number of cards issued.
select count(card_id)
from cards;

-- Q5. Return all rows where transaction_status = 'Declined'

select *
from transactions
where transaction_status = "Declined";

-- Q6. How many cards does each customer have?

select cards.customer_id, customers.first_name, customers.last_name ,count(*) as num_cards
from cards
join customers
on cards.customer_id = customers.customer_id
group by cards.customer_id, customers.first_name, customers.last_name;


-- Q7. Find customers who have more than 1 card.
select customer_id, count(*) as num_cards
from cards
group by customer_id
having count(*) > 1;

-- Q8. Find the average credit limit for each card_type.
select card_type, avg(credit_limit)
from cards
group by card_type;

-- Q9. Find the maximum credit limit issued
select max(credit_limit)
from cards;

-- Q10. Find customers with annual_income greater than 100000.
select customer_id
from customers
where annual_income > 100000;

-- Q11. Calculate the total transaction amount.
select round(sum(transaction_amount),2)
from transactions;


-- Q12. Find the average transaction amount.
select round(avg(transaction_amount),2)
from transactions;


-- Q13. Count transactions for each merchant_category.

select merchant_category, count(*) as num_transactions
from transactions
group by merchant_category;

-- Q14. Find the top 10 largest transactions.
select transaction_amount
from transactions
order by transaction_amount desc
limit 10;


-- Q15. Count transactions per transaction_type.
select transaction_type, count(*) as count
from transactions
group by transaction_type;


-- Q16.Show customer names with their card types.
select customers.first_name, customers.last_name, cards.card_type
from customers
join cards
on customers.customer_id = cards.customer_id
group by customers.first_name, customers.last_name, cards.card_type;


-- Q17. Find total spending per customer.
select customers.customer_id, customers.first_name, customers.last_name, sum(payments.payment_amount) as total_spending
from cards
join customers
on cards.customer_id = customers.customer_id
join payments
on cards.card_id = payments.card_id
group by customers.customer_id, customers.first_name, customers.last_name;


-- Q18. Find customers who made at least one transaction
select customers.customer_id, customers.first_name, customers.last_name
from cards
join customers
on cards.customer_id = customers.customer_id
join payments
on cards.card_id = payments.card_id
group by customers.customer_id, customers.first_name, customers.last_name
having count(*) > 1;


-- Q19. Find customers who never made a transaction.
select customers.customer_id, customers.first_name, customers.last_name
from cards
join customers
on cards.customer_id = customers.customer_id
join transactions
on cards.card_id = transactions.card_id
where transactions.transaction_id is null;


-- Q20. Show each transaction with the customer name.
select transactions.transaction_id, customers.first_name, customers.last_name
from cards
join customers
on cards.customer_id = customers.customer_id
join transactions
on cards.card_id = transactions.card_id;


-- Q21. Count how many payments are marked late.
select count(*) as late
from payments
where late_payment_flag = 1;


-- Q22. Find the average payment amount.
select avg(payment_amount) as average_amount
from payments;


-- Q23. Show cards where payment_amount > statement_balance.
select distinct(payments.card_id), payments.payment_amount, payments.statement_balance
from cards
join payments
on cards.card_id = payments.card_id
where payments.payment_amount > payments.statement_balance;


-- Q24. Count payments by payment_method.
select count(*) as payment_count, payment_method
from payments
group by payment_method;


-- Q25. Find cards with days_past_due > 0
select card_id
from payments
where days_past_due > 0;


-- Q26. Find the top 10 customers by total spending.
select c.first_name, c.last_name, (payment_amount) as spending 
from customers c
join cards 
on c.customer_id = cards.customer_id
join payments p
on cards.card_id = p.card_id
order by spending desc
limit 10;


-- Q27. Find customers whose credit score is above the average credit score
select first_name, last_name
from customers 
where credit_score > (select avg(credit_score) from customers);


-- Q28. Find customers who have more than 2 cards.
select customers.customer_id, customers.first_name, customers.last_name
from customers
join cards
on customers.customer_id = cards.customer_id
group by customers.customer_id, customers.first_name, customers.last_name
having count(cards.card_id) > 1;


-- Q29. Calculate the average income per city.customers
select city, round(avg(annual_income),2) as avg_income_per_city
from customers
group by city;


-- Q30. Find the city with the highest number of customers.
SELECT city, COUNT(*) AS num_customers
FROM customers
GROUP BY city
ORDER BY num_customers DESC
LIMIT 1;


-- Q31. Find the total number of customers, cards, transactions, and payments in one query.
SELECT
    (SELECT COUNT(*) FROM customers) AS total_customers,
    (SELECT COUNT(*) FROM cards) AS total_cards,
    (SELECT COUNT(*) FROM transactions) AS total_transactions,
    (SELECT COUNT(*) FROM payments) AS total_payments;


-- Q32. Identify duplicate customers based on customer_id
select customer_id, count(*) as duplicated_count
from customers
group by customer_id
having count(*) > 1;


-- Q33. Find customers whose email does not contain '@'
select * 
from customers 
where email not like "%@%";


-- Q34. Find customers with invalid ages (<18 or >100).
select *
from customers
where customer_age < 18 or customer_age > 100;


-- Q35. Find cards where available_credit > credit_limit.
select card_id
from cards
where available_credit > credit_limit;


-- Q36. Find negative transaction amounts.
select * 
from transactions 
where transaction_amount < 0;

 





