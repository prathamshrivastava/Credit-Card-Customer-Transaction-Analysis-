SELECT 
    customer_id, COUNT(*) AS cnt
FROM
    customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Removing the duplicate entries
CREATE TABLE customer_unique AS SELECT customer_id,
    MIN(first_name) AS first_name,
    MIN(last_name) AS last_name,
    MIN(gender) AS gender,
    MIN(customer_age) AS customer_age,
    MIN(marital_status) AS marital_status,
    MIN(education_level) AS education_level,
    MIN(employment_status) AS employment_status,
    MIN(annual_income) AS annual_income,
    MIN(credit_score) AS credit_score,
    MIN(email) AS email,
    MIN(city) AS city,
    MIN(state) AS state,
    MIN(join_date) AS join_date FROM
    customers
GROUP BY customer_id;

-- delete previous table and rename new table
drop table customers;
rename table customer_unique to customers;

-- deleting duplicates from cards
CREATE TABLE card_unique AS SELECT card_id,
    MIN(customer_id) AS customer_id,
    MIN(card_type) AS card_type,
    MIN(card_network) AS card_network,
    MIN(card_number) AS card_number,
    MIN(account_open_date) AS account_open_date,
    MIN(credit_limit) AS credit_limit,
    MIN(available_credit) AS available_credit,
    MIN(interest_rate) AS interest_rate,
    MIN(annual_fee) AS annual_fee,
    MIN(minimum_payment_due) AS minimum_payment_due,
    MIN(card_status) AS card_status
   FROM
    cards
GROUP BY card_id;

drop table cards;

rename table card_unique to cards;

select card_id, count(*) as cnt
from cards
group by card_id
having count(*) >1;


-- removing duplicates from transactions
select transaction_id, count(*) as cnt
from transactions
group by transaction_id
having count(*) >1;


CREATE TABLE transaction_unique AS SELECT transaction_id,
    MIN(card_id) AS card_id,
    MIN(transaction_date) AS transaction_date,
    MIN(transaction_amount) AS transaction_amount,
    MIN(merchant_name) AS merchant_name,
    MIN(merchant_category) AS merchant_category,
    MIN(merchant_city) AS merchant_city,
    MIN(merchant_country) AS merchant_country,
    MIN(currency) AS currency,
    MIN(fraud_flag) AS fraud_flag,
    MIN(transaction_status) AS transaction_status
	FROM
    transactions
GROUP BY transaction_id;

drop table transactions;

rename table transaction_unique to transactions;

-- removing duplicates from payments

CREATE TABLE payment_unique AS SELECT payment_id,
    MIN(card_id) AS card_id,
    MIN(billing_cycle_start) AS billing_cycle_start,
    MIN(billing_cycle_end) AS billing_cycle_end,
    MIN(minimum_payment_due) AS minimum_payment_due,
    MIN(payment_amount) AS payment_amount,
    MIN(payment_date) AS payment_date,
    MIN(payment_method) AS payment_method,
    MIN(payment_status) AS payment_status,
    MIN(late_payment_flag) AS late_payment_flag,
    MIN(days_past_due) AS days_past_due
	FROM
    payments
GROUP BY payment_id;

drop table payments;

rename table transaction_unique to payments;



-- remvoing negative amounts
select * 
from customers
where customer_age < 18 or customer_age > 100;

SET SQL_SAFE_UPDATES = 0;

delete from transactions
where transaction_amount < 0;

delete from customers
where customer_age < 18 or customer_age > 100;

select * 
from customers
where annual_income < 0;

delete from customers
where annual_income < 0;

select count(*) as cnt
from cards
where credit_limit < 0;

delete from cards
where credit_limit < 0;


-- standardizing categorical columns
select distinct(gender)
from customers;

update customers
set gender = case 
	when gender in ("Female", "F") then "Female"
    when gender in ("Male","M") then "Male"
    else gender
end;

select * from customers;

select distinct(marital_status)
from customers;

update customers
set marital_status = case
	when marital_status in ("D","Divorced") then "Divorced"
    when marital_status in ("S","Single") then "Single"
    when marital_status in ("MARRIED") then "Married"
    else marital_status
end;

update customers
set education_level = lower(education_level);

select distinct(education_level)
from customers;

update customers
set education_level = case
	when education_level in ("grad","graduate") then "Graduate"
    when education_level in ("pg","post graduate") then "Post Graduate"
    when education_level in ("phd","doctorate") then "doctorate"
    else education_level
end;

select distinct(employment_status)
from customers;

update customers
set employment_status = case
	when employment_status in ("Unemployed","jobless") then "Unemployed"
    when employment_status in ("Self-employed","self employed") then "Self Employed"
    else employment_status
end;

select distinct(card_network)
from cards;

select * from payments;

select distinct(payment_method)
from payments;

select * from transactions;

select distinct(transaction_status)
from transactions;

update transactions
set currency = upper(currency)







