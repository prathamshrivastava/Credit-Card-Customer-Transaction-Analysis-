from faker import Faker
import pandas as pd
import random
import numpy as np

fake = Faker()

# sizes
num_customers = 10000
num_cards = 15000
num_transactions = 70000
num_payments = 10000

############################
# 1. CUSTOMERS TABLE
############################

gender_options = ["Male","Female","male","FEMALE","M","F","Other","Unknown",""]
marital_options = ["Single","Married","Divorced","single","MARRIED","S","D",""]
education_options = ["High School","Graduate","Post Graduate","Doctorate",
                     "high school","grad","PG","PhD",""]
employment_options = ["Employed","Self-employed","Unemployed","Retired",
                      "employed","self employed","jobless",""]

customers = []

for i in range(num_customers):

    age = random.randint(18,80)

    # outlier ages
    if random.random() < 0.02:
        age = random.randint(-5,120)

    income = random.randint(20000,200000)

    # missing income
    if random.random() < 0.05:
        income = None

    # negative income
    if random.random() < 0.01:
        income = random.randint(-10000,-1)

    credit_score = random.randint(300,850)

    # impossible credit score
    if random.random() < 0.02:
        credit_score = random.randint(900,1200)

    date_val = fake.date_object()

    join_date = random.choice([
        date_val.strftime("%Y-%m-%d"),
        date_val.strftime("%d-%m-%Y"),
        date_val.strftime("%Y/%m/%d"),
        str(date_val)
    ])

    city = fake.city()

    if random.random() < 0.3:
        city = city.upper()

    if random.random() < 0.2:
        city = " " + city + " "

    email = fake.email()

    if random.random() < 0.05:
        email = fake.first_name()

    customers.append({
        "customer_id": i+1,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "gender": random.choice(gender_options),
        "customer_age": age,
        "marital_status": random.choice(marital_options),
        "education_level": random.choice(education_options),
        "employment_status": random.choice(employment_options),
        "annual_income": income,
        "credit_score": credit_score,
        "email": email,
        "city": city,
        "state": fake.state(),
        "join_date": join_date
    })

customers_df = pd.DataFrame(customers)

# duplicate rows
customers_df = pd.concat([customers_df, customers_df.sample(200)])

customers_df.to_csv("customers_dirty.csv", index=False)

############################
# 2️. CARDS TABLE
############################

card_types = ["Silver","Gold","Platinum","silver","GOLD",""]
networks = ["Visa","Mastercard","Amex","visa",""]

cards = []

for i in range(num_cards):

    credit_limit = random.randint(2000,20000)

    # negative credit limit
    if random.random() < 0.02:
        credit_limit = -credit_limit

    available_credit = credit_limit - random.randint(0,5000)

    # impossible available credit
    if random.random() < 0.02:
        available_credit = credit_limit + 5000

    cards.append({
        "card_id": i+1,
        "customer_id": random.randint(1, num_customers + 500), # invalid FK possible
        "card_type": random.choice(card_types),
        "card_network": random.choice(networks),
        "card_number": fake.credit_card_number(),
        "account_open_date": fake.date(),
        "credit_limit": credit_limit,
        "available_credit": available_credit,
        "interest_rate": random.choice([12.5,15.9,18.99,22.5,None]),
        "annual_fee": random.choice([0,50,99,150,None]),
        "minimum_payment_due": random.randint(10,500),
        "card_status": random.choice(["Active","Blocked","Closed","active",""]),
    })

cards_df = pd.DataFrame(cards)

cards_df = pd.concat([cards_df, cards_df.sample(200)])

cards_df.to_csv("cards_dirty.csv", index=False)

############################
# 3️. TRANSACTIONS TABLE
############################

categories = ["Grocery","Travel","Dining","Fuel","Shopping","grocery",""]
transaction_types = ["POS","Online","ATM","Contactless","online",""]
status_options = ["Approved","Declined","approved",""]

transactions = []

for i in range(num_transactions):

    amount = round(random.uniform(5,2000),2)

    # negative transactions
    if random.random() < 0.02:
        amount = -amount

    transactions.append({
        "transaction_id": i+1,
        "card_id": random.randint(1,num_cards + 200), # invalid FK possible
        "transaction_date": fake.date_time(),
        "transaction_amount": amount,
        "merchant_name": fake.company(),
        "merchant_category": random.choice(categories),
        "merchant_city": fake.city(),
        "merchant_country": fake.country(),
        "transaction_type": random.choice(transaction_types),
        "currency": random.choice(["USD","usd","EUR",""]),
        "fraud_flag": random.choice([0,1,None]),
        "transaction_status": random.choice(status_options)
    })

transactions_df = pd.DataFrame(transactions)

transactions_df = pd.concat([transactions_df, transactions_df.sample(500)])

transactions_df.to_csv("transactions_dirty.csv", index=False)

############################
# 4️. PAYMENTS TABLE
############################

payment_methods = ["Bank Transfer","Debit Card","ACH","bank transfer",""]

payments = []

for i in range(num_payments):

    statement_balance = random.randint(500,5000)

    payment_amount = random.randint(0,6000)

    payments.append({
        "payment_id": i+1,
        "card_id": random.randint(1,num_cards + 300), # invalid FK possible
        "billing_cycle_start": fake.date(),
        "billing_cycle_end": fake.date(),
        "statement_balance": statement_balance,
        "minimum_payment_due": random.randint(20,200),
        "payment_amount": payment_amount,
        "payment_date": random.choice([fake.date(), "", None]),
        "payment_method": random.choice(payment_methods),
        "payment_status": random.choice(["Paid","Pending","paid",""]),
        "late_payment_flag": random.choice([0,1,None]),
        "days_past_due": random.choice([0,5,10,30,-3])
    })

payments_df = pd.DataFrame(payments)

payments_df = pd.concat([payments_df, payments_df.sample(200)])

payments_df.to_csv("payments_dirty.csv", index=False)

print("Dirty datasets generated successfully!")