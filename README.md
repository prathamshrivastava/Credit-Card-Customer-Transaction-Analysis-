Credit Card Customer & Transaction Analytics (Data Analyst Portfolio Project)

Project Overview
This project simulates a real-world credit card issuing company's data environment and demonstrates the full workflow of a data analyst working with messy financial data.
The dataset contains 100,000+ synthetic records representing customers, credit cards, transactions, and payments. The data is intentionally dirty and uncleaned to mimic real-world datasets that analysts encounter in financial institutions.

The goal of this project is to showcase skills in:
1. Data Cleaning
2. SQL Data Analysis
3. Business Intelligence & Visualization
4. Fraud & Risk Analysis
5. Customer Behavior Analytics

This project follows a realistic end-to-end analytics pipeline, from raw messy data to meaningful business insights.

Business Scenario
A financial services company that issues credit cards wants to analyze its portfolio to better understand:
1. Customer demographics
2. Credit card usage patterns
3. Transaction behavior
4. Payment performance
5. Fraud risk

Revenue streams
The company’s raw data contains numerous data quality issues due to system migrations, inconsistent data entry, and multiple data sources.
As a Data Analyst, the task is to:
1. Clean and prepare the data
2. Ensure data integrity across tables
3. Analyze key financial and customer metrics
4. Build dashboards and insights for stakeholders

Tools & Technologies
This project uses the following tools:
🐍 Python (Data generation & preprocessing)
🐼 Pandas
🎭 Faker (Synthetic data generation)
🗄 SQL (Data analysis queries)
📊 Power BI / Tableau (Visualization)

Dataset Structure
The project contains four relational tables similar to those used in banking systems.

1. Customers Table
Customer demographic and financial profile data.
Key fields include:
1. customer_id
2. first_name
3. last_name
4. gender
5. customer_age
6. marital_status
7. education_level
8. employment_status
9. annual_income
10. credit_score
11. city
12. state
13. join_date

2️. Cards Table
Credit card account details.
Key fields include:
1. card_id
2. customer_id
3. card_type
4. card_network
5. card_number
6. account_open_date
7. credit_limit
8. available_credit
9. interest_rate
10. annual_fee
11. minimum_payment_due
12. card_status

3️. Transactions Table
All customer card transactions.
Key fields include:
1. transaction_id
2. card_id
3. transaction_date
4. transaction_amount
5. merchant_name
6. merchant_category
7. merchant_city
8. merchant_country
9. transaction_type
10. currency
11. fraud_flag
12. transaction_status

4️. Payments Table
Billing cycle and payment information.
Key fields include:
1. payment_id
2. card_id
3. billing_cycle_start
4. billing_cycle_end
5. statement_balance
6. minimum_payment_due
7. payment_amount
8. payment_date
9. payment_method
10. payment_status
11. late_payment_flag
12. days_past_due

Intentional Data Quality Issues
The dataset intentionally includes real-world messy data problems to practice data cleaning.
Examples include:
1. Missing Values
2. Missing income
3. Empty gender fields
4. Missing payment dates
5. Duplicates
6. Duplicate customer, card, and transaction records.
7. Inconsistent Categories

Analytical Questions
This project answers real business questions used in financial analytics.

Customer Analytics
1. Who are the highest spending customers?
2. What customer segments generate the most revenue?
3. How does income relate to credit utilization?

Card Portfolio Analysis
1. Distribution of customers by card type
2. Average credit limit by income bracket
3. Cards with the highest utilization ratio

Transaction Analysis
1. Monthly transaction volume trends
2. Top merchant categories by spending
3. Online vs POS transaction behavior

Fraud Analysis
1. Fraud rate by merchant category
2. Countries with the highest fraud activity
3. Cards with multiple suspicious transactions

Risk & Delinquency Analysis
1.Customers with late payments
2. Relationship between credit score and delinquency
3. Accounts likely to default


Future Improvements
Planned enhancements for this project include:
1. Fraud detection analysis
2. Customer segmentation
3. Credit risk scoring
4. Revenue forecasting
5. Advanced SQL analytics
6. Machine learning models for default prediction

Portfolio Value
This project demonstrates the ability to work with:
1. Large datasets
2. Complex relational data models
3. Data cleaning and preprocessing
4. Financial analytics
5. Business intelligence reporting
