CREATE TABLE exchange_rate
(
    location VARCHAR(20),
    indicator VARCHAR(20),
    subject VARCHAR(3),
    measure VARCHAR(6),
    frequency VARCHAR(1),
    year VARCHAR(4),
    value NUMERIC(28,20),
    flag_codes VARCHAR(50)
)

CREATE TABLE idh
(
    id INTEGER NOT NULL,
    utc_created DATE,
    utc_updated DATE,
    country VARCHAR(50),
    country_code VARCHAR(3),
    indicator_code VARCHAR(6),
    indicator VARCHAR(50),
    value NUMERIC(5,3),
    year VARCHAR(4),
    idh_pkey PRIMARY KEY (id)
)

CREATE TABLE transactions
(
    guarantee_number VARCHAR(256) NOT NULL,
    transaction_report_id VARCHAR(6) NOT NULL,
    amount_usd NUMERIC(15,2),
    currency_name VARCHAR(50),
    end_date DATE,
    business_sector VARCHAR(50),
    city_town VARCHAR(256),
    state_province_region_name VARCHAR(50),
    state_province_region_code VARCHAR(4),
    country_name VARCHAR(50),
    region_name VARCHAR(50),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    is_woman_owned bit(1),
    is_first_time_borrower bit(1),
    business_size VARCHAR(20),
    transactions_pkey PRIMARY KEY (transaction_report_id)
)

CREATE TABLE utilization_and_claims
(
    fiscal_year VARCHAR(4),
    guarantee_number VARCHAR(256),
    partner_name VARCHAR(256),
    country VARCHAR(50),
    guarantee_start_date DATE,
    guarantee_end_date DATE,
    final_date_for_placing_loans_under_coverage DATE,
    primary_target_segment VARCHAR(50),
    secondary_target_segment VARCHAR(50),
    primary_target_sector VARCHAR(50),
    secondary_target_sector VARCHAR(50),
    credit_agreement_type VARCHAR(256),
    status VARCHAR(50),
    guarantee_percentage VARCHAR(50),
    effective_maximum_cumulative_disbursements NUMERIC(18,2),
    cumulative_utilization NUMERIC(18,2),
    cumulative_utilization_percentage VARCHAR(50),
    cumulative_loans SMALLINT,
    claims NUMERIC(18,2),
    recoveries NUMERIC(18,2)
)