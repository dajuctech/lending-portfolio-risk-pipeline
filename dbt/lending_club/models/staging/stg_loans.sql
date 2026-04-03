with source as (
    select * from {{ source('lending_club_raw', 'loans_raw') }}
),

cleaned as (
    select
        id                                          as loan_id,
        loan_amnt                                   as loan_amount,
        funded_amnt                                 as funded_amount,
        grade,
        sub_grade,
        int_rate                                    as interest_rate,
        loan_status,
        issue_date,
        purpose,
        addr_state                                  as state,
        coalesce(mths_since_last_delinq, 0)         as mths_since_last_delinq,
        total_pymnt                                 as total_payment,
        out_prncp                                   as outstanding_principal,
        coalesce(delinq_2yrs, 0)                    as delinq_2yrs,
        annual_inc                                  as annual_income,
        dti,
        ingest_ts,
        row_hash,

        case
            when grade in ('A', 'B') then 'Low Risk'
            when grade in ('C', 'D') then 'Medium Risk'
            when grade in ('E', 'F', 'G') then 'High Risk'
            else 'Unknown'
        end                                         as risk_tier

    from source
    where id is not null
)

select * from cleaned
