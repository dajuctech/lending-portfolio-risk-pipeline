with staging as (
    select * from {{ ref('stg_loans') }}
)

select
    loan_id,
    loan_amount,
    funded_amount,
    grade,
    sub_grade,
    interest_rate,
    loan_status,
    issue_date,
    purpose,
    state,
    mths_since_last_delinq,
    total_payment,
    outstanding_principal,
    delinq_2yrs,
    annual_income,
    dti,
    risk_tier,
    ingest_ts,
    row_hash,

    -- derived metrics
    round(safe_divide(total_payment, funded_amount) * 100, 2)   as repayment_rate_pct,

    case
        when lower(loan_status) in (
            'charged off',
            'default',
            'does not meet the credit policy. status:charged off'
        ) then true
        else false
    end                                                          as is_defaulted

from staging
