with facts as (
    select * from {{ ref('fct_loans') }}
)

select
    grade,
    risk_tier,
    count(*)                                        as total_loans,
    round(avg(interest_rate), 2)                    as avg_interest_rate,
    round(avg(dti), 2)                              as avg_dti,
    round(avg(annual_income), 2)                    as avg_annual_income,
    round(avg(loan_amount), 2)                      as avg_loan_amount,
    sum(loan_amount)                                as total_loan_volume,
    round(avg(repayment_rate_pct), 2)               as avg_repayment_rate_pct,
    countif(is_defaulted = true)                    as total_defaults,
    round(
        countif(is_defaulted = true) * 100.0 / count(*), 2
    )                                               as default_rate_pct

from facts
group by grade, risk_tier
order by grade
