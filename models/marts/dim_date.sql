with dates as (
    select distinct
        cast(strftime(review_at, '%Y%m%d') as integer)  as date_key,
        cast(review_at as date)                          as date,
        year(review_at)                                  as year,
        month(review_at)                                 as month,
        quarter(review_at)                               as quarter,
        dayofweek(review_at)                             as day_of_week,
        case when dayofweek(review_at) in (0,6)
             then true else false end                    as is_weekend
    from {{ ref('stg_playstore_reviews') }}
    where review_at is not null
)

select * from dates
