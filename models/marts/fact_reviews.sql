{{
    config(
        materialized='incremental',
        unique_key='review_id'
    )
}}

with reviews as (
    select
        r.review_key                                         as review_id,
        a.app_key,
        d.developer_key,
        cast(strftime(r.review_at, '%Y%m%d') as integer)    as date_key,
        r.rating,
        r.thumbs_up_count,
        r.review_text,
        r.review_version
    from {{ ref('stg_playstore_reviews') }} r
    left join {{ ref('dim_apps') }} a on r.app_id = a.app_id
    left join {{ ref('dim_developers') }} d on a.developer_key = d.developer_key
    left join {{ ref('dim_date') }} dd on cast(strftime(r.review_at, '%Y%m%d') as integer) = dd.date_key
    where a.app_key is not null

    {% if is_incremental() %}
        and r.review_at > (select max(review_at) from {{ ref('stg_playstore_reviews') }}
                           where review_key in (select review_id from {{ this }}))
    {% endif %}
)

select * from reviews
