with categories as (
    select distinct
        md5(category_id)  as category_key,
        category_id,
        category_name
    from {{ ref('stg_playstore_apps') }}
    where category_id is not null
)

select * from categories
