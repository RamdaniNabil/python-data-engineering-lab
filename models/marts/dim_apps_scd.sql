with snapshot_data as (
    select
        app_key,
        app_id,
        app_name,
        developer_name,
        category_name,
        price,
        is_paid,
        catalog_rating,
        dbt_valid_from,
        dbt_valid_to,
        case when dbt_valid_to is null then true else false end as is_current
    from {{ ref('dim_apps_snapshot') }}
)

select * from snapshot_data
