with apps as (
    select
        a.app_key,
        a.app_id,
        a.app_name,
        d.developer_key,
        c.category_key,
        a.price,
        a.is_paid,
        a.installs,
        a.catalog_rating,
        a.ratings_count
    from {{ ref('stg_playstore_apps') }} a
    left join {{ ref('dim_developers') }} d on a.developer_name = d.developer_name
    left join {{ ref('dim_categories') }} c on a.category_id = c.category_id
)

select * from apps
