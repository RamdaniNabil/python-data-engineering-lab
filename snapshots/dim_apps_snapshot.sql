{% snapshot dim_apps_snapshot %}

{{
    config(
        target_schema='main',
        unique_key='app_id',
        strategy='check',
        check_cols=['app_name', 'category_name', 'developer_name', 'price']
    )
}}

select
    app_key,
    app_id,
    app_name,
    developer_name,
    category_name,
    price,
    is_paid,
    catalog_rating
from {{ ref('stg_playstore_apps') }}

{% endsnapshot %}
