with developers as (
    select distinct
        md5(app_id)           as developer_key,
        developer_name,
        developer_website,
        developer_email
    from {{ ref('stg_playstore_apps') }}
    where developer_name is not null
)

select * from developers
