with raw_apps as (
    select *
    from read_json_auto('/Users/KENZA/Desktop/lab2/playstore_pipeline/data/raw/note_taking_ai_apps.jsonl')
)

select
    md5(cast(appId as varchar))           as app_key,
    appId                                  as app_id,
    title                                  as app_name,
    developer                              as developer_name,
    developerId                            as developer_id,
    developerEmail                         as developer_email,
    developerWebsite                       as developer_website,
    genre                                  as category_name,
    genreId                                as category_id,
    cast(price as numeric)                 as price,
    cast(free as boolean)                  as is_paid,
    installs,
    cast(score as numeric)                 as catalog_rating,
    cast(ratings as integer)               as ratings_count
from raw_apps
where appId is not null
