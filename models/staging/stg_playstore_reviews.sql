with raw_reviews as (
    select *
    from read_json_auto('/Users/KENZA/Desktop/lab2/playstore_pipeline/data/raw/note_taking_ai_reviews.jsonl')
)

select
    md5(cast(reviewId as varchar))         as review_key,
    reviewId                               as review_id,
    app_id,
    app_name,
    cast(score as integer)                 as rating,
    cast(thumbsUpCount as integer)         as thumbs_up_count,
    content                                as review_text,
    reviewCreatedVersion                   as review_version,
    cast("at" as timestamp)                as review_at
from raw_reviews
where reviewId is not null
