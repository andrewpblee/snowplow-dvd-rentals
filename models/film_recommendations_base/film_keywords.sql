{{
    materialized='table',
    schema='public', 
    alias='film_keywords'
}}

with desc_array as ( -- transform desc to array for analysis
  select
    customer_id,
    rental_id,
    inventory_id,
    film_id,
    title,
    description,
    string_to_array(lower(description), ' ') as desc_array
  from {{ ref('customer_base_information') }}
),

keywords as ( -- row per word for desc, to remove unneeded words
  select distinct
    film_id,
    unnest(desc_array) as desc_word
  from desc_array
),

film_match as (
  select distinct
    film_id,
    desc_word
  from keywords
  where desc_word not in ('a', 'an', 'and', 'in', 'the', 'of', 'who')
),

-- self join to find which films have the most similar descriptions (more than 5 matched words)
select
    a.film_id,
    b.film_id as matched_film_id,
    count(distinct a.desc_word) as word_matches
from film_match a
left join film_match b
    on a.desc_word = b.desc_word
where a.film_id <> b.film_id
group by 1, 2
having count(distinct a.desc_word) > 5
