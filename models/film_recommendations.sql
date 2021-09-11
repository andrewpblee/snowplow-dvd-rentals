{{
    materialized='table',
    schema='public', 
    alias='film_recommendations'
}}

with unnested_watched_customer_films as ( -- we don't want to recommend something they've already watched
select
  customer_id,
  top_category,
  top_actor,
  unnest(films) as watched_film_id
from {{ ref('customer_favourite_cats_and_actors') }}
),

matched_word_film_recs as ( -- for each customer, get the films to rec based on words
  select distinct
    customer_id,
    matched_film_id
  from unnested_watched_customer_films unw
  inner join {{ ref('film_keywords') }} fc
    on unw.watched_film_id = fc.film_id
),

possible_film_recommendations as (
  -- cross join and filter to only include films that meet the following requirements:
  -- are in the customers favourite category, or contain their fav actor,
  -- are matched to a film they have already watched based off key words
  -- or are in the top 10 overall
  -- and have not been rented by the customer before
  select distinct
    c.customer_id,
    c.top_category,
    c.top_actor,
    b.film_id,
    b.category_id,
    b.actor_id,
    film_overall_performance,
    matched_film_id,
    b.film_performance_weighted_avg_rank
  from {{ ref('customer_favourite_cats_and_actors') }} c
  cross join {{ ref('film_rankings') }} b
  left outer join unnested_watched_customer_films as unw
    on c.customer_id = unw.customer_id
    and b.film_id = unw.watched_film_id
  left join matched_word_film_recs as mw
    on c.customer_id = mw.customer_id
    and b.film_id = mw.matched_film_id
  left join {{ ref('film_rankings') }} as bfr
    on b.film_id = bfr.film_id
  where watched_film_id is null
    and (
      c.top_category = b.category_id
      or c.top_actor = b.actor_id
      or matched_film_id is not null
      or film_overall_performance < 11
    )
),

films_matrix as (
  select
    customer_id,
    top_actor,
    top_category,
    film_id,
    category_id,
    film_overall_performance,
    matched_film_id,
    film_performance_weighted_avg_rank,
    array_agg(actor_id) as actors_in_film
  from possible_film_recommendations
  group by 1, 2, 3, 4, 5, 6, 7, 8
),

match_films as (
  -- work out how the film has been recommended
  select
    *,
    case when category_id = top_category then 1 else 0 end as category_match,
    case when top_actor = any(actors_in_film) then 1 else 0 end as actor_match,
    case when matched_film_id is not null then 1 else 0 end as keyword_match,
    case when film_overall_performance < 11 then 1 else 0 end as top_ten_overall_match -- possible take out if too generic?
  from films_matrix
),

film_match_overall_scoring as (
  -- the more matches the film rec has, the higher up it should be
  -- (this can be tweaked to be weighted)
  select
    *,
    category_match
      + actor_match
      + keyword_match
      + top_ten_overall_match as overall_score
  from match_films
),

ranking_customer_recommendations as (
  select
    customer_id,
    film_id,
    row_number() over (
      partition by customer_id
      order by overall_score desc,
      film_performance_weighted_avg_rank
    ) as recommendation_position
  from film_match_overall_scoring
)

select
  customer_id,
  json_agg(
    json_build_object(
      'film_id', film_id,
      'recommendation_position', recommendation_position
    )
  ) as recommendations
from ranking_customer_recommendations
where recommendation_position < 11
group by 1