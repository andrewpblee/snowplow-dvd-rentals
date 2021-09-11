{{
    materialized='table',
    schema='public', 
    alias='customer_favourite_cats_and_actors'
}}

with customers_film_categories as ( -- ranking customer's top categories
  select
    customer_id,
    category_id,
    count(fc.film_id) as film_count
  from {{ ref('customer_base_information') }}
  left join {{ ref('film_category') }} as fc
  on a.film_id = fc.film_id
  group by 1, 2
),

customer_fav_category as ( -- find the most popular category
  select distinct
    customer_id,
    first_value(category_id) over (
      partition by customer_id
      order by film_count desc
    ) as top_category
  from customers_film_categories
),

customers_film_actors as ( -- ranking customer's top actors
  select
    customer_id,
    actor_id,
    count(fc.film_id) as film_count
  from {{ ref('customer_base_information') }}
  left join {{ ref('film_actor') }} as fc
  on a.film_id = fc.film_id
  group by 1, 2
),

customer_fav_actor as ( -- find the most popular actor
  select distinct
    customer_id,
    first_value(actor_id) over (
      partition by customer_id
      order by film_count desc
    ) as top_actor
  from customers_film_actors
),

customer_favourites as (
  select
    cfc.customer_id,
    top_category,
    top_actor
  from customer_fav_category cfc
  left join customer_fav_actor cfa
    on cfc.customer_id = cfa.customer_id
),


select
    a.customer_id,
    top_category,
    top_actor,
    array_agg(film_id) as films,
    count(distinct film_id) as film_count
from {{ ref('customer_base_information') }}
left join customer_favourites as cf
    on a.customer_id = cf.customer_id
group by 1, 2, 3
