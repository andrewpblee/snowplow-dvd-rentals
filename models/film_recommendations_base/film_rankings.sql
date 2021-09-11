{{
    materialized='table',
    schema='public', 
    alias='film_rankings'
}}

with film_totals as ( -- beginning to work out our overall top 10 films
  select
    film_id,
    sum(amount) as amount,
    count(distinct p.rental_id) as rentals,
    count(distinct p.customer_id) as customers
  from {{ ref('rental') }} as r
  inner join {{ ref('inventory') }} as i
    on r.inventory_id = i.inventory_id
  inner join {{ ref('payment') }} as p
    on r.rental_id = p.rental_id
  group by 1
),

film_metrics as ( -- create metrics, and normalise with rank
  select
    film_id,
    amount,
    rentals,
    customers,
    amount / customers as amount_per_customer,
    dense_rank() over (order by amount desc) as amount_position,
    dense_rank() over (order by rentals desc) as rentals_position,
    dense_rank() over (order by amount / customers desc) as apc_position
  from film_totals
),

best_films as ( -- create weighted average to judge films
  select
    f.film_id,
    actor_id,
    category_id,
    ((amount_position * 2) + rentals_position + (apc_position * 2)) / 5 as film_performance_weighted_avg_rank
  from film_metrics as f
  inner join {{ ref('film_category') }} as fc
    on f.film_id = fc.film_id
  inner join {{ ref('film_actor') }} as fa
    on f.film_id = fa.film_id
),

-- rank films based on this metric
select distinct
    film_id,
    film_performance_weighted_avg_rank,
    dense_rank() over (order by film_performance_weighted_avg_rank) as film_overall_performance
from best_films