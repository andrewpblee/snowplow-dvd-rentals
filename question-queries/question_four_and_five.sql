with customer_info as (
    select
    c.customer_id,
    r.rental_date,
    r.inventory_id,
    r.rental_id,
    amount,
    first_value(r.rental_date) over (
      partition by c.customer_id
      order by r.rental_date
    ) as first_rental_date
  from customer c
  left join rental r
    on c.customer_id = r.customer_id
  left join payment p
    on r.rental_id = p.rental_id
),

revenue as (
  select
    customer_id,
    sum(case when rental_date between first_rental_date and first_rental_date + interval '30 days' then amount end) first_30_day_revenue,
    sum(amount) as total_revenue
  from customer_info
  group by 1
),

percentiles as (
  select
    percentile_cont(0.3) within group (order by first_30_day_revenue) as "30th",
    percentile_cont(0.6) within group (order by first_30_day_revenue) as "60th",
    percentile_cont(0.9) within group (order by first_30_day_revenue) as "90th"
  from revenue
  where first_30_day_revenue is not null
),

tier_ranking as (
  select
    customer_id,
    first_30_day_revenue,
    case
      when first_30_day_revenue>= "90th" then 'platinum'
      when first_30_day_revenue>= "60th" then 'gold'
      when first_30_day_revenue>= "30th" then 'silver'
      when first_30_day_revenue is not null then 'bronze'
    else 'did_not_purchase'
    end as tier_rank
  from revenue, percentiles
),

customer_films as (
  select
    ci.customer_id,
    ci.rental_date,
    i.film_id,
    ci.rental_id,
    f.title,
    first_value(f.title) over (
      partition by ci.customer_id
      order by ci.rental_date
    ) as first_film,
    last_value(f.title) over (
      partition by ci.customer_id
      order by ci.rental_date
      rows between unbounded preceding and unbounded following
    ) as last_film,
    last_value(ci.rental_date) over (
      partition by ci.customer_id
      order by ci.rental_date
      rows between unbounded preceding and unbounded following
    ) as last_rental_date,
    lead(rental_date, 1) over (
      partition by customer_id
      order by rental_date
    ) as next_rental_date
  from customer_info as ci
  left join inventory as i
    on ci.inventory_id = i.inventory_id
  left join film f
    on i.film_id = f.film_id
),

time_between_rentals as (
  select
    customer_id,
    rental_date,
    first_film,
    last_film,
    last_rental_date,
    next_rental_date,
    next_rental_date::date - rental_date::date as diff
  from customer_films
),

avg_time_between_rentals as (
  select
    customer_id,
    first_film,
    last_film,
    last_rental_date,
    avg(diff) avg_days_between_rentals
  from time_between_rentals
  group by 1, 2, 3, 4
),

actors_by_rentals as (
  select
    cf.customer_id,
    actor_id,
    count(distinct rental_id) as rentals
  from customer_films as cf
  left join film_actor as fa
  on cf.film_id = fa.film_id
  group by 1, 2
),

actors_ranked as (
  select
    customer_id,
    actor_id,
    row_number() over (partition by customer_id order by rentals desc) as actor_ranked
  from actors_by_rentals
),

top_three_favourite_actors as (
  select
    customer_id,
    json_agg(
      json_build_object(
        'rank', actor_ranked,
        'actor_id', actor_id
      )
    ) as top_3_favourite_actors
  from actors_ranked
  where actor_ranked < 4
  group by 1
)

select
  r.customer_id,
  r.first_30_day_revenue,
  r.total_revenue,
  first_film,
  last_film,
  avg_days_between_rentals,
  last_rental_date,
  top_3_favourite_actors
from revenue as r
left join avg_time_between_rentals atbr
  on r.customer_id = atbr.customer_id
left join top_three_favourite_actors ttfa
  on r.customer_id = ttfa.customer_id