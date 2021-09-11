with a as ( -- find all films that a customer has watched
  select
    c.customer_id,
    r.rental_id,
    i.inventory_id,
    i.film_id,
    f.title,
    f.description
  from customer as c
  inner join rental as r
    on c.customer_id = r.customer_id
  inner join inventory as i
    on r.inventory_id = i.inventory_id
  inner join film as f
    on i.film_id = f.film_id
),

desc_array as ( -- transform desc to array for analysis
  select
    customer_id,
    rental_id,
    inventory_id,
    film_id,
    title,
    description,
    string_to_array(lower(description), ' ') as desc_array
  from a
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

film_combos as ( -- self join to find which films have the most similar descriptions (more than 5 matched words)
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
),

customers_film_categories as ( -- ranking customer's top categories
  select
    customer_id,
    category_id,
    count(fc.film_id) as film_count
  from a
  left join film_category as fc
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
  from a
  left join film_actor as fc
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

customer_information as (
  select
    a.customer_id,
    top_category,
    top_actor,
    array_agg(film_id) as films,
    count(distinct film_id) as film_count
  from a
  left join customer_favourites as cf
    on a.customer_id = cf.customer_id
  group by 1, 2, 3
),

film_totals as ( -- beginning to work out our overall top 10 films
  select
    film_id,
    sum(amount) as amount,
    count(distinct p.rental_id) as rentals,
    count(distinct p.customer_id) as customers
  from rental as r
  inner join inventory as i
    on r.inventory_id = i.inventory_id
  inner join payment as p
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
  inner join film_category as fc
    on f.film_id = fc.film_id
  inner join film_actor as fa
    on f.film_id = fa.film_id
),

best_films_ranked as ( -- rank films based on this metric
  select distinct
    film_id,
    film_performance_weighted_avg_rank,
    dense_rank() over (order by film_performance_weighted_avg_rank) as film_overall_performance
  from best_films
),

unnested_watched_customer_films as ( -- we don't want to recommend something they've already watched
select
  customer_id,
  top_category,
  top_actor,
  unnest(films) as watched_film_id
from customer_information
),

matched_word_film_recs as ( -- for each customer, get the films to rec based on words
  select distinct
    customer_id,
    matched_film_id
  from unnested_watched_customer_films unw
  inner join film_combos fc
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
  from customer_information c
  cross join best_films b
  left outer join unnested_watched_customer_films as unw
    on c.customer_id = unw.customer_id
    and b.film_id = unw.watched_film_id
  left join matched_word_film_recs as mw
    on c.customer_id = mw.customer_id
    and b.film_id = mw.matched_film_id
  left join best_films_ranked as bfr
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