with rentals_by_category as (
  select
    fc.film_id,
    fc.category_id,
    count(rental_id) as rentals
  from rental as r
  inner join inventory i
    on r.inventory_id = i.inventory_id
  inner join film_category as fc
    on i.film_id = fc.film_id
  where rental_date between '2005-01-01' and '2006-06-30'
  group by 1, 2
),

categories_ranked as (
  select
    category_id,
    film_id,
    rentals,
    row_number() over (partition by category_id order by rentals desc) as ranked
  from rentals_by_category
)

select
  category_id,
  film_id,
  rentals,
  ranked
from categories_ranked
where ranked <= 10