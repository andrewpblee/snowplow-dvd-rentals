with monthly_customer_values as (--find the monthly value of every customer
  select
    r.customer_id,
    date_trunc('month', rental_date) as rental_month,
    sum(amount) as amount
  from rental as r
  inner join payment as p
    on r.rental_id = p.rental_id
  where rental_date between '2005-01-01' and '2005-12-31'
  group by 1, 2
),

total_customer_value as ( -- find outliers from total amount
  select
    customer_id,
    sum(amount) as total_amount
  from monthly_customer_values
  group by 1
),

outliers as (
  select
    percentile_cont(0.1) within group (order by total_amount) as ten_perc,
    percentile_cont(0.9) within group (order by total_amount) as ninety_perc
  from total_customer_value
),

customers_to_analyse as ( --remove outliers
  select customer_id
  from total_customer_value, outliers
  where total_amount > ten_perc
  and total_amount < ninety_perc
)

select
  store_id,
  rental_month,
  avg(amount) as average_amount
from monthly_customer_values as mcv
left outer join customers_to_analyse as cta
  on mcv.customer_id = cta.customer_id
inner join customer as c
  on mcv.customer_id = c.customer_id
where cta.customer_id is null
group by 1, 2
order by 1, 2
