{{
    materialized='table',
    schema='public', 
    alias='customer_base_information'
}}

-- find all films that a customer has watched
select
    c.customer_id,
    r.rental_id,
    i.inventory_id,
    i.film_id,
    f.title,
    f.description
    from {{ ref('customer') }} as c
inner join {{ ref('rental') }} as r
    on c.customer_id = r.customer_id
inner join {{ ref('inventory') }} as i
    on r.inventory_id = i.inventory_id
inner join {{ ref('film') }} as f
    on i.film_id = f.film_id
