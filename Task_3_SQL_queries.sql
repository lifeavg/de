-- https://github.com/devrimgunduz/pagila

-- Вывести количество фильмов в каждой категории, отсортировать по убыванию.
select c.category_id, c.name, count(*) films_in_category
from pagila.public.film_category fc
-- можно и без привязки к category таблице, если имена не нужны
    join pagila.public.category c on c.category_id = fc.category_id
group by c.category_id
order by films_in_category desc;

-- Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
-- ( предпологаем, что фильм был арендован только если аренда (rental)
-- была оплачена (payment запись существует) )
select a.actor_id, a.first_name, a.last_name, count(*) films_rented
from pagila.public.payment p
    join pagila.public.rental r on r.rental_id = p.rental_id
    join pagila.public.inventory i on r.inventory_id = i.inventory_id
    join pagila.public.film_actor fa on i.film_id = fa.film_id
    -- можно убрать если a.first_name и a.last_name не нужны
    join pagila.public.actor a on a.actor_id = fa.actor_id
group by a.actor_id
order by films_rented asc
limit 10;

-- Вывести категорию фильмов, на которую потратили больше всего денег.
select c.category_id, c.name, sum(p.amount) amount_spent
from pagila.public.payment p
    join pagila.public.rental r on r.rental_id = p.rental_id
    join pagila.public.inventory i on r.inventory_id = i.inventory_id
    join pagila.public.film_category fc on i.film_id = fc.film_id
    join pagila.public.category c on c.category_id = fc.category_id
group by c.category_id
order by amount_spent desc
limit 1;

-- Вывести названия фильмов, которых нет в inventory.
-- Написать запрос без использования оператора IN.
select f.title
from pagila.public.film f
    left join pagila.public.inventory i on f.film_id = i.film_id
where i.inventory_id is null;

-- Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
-- Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
with counted_roles as (
        select a.actor_id,
               a.first_name,
               a.last_name,
               count(*) roles_in_films
        from pagila.public.category c
            join pagila.public.film_category fc on c.category_id = fc.category_id
            join pagila.public.film_actor fa on fc.film_id = fa.film_id
            join actor a on a.actor_id = fa.actor_id
        where c.name = 'Children'
        group by a.actor_id),
    actor_ranks as (
        select counted_roles.actor_id,
               counted_roles.first_name,
               counted_roles.last_name,
               counted_roles.roles_in_films,
               dense_rank() over (order by counted_roles.roles_in_films) actor_rank
        from counted_roles),
    max_rank as (select max(ar.actor_rank) r from actor_ranks ar)
select ar_f.actor_id,
       ar_f.first_name,
       ar_f.last_name,
       ar_f.roles_in_films
from actor_ranks ar_f
where ar_f.actor_rank > (select r from max_rank) - 3
order by roles_in_films desc;

-- Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
-- Отсортировать по количеству неактивных клиентов по убыванию.
select ct.city_id,
       ct.city,
       count(case cst.active when 1 then 1 end) active,
       count(case cst.active when 0 then 1 end) not_active
from pagila.public.customer cst
    join pagila.public.address a on a.address_id = cst.address_id
    join pagila.public.city ct on ct.city_id = a.city_id
group by ct.city_id
order by not_active desc;

-- Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах
-- (customer.address_id в этом city), и которые начинаются на букву “a”.
-- То же самое сделать для городов в которых есть символ “-”.
-- Написать все в одном запросе.
select distinct on (ct.city_id)
    ct.city_id,
    ct.city,
    ctg.category_id,
    ctg.name,
    sum(r.return_date - r.rental_date) rent_time
from rental r
    join customer c on c.customer_id = r.customer_id
    join address a on a.address_id = c.address_id
    join city ct on ct.city_id = a.city_id
    join inventory i on r.inventory_id = i.inventory_id
    join film_category fc on i.film_id = fc.film_id
    join category ctg on fc.category_id = ctg.category_id
where r.return_date is not null
   and starts_with(lower(ctg.name), 'a')
   and position('-' in ct.city) > 0
group by ct.city_id, ctg.category_id
order by ct.city_id,  rent_time desc;
