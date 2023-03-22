SQL_ROOMS = """
create table if not exists rooms (
    id serial primary key,
    name character(100) not null 
)
"""

SQL_CHECK_TYPE = """
select * from pg_type where typname = %s
"""

SQL_SEX_TYPE = """
create type {} as enum ('F', 'M')
"""

SQL_STUDENTS = """
create table if not exists students (
    id serial primary key,
    birthday date not null,
    sex sex_t not null,
    name character(150) not null,
    room serial not null,
    foreign key (room) references rooms (id)
)
"""

SQL_INSERT_ROOM = """
insert into rooms (id, name)
values(%(id)s, %(name)s)
"""

SQL_INSERT_STUDENT = """
insert into dormitory.public.students (id, birthday, sex, name, room)
values(%(id)s, %(birthday)s, %(sex)s, %(name)s, %(room)s)
"""

SQL_ROOMS_STUD_NUM = """
select room, count(*) students_number
from dormitory.public.students
group by room
"""

SQL_AVG_YOUNGEST_ROOMS = """
select room
from dormitory.public.students
group by room
order by avg(age(current_date, birthday))
limit 5
"""

SQL_BIGGEST_AGE_DIFF = """
select room
from dormitory.public.students
group by room
order by max(age(current_date, birthday)) - min(age(current_date, birthday)) desc
limit 5
"""

SQL_ROOMS_DIFF_SEX = """
select room
from dormitory.public.students
group by room
having count(distinct sex) > 1
"""