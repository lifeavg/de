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
