create schema if not exists data;

create table if not exists data.events
(
    id          uuid         primary key,
    ts_sent     timestamptz  not null,
    ts_recv     timestamptz  not null default now(),
    payload     jsonb        not null
);