create table tabhouse
(
    tid        int auto_increment
        primary key,
    title      varchar(100) null,
    room       varchar(50)  null,
    area       varchar(30)  null,
    floor      varchar(30)  null,
    mdate      varchar(30)  null,
    location   text         null,
    det_price  float        null,
    unit_price float        null,
    tags       varchar(30)  null,
    dis        varchar(50)  null
)  engine = InnoDB default charset = utf8;

create table area_unit_price(
    id int auto_increment
         primary key ,
    dis varchar(50) null ,
    size int null ,
    unit_price float null
)ENGINE=InnoDB default charset = utf8;