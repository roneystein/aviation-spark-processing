Scripts for Cassandra

CREATE KEYSPACE capstone2 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE capstone2.top10carriers (
origin text PRIMARY KEY,
least_delayed text ) ;

CREATE TABLE capstone2.top10air (
rank int,
origin text,
destination text,
percentage_delayed int,
PRIMARY KEY (origin, rank) ) ;

CREATE TABLE capstone2.xytop10carriers (
rank int,
origin text,
destination text,
carrier text,
percentage_delayed int,
PRIMARY KEY ((origin, destination), rank) ) ;

CREATE TABLE capstone2.xymeandelay (
origin text,
destination text,
mean_delay float,
PRIMARY KEY (origin, destination) ) ;

CREATE TABLE capstone.xyz (
xdepdate text,
xdeptime text,
xflight text,
origin text,
intermediate text,
destination text,
ydepdate text,
ydeptime text,
yflight text,
delay int,
PRIMARY KEY (xdepdate, origin, intermediate, destination) ) ;


TRUNCATE top10air;
TRUNCATE top10carriers;
TRUNCATE xytop10arrival;
TRUNCATE xymeandelay;
TRUNCATE xyz;