Scripts for Cassandra

CREATE KEYSPACE capstone2 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

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

// departure_date, period (AM/PM), origin, destination, flight, departure_time, delay)
CREATE TABLE capstone2.xyz (
departure_date text,
period text,
origin text,
destination text,
flight text,
departure_time text,
delay int,
PRIMARY KEY (departure_date, period, origin, destination) ) ;


TRUNCATE top10air;
TRUNCATE xytop10carriers;
TRUNCATE xymeandelay;
TRUNCATE xyz;