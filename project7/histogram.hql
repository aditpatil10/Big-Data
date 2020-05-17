drop table Colors;
drop table Redd;
drop table Greenn;
drop table Bluee;

create table Colors (
  red int,
  green int,
  blue int)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:P}' overwrite into table Colors;

create table Redd (
  type int, 
  intensity int,
  tot int
);


create table Greenn (
  type int,  
  intensity int,
  tot int
  );

create table Bluee (
  type int,  
  intensity int,
  tot int
);

INSERT OVERWRITE table Redd SELECT 1 as type, red as intensity, COUNT(*) as tot FROM Colors GROUP BY red;
INSERT OVERWRITE table Greenn SELECT 2 as type, green as intensity, COUNT(*) as tot FROM Colors GROUP BY green;
INSERT OVERWRITE table Bluee SELECT 3 as type, blue as intensity, COUNT(*) as tot FROM Colors GROUP BY blue;


SELECT * FROM Redd UNION SELECT * FROM Greenn UNION SELECT * FROM Bluee;
