set default_parallel 1

COLOR = LOAD '$P' using PigStorage(',') as (red:long, green:long, blue:long);
REDD = foreach COLOR generate red;
GREENN = foreach COLOR generate green;
BLUEE = foreach COLOR generate blue;
RG = GROUP REDD by red;
GG = GROUP GREENN by green;
BG = GROUP BLUEE by blue;
R_GROUP = FOREACH RG generate 1 as i, group, COUNT(REDD);
G_GROUP = FOREACH GG generate 2 as i, group, COUNT(GREENN);
B_GROUP = FOREACH BG generate 3 as i, group, COUNT(BLUEE);
r_g_b = UNION R_GROUP, G_GROUP, B_GROUP;
OUTPUTT = GROUP r_g_b BY 1;
O = FOREACH OUTPUTT GENERATE FLATTEN(r_g_b);
/* O = FOREACH a_b_c GENERATE $0, $1, $2; */
store O into '$O' using PigStorage (',') PARALLEL 1;