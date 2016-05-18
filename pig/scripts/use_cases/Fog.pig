import 'pig/macro/LoadUndrData.macro';
import 'pig/macro/LoadCSVData1.macro';
import 'pig/macro/StoreCSVData.macro';

wdata = LoadUndrData('/user/nik/airport_weather.csv');
cdata = LoadCSVData1('/user/nik/fullundr.csv');

--t2 = FOREACH wdata GENERATE FLATTEN(STRSPLIT(OB_TIME,' ')),SRC_ID,PRST_WX_ID;
--t2 = FILTER t1 BY (($0 matches '2014.+') OR ($0 matches '2014-02.+'));
--t3 = GROUP t2 BY ($0, $5);
w1 = FOREACH wdata GENERATE
    SRC_ID,Dates, MaxTemperature, MeanTemperature, MinTemperature, DewPoint, MeanDew_Point, Min_Dewpoint, Max_Humidity,  Mean_Humidity,  Min_Humidity,  Max_Sea_Level_PressurehPa,  Mean_Sea_Level_Pressure,  Min_Sea_Level_Pressure,  Max_Visibility,  Mean_Visibility,  Min_Visibility,  Max_Wind_Speed,  Mean_Wind_Speed,  Max_Gust_Speed, Precipitation,  CloudCover, WindDirDegrees,
    ((Precipitation == 0.0) AND ((Events matches '.+Rain.+') OR (Events matches '.+Snow.+') OR (Events matches '.+Hail.+')) AND (NOT(Events matches '.+Fog.+')) ? '' : Events) as Events:chararray;
--NE RABOTAET ^^^^
--t5 = FOREACH t2 GENERATE $0 AS DATE:chararray, $1 AS TIME:chararray, $2 as SRC_ID:int, $3 AS PRST_WX_ID:int;
--t5 = FILTER t4 BY (PRST_WX_ID > 29);

--cdata = FILTER cdata1 BY ((Date matches '../01/2014') OR (Date matches '../02/2014'));
fin = FOREACH cdata GENERATE
        ToDate(Date,'dd/mm/yyyy') as Tate:datetime,
        Accident_Index,Longitude,Latitude,Time,Weather_Conditions,Road_Surface_Conditions,w_src,dist;
fin1 = FOREACH fin GENERATE
        CONCAT (ToString(Tate,'yyyy'),CONCAT('-',CONCAT(ToString(Tate,'m'),CONCAT('-',ToString(Tate,'d'))))) as Tate:chararray,
        Accident_Index,Longitude,Latitude,Time,Weather_Conditions,Road_Surface_Conditions,w_src,dist;

joined = JOIN fin1 BY (Tate, w_src) LEFT, w1 BY (Dates, SRC_ID);
joned = FILTER joined BY ((NOT (Dates=='')));
--dump joned;
--c1 = GROUP joned BY (Accident_Index);
--define kok com.google.transit.realtime.DataNearHour();
--c2 = FOREACH c1 GENERATE FLATTEN (kok(joned));
--dump c2;
f1 = GROUP joned BY (Events);
f2 = FOREACH f1 GENERATE
        FLATTEN(group) AS (Events),
        COUNT(joned) as cnt;

--f3 = ORDER f2 BY PRST_WX_ID1, cnt DESC;
--dump f3;
--cogr = GROUP joned BY (Accident_Index);
--dump cogr;
--joined = JOIN fin1 BY (w_src, Tate) LEFT, t4 by (SRC_ID, DATE);

--dump f1;
t6 = GROUP w1 BY (Events);
t7 = FOREACH t6 GENERATE
        FLATTEN(group) AS (Events),
        COUNT(w1) as cnt;
StoreCSVData('/user/nik/undracc', f2);
StoreCSVData('/user/nik/undrall', t7);
