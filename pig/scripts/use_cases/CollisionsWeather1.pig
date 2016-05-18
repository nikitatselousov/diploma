import 'pig/macro/LoadataTemp.macro';
import 'pig/macro/LoadCSVData1.macro';
import 'pig/macro/StoreCSVData.macro';

wdata = LoadataRain('/user/nik/midas_raindrnl_201401-201412.csv');
cdata = LoadCSVData1('/user/nik/full.csv');

t2 = FOREACH wdata GENERATE FLATTEN(STRSPLIT(OB_END_TIME,' ')),ID_TYPE         ,ID              ,OB_HOUR_COUNT   ,VERSION_NUM     ,MET_DOMAIN_NAME ,SRC_ID          ,REC_ST_IND      ,MAX_AIR_TEMP    ,MIN_AIR_TEMP    ,MIN_GRSS_TEMP   ,MIN_CONC_TEMP   ,MAX_AIR_TEMP_Q  ,MIN_AIR_TEMP_Q  ,MIN_GRSS_TEMP_Q ,MIN_CONC_TEMP_Q ,METO_STMP_TIME  ,MIDAS_STMP_ETIME,MAX_AIR_TEMP_J  ,MIN_AIR_TEMP_J  ,MIN_GRSS_TEMP_J ,MIN_CONC_TEMP_J;
--t2 = FILTER t1 BY (($0 matches '2014.+') OR ($0 matches '2014-02.+'));
--t3 = GROUP t2 BY ($0, $5);

--define kok com.google.transit.realtime.BatteryUsage();
--t4 = FOREACH t3 GENERATE FLATTEN (kok(t2));
t4 = FOREACH t2 GENERATE $0 AS DATE:chararray, $1 AS TIME:chararray, $3 as ID:chararray, $7 as SRC_ID:int, $9 as MAX_AIR_TEMP:chararray    ,$10 as MIN_AIR_TEMP:chararray    ,$11 as MIN_GRSS_TEMP:chararray   ,$12 as MIN_CONC_TEMP:chararray;

--cdata = FILTER cdata1 BY ((Date matches '../01/2014') OR (Date matches '../02/2014'));
grouped = GROUP cdata BY (w_src, Date);
comb = FOREACH grouped GENERATE
        FLATTEN(group) AS (w_src, Date),
        COUNT (cdata) as cnt;

fin = FOREACH comb GENERATE
        ToDate(Date,'dd/mm/yyyy') as Tate:datetime, w_src, cnt;
fin1 = FOREACH fin GENERATE
        CONCAT (ToString(Tate,'yyyy'),CONCAT('-',CONCAT(ToString(Tate,'mm'),CONCAT('-',ToString(Tate,'dd'))))) as Tate:chararray
        , w_src, cnt;
joined = JOIN fin1 BY (w_src, Tate) LEFT, t4 by (SRC_ID, DATE);

--dump f1;
StoreCSVData('/user/nik/result1', joined);
