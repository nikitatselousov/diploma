import 'pig/macro/LoadataRain.macro';
import 'pig/macro/LoadCSVData1.macro';
import 'pig/macro/StoreCSVData.macro';

wdata = LoadataRain('/user/nik/midas_raindrnl_201401-201412.csv');
cdata = LoadCSVData1('/user/nik/full.csv');

t2 = FOREACH wdata GENERATE FLATTEN(STRSPLIT(OB_DATE,' ')),ID              ,ID_TYPE         ,VERSION_NUM     ,MET_DOMAIN_NAME ,OB_END_CTIME    ,OB_DAY_CNT      ,SRC_ID          ,REC_ST_IND      ,PRCP_AMT        ,OB_DAY_CNT_Q    ,PRCP_AMT_Q      ,METO_STMP_TIME  ,MIDAS_STMP_ETIME,PRCP_AMT_J;
--t2 = FILTER t1 BY (($0 matches '2014.+') OR ($0 matches '2014-02.+'));
--t3 = GROUP t2 BY ($0, $5);

--define kok com.google.transit.realtime.BatteryUsage();
--t4 = FOREACH t3 GENERATE FLATTEN (kok(t2));
t4 = FOREACH t2 GENERATE $0 AS DATE:chararray, $1 AS TIME:chararray, $3 as ID:chararray, $8 as SRC_ID:int, $10 as PRCP_AMT:chararray;

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
StoreCSVData('/user/nik/result2', joined);
