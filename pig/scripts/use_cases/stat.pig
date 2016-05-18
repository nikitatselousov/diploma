import 'pig/macro/LoadCSVData.macro';
import 'pig/macro/StoreCSVData.macro';

wdata = LoadCSVData('/user/nik/midas_wxhrly_201401-201412.csv');
--dump wdataa;
--opp = FILTER wdataa BY (NOT(MET_DOMAIN_NAME matches '.*AWSHRLY.*'));
sa = FOREACH wdata GENERATE
       SRC_ID as SRC_ID:int;
sb = DISTINCT sa;
StoreCSVData('/user/nik/stations', sb);