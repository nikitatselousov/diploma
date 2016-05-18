import 'pig/macro/LoadCSVData1.macro';
import 'pig/macro/StoreCSVData.macro';

cdata = LoadCSVData1('/user/nik/full.csv');
--dump wdataa;
--opp = FILTER wdataa BY (NOT(MET_DOMAIN_NAME matches '.*AWSHRLY.*'));
grouped = GROUP cdata BY (Weather_Conditions);
comb = FOREACH grouped GENERATE
        FLATTEN(group) AS (Weather_Conditions),
        COUNT (cdata) as cnt;
StoreCSVData('/user/nik/colls', comb);