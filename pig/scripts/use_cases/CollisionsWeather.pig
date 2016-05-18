import 'pig/macro/LoadCSVData.macro';
import 'pig/macro/LoadCSVData1.macro';
import 'pig/macro/StoreCSVData.macro';

wdata = LoadCSVData('/usr/local/diploma/midas_wxdrnl_201401-201412.csv');
cdata = LoadCSVData1('/usr/local/diploma/full.csv');

t1 = GROUP wdata BY ($0, SRC_ID);

define kok com.google.transit.realtime.BatteryUsage();
t2 = FOREACH t1 GENERATE FLATTEN (kok(wdata));

--Привести даты к одному формату и join a by (id, name), b by (id, name)
--kek = JOIN cdata BY w_src, filtered BY SRC_ID;

grouped = GROUP cdata BY (w_src, Date);
comb = FOREACH grouped GENERATE
        FLATTEN(group) AS (w_src, Date),
        COUNT (cdata),
        MAX (cdata.dist);

fin = FOREACH comb GENERATE
        ToDate(Date,'dd/mm/yyyy') as Date:datetime,
        w_src as w_src:int,
        $2 as cnt:int,
        $3 as dist:double;
fin1 = FOREACH fin GENERATE
        CONCAT (ToString(Date,'yyyy'),CONCAT('-',CONCAT(ToString(Date,'mm'),CONCAT('-',ToString(Date,'dd'))))) as Date:chararray,
        w_src as w_src:int,
        $2 as cnt:int,
        $3 as dist:double;

--dump fin1;
joined = JOIN t2 BY(Date, SRC_ID) RIGHT, fin1 BY (Date, w_src);
StoreCSVData('/usr/local/diploma/result.csv', joined);

