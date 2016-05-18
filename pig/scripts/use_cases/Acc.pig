import 'pig/macro/LoadCSVData1.macro';
import 'pig/macro/StoreCSVData.macro';

cdata = LoadCSVData1('/user/nik/full.csv');
b = GROUP cdata BY (Date, w_src);
c = FOREACH b GENERATE
            FLATTEN (group) AS (Date,w_src),
            COUNT(cdata) AS cnt;
StoreCSVData('/user/nik/acc1', c);
--Absence of present weather means fine weather?