import 'pig/macro/LoadataRain.macro';
import 'pig/macro/StoreCSVData.macro';

wdata = LoadataRain('/user/nik/midas_raindrnl_201401-201412.csv');

f1 = FILTER wdata BY (PRCP_AMT > 0);
f2 = FILTER wdata BY (PRCP_AMT == 0);

c1 = GROUP f1 all;
b1 = foreach c1 generate COUNT(f1) as cnt;


c2 = GROUP f2 all;
b2 = foreach c2 generate COUNT(f2) as cnt;

k = UNION b2, b1;
StoreCSVData('/user/nik/rain', k);