import 'pig/macro/LoadFlowFullData.macro';
import 'pig/macro/LoadFlowData.macro';
import 'pig/macro/LoadUndrData.macro';
import 'pig/macro/StoreCSVData.macro';

tdata1 = LoadFlowData('/user/nik/Oct2014.csv');
tdata2 = LoadFlowData('/user/nik/Jul2014.csv');
tdata3 = LoadFlowData('/user/nik/JAN14JT.csv');
tdata4 = LoadFlowData('/user/nik/MAR14JT.csv');

tdata = UNION ONSCHEMA tdata1, tdata2, tdata3, tdata4;
cdata = LoadFlowFullData('/user/nik/fullflow.csv');
wdata = LoadUndrData('/user/nik/airport_weather.csv');

t1 = GROUP tdata BY (LinkRef, Date);
t2 = FOREACH t1 GENERATE
    FLATTEN (group) AS (LinkRef, Date),
    SUM(tdata.Flow) AS Flow;

c1 = FOREACH cdata GENERATE
        LinkRef, SRC_ID;

g0 = JOIN t2 BY LinkRef LEFT, c1 BY LinkRef;
g2 = FOREACH g0 GENERATE
        FLATTEN(STRSPLIT(Date,' ')) AS (Datme:chararray, Time),
        t2::LinkRef, Flow, SRC_ID;
--g3 = FILTER g2 BY ((NOT(Date is null)) AND (NOT(Date == ' ')) AND (NOT(Date == '')));
g3 = FILTER g2 BY (Datme matches '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]');
g4 = FOREACH g3 GENERATE
        ToDate(Datme,'yyyy-MM-dd') as Tates:datetime,
        LinkRef, Flow, SRC_ID;
--dump g4;
w0 = FILTER wdata BY (Dates matches '[0-9]+-[0-9]+-[0-9]+');
w1 = FOREACH w0 GENERATE
    SRC_ID,
    ToDate(Dates,'yyyy-M-d') as Tate:datetime,
    MeanTemperature, Mean_Visibility,  Precipitation,  CloudCover,
    ((Precipitation == 0.0) AND ((Events matches '.+Rain.+') OR (Events matches '.+Snow.+') OR (Events matches '.+Hail.+')) AND (NOT(Events matches '.+Fog.+')) ? '' : Events) as Events:chararray;
--dump w1;
joined = JOIN g4 BY (Tates, SRC_ID) LEFT, w1 BY (Tate, SRC_ID);

joned = FILTER joined BY (NOT (Tate is null));

f1 = GROUP joned BY (LinkRef,Events);
f2 = FOREACH f1 GENERATE
        FLATTEN(group) AS (LinkRef, Events),
        AVG(joned.Flow) as aveg,
        COUNT(joned) as cnt;
--dump f2;
StoreCSVData('/user/nik/traf', f2);
--StoreCSVData('/user/nik/undrall', t7);
