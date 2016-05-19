import 'pig/macro/LoadRoadAcc.macro';
import 'pig/macro/LoadCasualities.macro';
import 'pig/macro/LoadVehicles.macro';
import 'pig/macro/StoreCSVData.macro';

acc = LoadRoadAcc('/diploma/DfTRoadSafety_Accidents_2014.csv');
cass = LoadCasualities('/diploma/DfTRoadSafety_Casualties_2014.csv');
vehs = LoadVehicles('/diploma/Road Safety - Vehicles by Make and Model 2014.csv');

a1 = GROUP acc BY Accident_Severity;
a11 = FOREACH a1 GENERATE
        FLATTEN(group) AS (Accident_Severity),
        COUNT (acc) as cnt;

a2 = GROUP acc BY Road_Type;
a22 = FOREACH a2 GENERATE
        FLATTEN(group) AS (Road_Type),
        COUNT (acc) as cnt;

a3 = GROUP acc BY Junction_Control;
a33 = FOREACH a3 GENERATE
        FLATTEN(group) AS (Junction_Control),
        COUNT (acc) as cnt;

a4 = GROUP acc BY Weather_Conditions;
a44 = FOREACH a4 GENERATE
        FLATTEN(group) AS (Weather_Conditions),
        COUNT (acc) as cnt;
--dump a22;
--dump a33;
dump a44;