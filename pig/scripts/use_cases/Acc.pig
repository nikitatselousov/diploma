import 'pig/macro/LoadRoadAcc.macro';
import 'pig/macro/LoadCasualities.macro';
import 'pig/macro/LoadVehicles.macro';
import 'pig/macro/StoreCSVData.macro';

acc = LoadRoadAcc('/diploma/input_data/road_safety/DfTRoadSafety_Accidents_2014.csv');
cass = LoadCasualities('/diploma/input_data/road_safety/DfTRoadSafety_Casualties_2014.csv');
vehs = LoadVehicles('/diploma/input_data/road_safety/DfTRoadSafety_Vehicles_Marks_2014.csv');

store acc into 'diploma/road_accidents' using org.elasticsearch.hadoop.pig.EsStorage();
store cass into 'diploma/casualities' using org.elasticsearch.hadoop.pig.EsStorage();
store vehs into 'diploma/vehicles' using org.elasticsearch.hadoop.pig.EsStorage();

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
--dump a44;