import 'pig/macro/LoadRoadAcc.macro';
import 'pig/macro/LoadUndrData.macro';
import 'pig/macro/LoadAirportsData.macro';
import 'pig/macro/StoreCSVData.macro';

acc = LoadRoadAcc('/diploma/input_data/road_safety/DfTRoadSafety_Accidents_2014.csv');
wea = LoadUndrData('/diploma/input_data/underground_weather/underground_weather_2014.csv');
air = LoadAirportsData('/diploma/input_data/underground_weather/airports.csv');

define kok com.nik.diploma.CalculateDistance();
air1 = FOREACH air GENERATE
    ident, latitude_deg, longitude_deg, iso_country;
air2 = FILTER air1 BY (iso_country matches '.*GB.*');

acc1 = FOREACH acc GENERATE
    Accident_Index, Latitude, Longitude;
g2 = UNION ONSCHEMA acc1, air2;
g3 = ORDER g2 BY Accident_Index;

g4 = FOREACH g3 GENERATE FLATTEN (kok(*));
--g2 = GROUP g1 BY Accident_Index;
g5 = FOREACH g4 GENERATE $0 AS Accident_Index, $3 AS ident, $7 AS dist;
j1 = JOIN acc BY Accident_Index LEFT, g5 BY Accident_Index;
store j1 into 'test/road_accidents_airports' using org.elasticsearch.hadoop.pig.EsStorage();
--StoreCSVData('/diploma/output_data/', j1);
