import 'pig/macro/LoadRoadAcc.macro';
import 'pig/macro/LoadCasualities.macro';
import 'pig/macro/LoadVehicles.macro';
import 'pig/macro/StoreCSVData.macro';

acc = LoadRoadAcc('/diploma/input_data/road_safety/DfTRoadSafety_Accidents_2014.csv');
cass = LoadCasualities('/diploma/input_data/road_safety/DfTRoadSafety_Casualties_2014.csv');
vehs = LoadVehicles('/diploma/input_data/road_safety/DfTRoadSafety_Vehicles_Marks_2014.csv');

a1 = FILTER acc BY Accident_Index != 'Accident_Index';
a2 = FOREACH a1 GENERATE ToUnixTime(ToDate(CONCAT(Date,Time),'dd/MM/yyyyHH:mm', 'GMT')) AS timestamp,
        Accident_Index,Location_Easting_OSGR,Location_Northing_OSGR,Longitude,Latitude,Police_Force,Accident_Severity,Number_of_Vehicles,Number_of_Casualties,Date,Day_of_Week,Time,Local_Authority_District,Local_Authority_Highway,st_Road_Class,st_Road_Number,Road_Type,Speed_limit,Junction_Detail,Junction_Control,nd_Road_Class,nd_Road_Number,Pedestrian_Crossing_Human_Control,Pedestrian_Crossing_Physical_Facilities,Light_Conditions,Weather_Conditions,Road_Surface_Conditions,Special_Conditions_at_Site,Carriageway_Hazards,Urban_or_Rural_Area,Did_Police_Officer_Attend_Scene_of_Accident,LSOA_of_Accident_Location;

store a2 into 'diploma/road_accidents' using org.elasticsearch.hadoop.pig.EsStorage();
--store cass into 'diploma/casualities' using org.elasticsearch.hadoop.pig.EsStorage();
--store vehs into 'diploma/vehicles' using org.elasticsearch.hadoop.pig.EsStorage();

--dump a22;
--dump a33;
--dump a44;