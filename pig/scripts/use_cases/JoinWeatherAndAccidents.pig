import 'pig/macro/LoadRoadAcc.macro';
import 'pig/macro/LoadUndrData.macro';
import 'pig/macro/StoreCSVData.macro';

acc = LoadRoadAcc('/diploma/input_data/road_safety/DfTRoadSafety_Accidents_2014.csv');
wea = LoadUndrData('/diploma/input_data/underground_weather/underground_weather_2014.csv');

