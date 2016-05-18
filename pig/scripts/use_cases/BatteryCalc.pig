import 'pig/macro/GetLogBounds.macro';
import 'pig/macro/LoadBeans.macro';
import 'pig/macro/StoreCSVData.macro';

-- Load all beans from log files
all_beans = LoadBeans('$INPUT_LOG_FILE');


--Battery State united with Self state
--In self state beans 'Level' is -1 for user defined function InterpolateBatteryLevel to recognize moments of shutdown
--Also in this beans timestamp is replaced with startTime, for correct sort
battery_beans = filter all_beans by BEAN_TYPE == 'BATTERY_STATE' or BEAN_TYPE=='SELF_STATE';
battery_levels = foreach battery_beans generate
DEVICE_ID as DeviceID,
(BEAN_FIELDS#'startTime' is null ? TIMESTAMP:BEAN_FIELDS#'startTime') as Timestamp,
((int)BEAN_FIELDS#'Level' is null ? 0:(int)BEAN_FIELDS#'Level') as Level:int,
(BEAN_FIELDS#'PlugState' is null ? 'Self': BEAN_FIELDS#'PlugState') as PlugState:chararray;







--dump battery_beans;
--dump battery_levels;
-- Group Battery State beans by device ID and sort them by timestamp
battery_levels_grouped = group battery_levels by DeviceID;
battery_levels_grouped_sorted = foreach battery_levels_grouped {
sorted = order battery_levels by Timestamp;
generate group as DeviceID, sorted as SortedBeans;
};

--dump battery_levels_grouped;
--dump battery_levels_grouped_sorted;

-- Interpolate battery level for whole log
-- Intervals with self state bean between battery beans are not being interpolated
define interpolateBatteryLevel com.motorolasolutions.bigdata.mcdf.pig.eval.InterpolateBatteryLevel2();
battery_levels_interpolated = foreach battery_levels_grouped_sorted generate
DeviceID,
flatten(interpolateBatteryLevel(SortedBeans))
as (Timestamp, Level, PlugState, PointType);

--dump battery_levels_interpolated;


--StoreCSVData('$OUTPUT_BATTERY_STATE_FILE', battery_levels_interpolated);


-- Applications beans united with self state beans
-- Self state bean are replace with Application beans with empty Appsrun field
app_beans_0 = filter all_beans by BEAN_TYPE == 'APPLICATIONS';
app_beans = foreach app_beans_0 generate
    'APPLICATIONS' as BEAN_TYPE,
    DEVICE_ID,
    TIMESTAMP,
    BEAN_FIELDS#'AppsRun' as AppsRun;


battery_joined1 = join
    battery_levels by (DeviceID, Timestamp) full outer,
    app_beans by (DEVICE_ID, TIMESTAMP);
--dump battery_joined1;

battery_joined = foreach battery_joined1 generate

(DEVICE_ID is null ? DeviceID:DEVICE_ID) as DeviceID,
(TIMESTAMP is null ? Timestamp:TIMESTAMP) as Timestamp,
Level as Level:double,
(PlugState is null ? 'Apps':PlugState) as PlugState:chararray,
AppsRun as Appsrun;



--dump battery_joined;


battery_apps_grouped = group battery_joined by DeviceID;
battery_apps_grouped_sorted = foreach battery_apps_grouped {
    sorted = order battery_joined by Timestamp;
    generate group as DeviceID, sorted as SortedData;
};

--dump battery_apps_grouped_sorted;
define calculateLevels com.motorolasolutions.bigdata.mcdf.pig.eval.CalcLevelsApps();
calculated_levels1 = foreach battery_apps_grouped_sorted generate
    DeviceID,
    flatten(calculateLevels(SortedData))
    as (Timestamp, Level, Plugstate, AppsRun);

calculated_levels = order calculated_levels1 by DeviceID, Timestamp;


--dump calculated_levels;

--StoreCSVData('$OUTPUT_BATTERY_USAGE_FILE', calculated_levels);
-- Group battery and apps data by device ID and sort them by timestamp
calculated_grouped = group calculated_levels by DeviceID;
calculated_grouped_sorted = foreach calculated_grouped {
    sorted = order calculated_levels by Timestamp;
    generate group as DeviceID, sorted as SortedData1;
};

--dump calculated_grouped_sorted;

-- Get battery usage intervals for each application
define getBatteryUsage com.motorolasolutions.bigdata.mcdf.pig.eval.BatteryUsage2();
battery_usage = foreach calculated_grouped_sorted generate
    DeviceID,
    flatten(getBatteryUsage(SortedData1))
    as (Application, Timestamp, BatteryUsage);

--dump battery_usage;

StoreCSVData('$OUTPUT_BATTERY_USAGE_FILE', battery_usage);


-- Calculate battery usage speed
battery_usage_grouped = group battery_usage by (DeviceID, Application);
battery_usage_speed = foreach battery_usage_grouped generate
    group.DeviceID, group.Application,
    SUM(battery_usage.BatteryUsage) / COUNT(battery_usage.BatteryUsage) as BatteruUsageRatio;
--dump battery_usage_speed;
--StoreCSVData('$OUTPUT_BATTERY_USAGE_SPEED_FILE', battery_usage_speed);