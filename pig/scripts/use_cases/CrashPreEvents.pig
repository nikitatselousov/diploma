import 'pig/macro/GetLogBounds.macro';
import 'pig/macro/LoadBeans.macro';
import 'pig/macro/StoreCSVData.macro';

-- Load all beans from log files
all_beans = LoadBeans('$INPUT_LOG_FILE');


-- Filter out all beans except Battery State
battery_beans = filter all_beans by BEAN_TYPE == 'BATTERY_STATE';
battery_states = foreach battery_beans generate
    BEAN_TYPE, DEVICE_ID, TIMESTAMP, BEAN_FIELDS#'PlugState';

-- Filter out all beans except Wi-Fi Switch in the "connected" state
wifi_beans = filter all_beans by BEAN_TYPE == 'WIFI_SWITCH' AND BEAN_FIELDS#'State' == 'CONNECTED';
wify_states = foreach wifi_beans generate
    BEAN_TYPE, DEVICE_ID, TIMESTAMP, BEAN_FIELDS#'Ssid';

battery_wifi_states = union battery_states, wify_states;

-- Group battery and Wi-Fi states by device ID and Bean Type and sort them by timestamp
battery_wifi_states_grouped = group battery_wifi_states by (BEAN_TYPE, DEVICE_ID);
battery_wifi_states_grouped_sorted = foreach battery_wifi_states_grouped {
    sorted = order battery_wifi_states by TIMESTAMP;
    generate group.BEAN_TYPE as BeanType, group.DEVICE_ID as DeviceID, sorted as SortedStates;
};

-- Remove duplicate battery and Wi-Fi events
define removeDuplicateStates com.motorolasolutions.bigdata.mcdf.pig.eval.RemoveDuplicateStates();
battery_wifi_separate_states = foreach battery_wifi_states_grouped_sorted generate
    BeanType, DeviceID,
    flatten(removeDuplicateStates(SortedStates, BeanType)) as (Timestamp, EventName),
    '' as EventParam;


-- Filter out all beans except Applications
app_beans_0 = filter all_beans by BEAN_TYPE == 'APPLICATIONS';
app_beans = foreach app_beans_0 generate
    BEAN_TYPE, DEVICE_ID, TIMESTAMP, BEAN_FIELDS#'AppsRun';

-- Group beans by device ID and Bean Type and sort them by timestamp in ascending order
beans_filtered_grouped = group app_beans by (BEAN_TYPE, DEVICE_ID);
beans_filtered_grouped_sorted = foreach beans_filtered_grouped {
    sorted = order app_beans by TIMESTAMP;
    generate group.BEAN_TYPE as BeanType, group.DEVICE_ID as DeviceID, sorted as Messages;
};

-- Get separate application events: (App list change events ) --> (App Start) & (App Stop)
define getAppEvent com.motorolasolutions.bigdata.mcdf.pig.eval.ApplicationEvent();
app_events = foreach beans_filtered_grouped_sorted generate
    BeanType, DeviceID,
    flatten(getAppEvent(Messages)) as (Timestamp, EventName, EventParam);


-- Filter out all beans except Crashes
crash_beans = filter all_beans by BEAN_TYPE == 'CRASH';
crash_events = foreach crash_beans generate
    BEAN_TYPE as BeanType,
    DEVICE_ID as DeviceID,
    TIMESTAMP as Timestamp,
    BEAN_FIELDS#'CrashProcName' as EventName:chararray,
    BEAN_FIELDS#'CrashSigName' as EventParam:chararray;


all_events = union battery_wifi_separate_states, app_events, crash_events;

StoreCSVData('$OUTPUT_ALL_EVENTS_FILE', all_events);

all_events_grouped = group all_events by DeviceID;
all_events_grouped_sorted = foreach all_events_grouped {
    sorted = order all_events by Timestamp;
    generate group as DeviceID, sorted as Events;
};

-- Find event sequences that happened before each crash
define getCrashPreEvents com.motorolasolutions.bigdata.mcdf.pig.eval.CrashPreEvents('2');
pre_crash_events = foreach all_events_grouped_sorted generate
    flatten(getCrashPreEvents(Events)) as (DeviceID, Timestamp, EventName, EventParam, EventsBeforeCrash);

StoreCSVData('$OUTPUT_CRASH_PRE_EVENTS_FILE', pre_crash_events);
