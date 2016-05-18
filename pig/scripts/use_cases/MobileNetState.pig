import 'pig/macro/GetLogBounds.macro';
import 'pig/macro/LoadBeans.macro';
import 'pig/macro/StoreCSVData.macro';

-- Load all beans from log files
all_beans = LoadBeans('$INPUT_LOG_FILE');


-- Filter out all beans except Wi-Fi Switch
wifi_beans = filter all_beans by BEAN_TYPE == 'WIFI_SWITCH';
wifi_states = foreach wifi_beans generate
    BEAN_TYPE, DEVICE_ID, TIMESTAMP, BEAN_FIELDS#'State', BEAN_FIELDS#'Ssid';

-- Filter out all beans except Mobile Net State
mobile_net_beans = filter all_beans by BEAN_TYPE == 'MOBILE_NET_STATE';
mobile_net_states = foreach mobile_net_beans generate
    BEAN_TYPE, DEVICE_ID, TIMESTAMP, BEAN_FIELDS#'State', BEAN_FIELDS#'Operator';

wifi_mobile_net_states = union wifi_states, mobile_net_states;

-- Group Wi-Fi and Mobile Net states by device ID and sort them by timestamp
wifi_mobile_net_states_grouped = group wifi_mobile_net_states by DEVICE_ID;
wifi_mobile_net_states_grouped_sorted = foreach wifi_mobile_net_states_grouped {
    sorted = order wifi_mobile_net_states by TIMESTAMP;
    generate group as DeviceID, sorted as SortedStates;
};

-- Calculate time spent connected to different data connection types
define calculateDataConnectionDurations
    com.motorolasolutions.bigdata.mcdf.pig.eval.CalculateDataConnectionDurations('$TIME_SCALE', '$PRECISION_FACTOR');
data_connection_durations = foreach wifi_mobile_net_states_grouped_sorted generate
    DeviceID,
    flatten(calculateDataConnectionDurations(SortedStates))
    as (Timestamp, ConnectionType, Duration);

StoreCSVData('$OUTPUT_DATA_CON_DURATION_FILE', data_connection_durations);


-- Get mobile networks technologies types
mobile_net_tech_types = foreach mobile_net_beans generate
    DEVICE_ID, TIMESTAMP, BEAN_FIELDS#'NetworkTechnologyType', BEAN_FIELDS#'Operator';

-- Group by device ID and sort them by timestamp
mobile_net_tech_types_grouped = group mobile_net_tech_types by DEVICE_ID;
mobile_net_tech_types_grouped_sorted = foreach mobile_net_tech_types_grouped {
    sorted = order mobile_net_tech_types by TIMESTAMP;
    generate group as DeviceID, sorted as SortedTypes;
};

-- Calculate time spent on different (2G/3G/4G) public networks' types
define calculateNetTechTypeConnectionDurations
    com.motorolasolutions.bigdata.mcdf.pig.eval.CalculateNetTechTypeConnectionDurations('$TIME_SCALE', '$PRECISION_FACTOR');
net_tech_type_con_durations = foreach mobile_net_tech_types_grouped_sorted generate
    DeviceID,
    flatten(calculateNetTechTypeConnectionDurations(SortedTypes))
    as (Timestamp, NetworkTechnologyType, Duration);

StoreCSVData('$OUTPUT_NET_TECH_TYPE_CON_DURATION_FILE', net_tech_type_con_durations);


-- Get mobile networks types
mobile_net_types = foreach mobile_net_beans generate
    DEVICE_ID, TIMESTAMP, BEAN_FIELDS#'NetworkType';

-- Group by device ID and sort them by timestamp
mobile_net_types_grouped = group mobile_net_types by DEVICE_ID;
mobile_net_types_grouped_sorted = foreach mobile_net_types_grouped {
    sorted = order mobile_net_types by TIMESTAMP;
    generate group as DeviceID, sorted as SortedTypes;
};

-- Calculate time spent on different networks' types
define calculateNetTypeConnectionDurations
    com.motorolasolutions.bigdata.mcdf.pig.eval.CalculateNetTypeConnectionDurations('$TIME_SCALE', '$PRECISION_FACTOR');
net_type_con_durations = foreach mobile_net_types_grouped_sorted generate
    DeviceID,
    flatten(calculateNetTypeConnectionDurations(SortedTypes))
    as (Timestamp, NetworkType, Duration);

StoreCSVData('$OUTPUT_NET_TYPE_CON_DURATION_FILE', net_type_con_durations);
