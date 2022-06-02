register /usr/lib/pig/piggybank.jar;

capitalbike = LOAD '$Input'
   USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') 
    AS (Duration:int,
        Start_date:chararray, 
        End_date:chararray, 
        Start_station_number:chararray, 
        Start_station:chararray, 
        End_station_number:chararray, 
        End_station:chararray, 
        Bike_number:chararray, 
        Member_type:chararray
    );
--dump capitalbike;

capitalbikeregexdate = filter capitalbike by
    (Start_date MATCHES '^([0-9]{4})-([0-1][0-9])-([0-3][0-9])\\s([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9])$')
    and (End_date MATCHES '^([0-9]{4})-([0-1][0-9])-([0-3][0-9])\\s([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9])$');
--dump capitalbikeregexdate;

capitalbikedate = foreach capitalbikeregexdate generate
        Duration,
        ToDate(Start_date,'yyyy-MM-dd HH:mm:ss') AS Start_date_t,
        ToDate(End_date,'yyyy-MM-dd HH:mm:ss') AS End_date_t,
        Start_station_number, 
        Start_station, 
        End_station_number, 
        End_station, 
        Bike_number, 
        Member_type;
--dump capitalbikeregexdate;

capitalbikedateweek_01 = foreach capitalbikedate generate
        Duration,
        GetWeekYear(Start_date_t) AS Start_date_wy,
        GetWeek(Start_date_t) AS Start_date_w,
        GetWeekYear(End_date_t) AS End_date_wy,
        GetWeek(End_date_t) AS End_date_w,
        Start_station_number, 
        Start_station, 
        End_station_number, 
        End_station, 
        Bike_number, 
        Member_type;
--dump capitalbikedateweek_01;

bikeweek = GROUP  capitalbikedateweek_01 BY (Bike_number,Start_date_wy,Start_date_w);

stationweek = GROUP  capitalbikedateweek_01 BY (Start_station_number,Start_date_wy,Start_date_w);

bikeweek_duration_SUM = FOREACH bikeweek GENERATE group, SUM(capitalbikedateweek_01.Duration) as SUM;

stationweek_COUNT = FOREACH stationweek GENERATE group, COUNT(capitalbikedateweek_01.Bike_number) as COUNT;

station_week = FOREACH station_week { 
    casualM = FILTER capitalbikedateweek_01 BY Member_type == 'Casual';
    memberM = FILTER capitalbikedateweek_01 BY Member_type == 'Member';
    GENERATE 
        group.Start_date_wy as Start_date_wy, 
        group.Start_date_w as Start_date_w, 
        group.Start_station_number as station_number, 
        group.Start_station as station_name,
        COUNT(casualM) as casuals,
        COUNT(memberM) as members,
        COUNT(capitalbikedateweek_01.Start_station) as total_start_bikes,
        COUNT(capitalbikedateweek_01.End_station_number) as total_end_bikes,
        SUM(capitalbikedateweek_01.Duration) as total_duration;
};

STORE bikeweek_duration_SUM INTO '$Output' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');
STORE station_week INTO '$Output2' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');
