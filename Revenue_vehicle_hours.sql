/* This Script will pull a table of stops from the entire MBTA network to calcuilate Base Coverage*/
/* Other scripts to import GTFS tables were used and can be found at https://github.com/laidig/gtfs_SQL_importer */

DROP TABLE IF EXISTS allstops; 
CREATE TABLE allstops AS 
SELECT DISTINCT stop_id, stop_name, stop_lat, stop_lon
FROM gtfs_stops;

/* For the Freqent service, we will crate a table of stops on the key bus routes, light and heavy rail: where
route_desc: Rapid Tranit, Key Bus, Rail Replacement Bus
*/
DROP TABLE IF EXISTS frequent_stops;
CREATE TABLE frequent_stops AS 
SELECT DISTINCT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
FROM gtfs_routes as r, gtfs_trips as t, gtfs_stop_times as st, gtfs_stops as s
where route_desc in ('Rail Replacement Bus','Rapid Transit','Key Bus') 
AND r.route_id = t.route_id
AND t.trip_id = st.trip_id
AND st.stop_id = s.stop_id;

/* import calendar.txt table */
--Set up table
create table gtfs_calendar (
	service_id text,
	monday int,
	tuesday int,
	wednesday int,
	thursday int,
	friday int,
	saturday int,
	sunday int,
	start_date int,
	end_date int
);

-- Import from CSV
COPY gtfs_calendar FROM '/Users/ASKalila 1/Downloads/MBTA_GTFS/calendar.txt' CSV HEADER; -- ENCODING 'windows-1252';

/* Part 5) for weekdays, pull durations from stop_times of each trip whose service runds on all weekdays */

select route_desc, round(sum(trip_duration)/60.,2) as RVH from (
select  t.trip_id, c.service_id, r.route_desc, 
(left(max(st.departure_time), 2)::int * 60) + (substring(max(st.departure_time) from 4 for 2)::int) - (left(min(st.arrival_time), 2)::int * 60) + (substring(min(st.arrival_time) from 4 for 2)::int) as trip_duration
from gtfs_stop_times as st, gtfs_routes as r, gtfs_trips as t, 
	/* Add columns to calendar table to summing service on all weekdays to filter for trips that run on all weekdays */
(select *, monday+tuesday+wednesday+thursday+friday as weekdays from gtfs_calendar) as c
where r.route_id = t.route_id
and t.trip_id = st.trip_id
and t.service_id = c.service_id
	-- filter here for weekdays
and c.weekdays = 5
group by (t.trip_id, c.service_id,r.route_desc)
) as table1
group by table1.route_desc;

/* Part 5) for Saturdays, pull durations from stop_times of each trip whose service runds on Satrudays */

select route_desc, round(sum(trip_duration)/60.,2) as RVH from (
select  t.trip_id, c.service_id, r.route_desc, 
(left(max(st.departure_time), 2)::int * 60) + (substring(max(st.departure_time) from 4 for 2)::int) - (left(min(st.arrival_time), 2)::int * 60) + (substring(min(st.arrival_time) from 4 for 2)::int) as trip_duration
from gtfs_stop_times as st, gtfs_routes as r, gtfs_trips as t, 
	/* Add columns to calendar table to summing service on all weekdays to filter for trips that run on all weekdays */
(select *, monday+tuesday+wednesday+thursday+friday as weekdays from gtfs_calendar) as c
where r.route_id = t.route_id
and t.trip_id = st.trip_id
and t.service_id = c.service_id
	-- filter here for weekdays
and c.saturday = 1
group by (t.trip_id, c.service_id,r.route_desc)
) as table1
group by table1.route_desc;


