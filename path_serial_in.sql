create or replace function p3.path_serial_in(
	road_segment_after      integer,
	road_segment_before     integer[],
	from_day                integer     default null,
	from_month              integer     default null,
	from_year               integer     default null,
	from_day_of_week        integer     default null,
	from_hour               integer     default 0,
	from_minute             integer     default 0,
	to_hour                 integer     default 23,
	to_minute               integer     default 59
)
returns table (
	road_segment integer,
	total_count integer
) as
$$
begin
    -- Preconditions
    if from_day is not null and from_month is not null and from_year is not null and from_day_of_week is not null
    then raise exception 'If you want to select a specific day, you can''t select a day of the week, and vice versa.';
	end if;
    -- Query
	for i in 0 .. array_upper(road_segment_before, 1)
   		loop
		if i = 0 then
			return query
			select
				road_segment_after,
				cast(count(*) as integer)
			from
				mapmatched_data.viterbi_match_osm_dk_20140101 v
			inner join
				dims.dimdate d
			on
				v.datekey = d.datekey
			inner join
				dims.dimtime t
			on
				v.timekey = t.timekey
			where
				v.segmentkey = road_segment_after and
				(from_day is null or d.day = from_day) and
				(from_month is null or d.month = from_month) and
				(from_year is null or d.year = from_year) and
				(from_day_of_week is null or d.iso_weekday = from_day_of_week) and
				t.hour >= from_hour and
				t.minute >= from_minute and
				t.hour <= to_hour and
				t.minute <= to_minute
			group by
				1;
		else
			return query
			with a as (
				select
					v.trip_id,
					v.trip_segmentno
				from
					mapmatched_data.viterbi_match_osm_dk_20140101 v
				inner join
					dims.dimdate d
				on
					v.datekey = d.datekey
				inner join
					dims.dimtime t
				on
					v.timekey = t.timekey
				where
					v.segmentkey = road_segment_after and
					(from_day is null or d.day = from_day) and
					(from_month is null or d.month = from_month) and
					(from_year is null or d.year = from_year) and
					(from_day_of_week is null or d.iso_weekday = from_day_of_week) and
					t.hour >= from_hour and
					t.minute >= from_minute and
					t.hour <= to_hour and
					t.minute <= to_minute
			)
			select
				v.segmentkey,
				cast(count(*) as integer)
			from
				mapmatched_data.viterbi_match_osm_dk_20140101 v, a
			where
				v.trip_id = a.trip_id and
				v.trip_segmentno = a.trip_segmentno - i and
				v.segmentkey = road_segment_before[i]
			group by
				1;
		end if;
	end loop;
end;
$$ language plpgsql