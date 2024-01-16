with a as (
	select
		v.trip_id,
		v.trip_segmentno
	from
		mapmatched_data.viterbi_match_osm_dk_20140101 v
	where
		v.segmentkey = 569303
)
select
	v.segmentkey,
	count(*),
	round(count(*) * 100.0 / sum(count(*)) over())
from
	mapmatched_data.viterbi_match_osm_dk_20140101 v, a
where
	v.trip_id = a.trip_id and
	v.trip_segmentno = a.trip_segmentno + 1 and
	v.segmentkey in (569302, 632565, 632566)
group by
	1