Select count(Activity_Time) as count
FROM kannappan_dataset01.kannappan_event_count
where Date(Activity_Time) = Date('%s')
