# Interfacing as an AI Backend

The downloaded features are published in the `detect.feature` queue. Payload of messages in this queue allows identifing the subject.

Newly downloaded features can also be polled the backend by issuing SQL queries to further ensure synchronization:

```sql
select f.*, j.status from "geoportal"."features" f
left join "geoportal"."recognition_jobs" j on j.feature_id = f.id
where f.downloaded and (j.id is null or j.status = '3_FAILED');
```

It is the backend's responsibility to ensure the state synchronization. Right after the event/poll gets received a `"geoportal"."recognition_jobs"` record need to get created/updated.
