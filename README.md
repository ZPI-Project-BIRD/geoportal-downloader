# Interfacing as an AI Backend

The AI backend can atomically claim the oldest job that is pending, failed, or has
been processing for more than 30 minutes:

```sql
WITH next_job AS (
  SELECT j.id
  FROM geoportal.recognition_jobs AS j
  JOIN geoportal.features AS f ON f.id = j.feature_id
  WHERE f.downloaded_at IS NOT NULL
    AND (
      j.status IN ('0_PENDING', '3_FAILED')
      OR (j.status = '1_STARTED' AND j.started_at < NOW() - INTERVAL '30 minutes')
    )
  ORDER BY j.created_at ASC, j.id ASC
  FOR UPDATE OF j SKIP LOCKED
  LIMIT 1
)
UPDATE geoportal.recognition_jobs AS j
SET status = '1_STARTED', started_at = NOW(), finished_at = NULL, error = NULL
FROM next_job, geoportal.features AS f
WHERE j.id = next_job.id AND f.id = j.feature_id
RETURNING f.*, j.*;
```

The command returns no rows when no job is ready. `SKIP LOCKED` prevents
concurrent backend workers from claiming the same job.
