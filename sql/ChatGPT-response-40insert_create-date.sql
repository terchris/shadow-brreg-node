INSERT INTO urbalurba_status (database_download_date) 
SELECT created 
FROM pg_tables 
WHERE tablename = 'brreg_enheter_alle';