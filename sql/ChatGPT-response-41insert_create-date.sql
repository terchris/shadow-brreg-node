INSERT INTO urbalurba_status (database_download_date) 
SELECT (pg_stat_file('base/'||oid ||'/PG_VERSION')).modification 
FROM pg_database 
WHERE datname = 'importdata';