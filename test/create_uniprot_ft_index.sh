# hash build

/data/wyuan/pgsql/bin/psql \
  -h localhost -p 55432 -U postgres -d postgres \
  -c "CREATE INDEX CONCURRENTLY idx_uniprot_sprot_dr_accession_hash
      ON uniprot_sprot_dr USING hash (accession);"

nohup /data/wyuan/pgsql/bin/psql \
  -h localhost -p 55432 -U postgres -d postgres \
  -c "SET max_parallel_maintenance_workers = 16;
      SET maintenance_work_mem = '2GB';
      CREATE INDEX idx_uniprot_trembl_dr_accession_btree
      ON uniprot_trembl_dr USING btree (accession);" \
  > create_index_trembl.log 2>&1 &
# hash check

/data/wyuan/pgsql/bin/psql   -h localhost -p 55432 -U postgres -d postgres   -c "SELECT pid,
             phase,
             CASE
                 WHEN tuples_total > 0 THEN round(100.0 * tuples_done / tuples_total, 2)
                 ELSE NULL
             END AS pct,
             tuples_done,
             tuples_total
      FROM pg_stat_progress_create_index;"

# idmapping btree

/data/wyuan/pgsql/bin/psql \
  -h localhost -p 55432 -U postgres -d postgres \
  -c "CREATE INDEX CONCURRENTLY idx_uniprot_geneid_btree
      ON uniprot_idmapping USING btree (db_id)
      WHERE db_name = 'GeneID';"

# btree check

/data/wyuan/pgsql/bin/psql   -h localhost -p 55432 -U postgres -d postgres   -c "
SELECT pid,
       phase,
       CASE
           WHEN tuples_total > 0 THEN round(100.0 * tuples_done / tuples_total, 2)
           ELSE NULL
       END AS pct,
       tuples_done,
       tuples_total
FROM pg_stat_progress_create_index;
"





nohup /data/wyuan/pgsql/bin/psql \
  -h localhost -p 55432 -U postgres -d postgres \
  -c "SET max_parallel_maintenance_workers = 16" \
  -c "SET maintenance_work_mem = '2GB'" \
  -c "CREATE INDEX CONCURRENTLY idx_uniprot_trembl_ft_accession_btree ON uniprot_trembl_ft USING btree (accession)" \
  > create_index_trembl.log 2>&1 &