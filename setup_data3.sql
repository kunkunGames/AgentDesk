
DO $$
DECLARE
    r_id TEXT;
    i INT;
    j INT;
BEGIN
    FOR i IN 1..50 LOOP
        r_id := 'run_' || i;
        INSERT INTO auto_queue_runs (id, status) VALUES (r_id, 'active');
    END LOOP;

    FOR i IN 1..50 LOOP
        r_id := 'run_' || i;
        FOR j IN 1..10000 LOOP
            INSERT INTO auto_queue_entries (id, run_id, status, updated_at) VALUES ('entry_' || i || '_' || j, r_id, 'pending', NOW() - (j * interval '1 minute'));
        END LOOP;
    END LOOP;
END $$;
