CREATE OR REPLACE FUNCTION json_extract(input JSONB, path TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF input IS NULL OR path IS NULL THEN
        RETURN NULL;
    END IF;

    IF path = '$' THEN
        RETURN input::text;
    END IF;

    BEGIN
        -- Restore the 0058 implementation.
        RETURN jsonb_path_query_first(input, ('strict ' || path)::jsonpath) #>> '{}';
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END;
END;
$$;
