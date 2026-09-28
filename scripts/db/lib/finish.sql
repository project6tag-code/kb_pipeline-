-- Final verdict for the verification scripts. READ ONLY.
--
-- Raises (psql exits non-zero under ON_ERROR_STOP) if any check recorded a
-- failure in kbp_verify.failures; otherwise prints the pass line. The
-- caller's READ ONLY transaction is rolled back in both cases.

DO $finish$
DECLARE
    failures integer := coalesce(nullif(current_setting('kbp_verify.failures', true), ''), '0')::integer;
BEGIN
    IF failures > 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: % check(s) failed; see the FAIL lines above', failures;
    END IF;
    RAISE NOTICE 'VERIFICATION PASSED: every check passed';
END
$finish$;

ROLLBACK;
