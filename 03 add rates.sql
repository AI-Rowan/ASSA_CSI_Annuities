DROP TABLE IF EXISTS mortality_sandbox.assa_new_gen_expected;

CREATE TABLE mortality_sandbox.assa_new_gen_expected
	WITH (
	    -- priority = 4,
			format = 'ORC',
			orc_compression = 'ZLIB',
			partitioned_by = ARRAY ['calendar_year', 'company_code'],
			bucketed_by = ARRAY ['policy_year','sex'],
			bucket_count = 25
			) AS
SELECT 'SA 85-90'                                                          AS rate_table
      ,rates.mortality_rate_qx
      ,rates.force_of_mortality_mux
      ,exps.expyearscen * rates.force_of_mortality_mux                     AS expected_count
      ,exps.expyearscen_exact * rates.force_of_mortality_mux               AS expected_count_exact
      ,exps.aar_weighted_exposure * rates.force_of_mortality_mux           AS expected_amount
      ,exps.aar_weighted_exposure_exact * rates.force_of_mortality_mux     AS expected_amount_exact
      ,exps.*
  FROM (SELECT * FROM mortality_sandbox.assa_new_gen_exposure
        UNION ALL
        SELECT * FROM mortality_sandbox.assa_new_gen_exposure_18) AS exps
       LEFT JOIN mortality_sandbox.mortality_sa8590 AS rates ON rates.age = exps.age_last_at_pa AND rates.duration = exps.duration3
 WHERE company_code != 12;
