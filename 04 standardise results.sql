DROP TABLE IF EXISTS mortality_sandbox.assa_new_gen_expected_std;

CREATE TABLE mortality_sandbox.assa_new_gen_expected_std
	WITH (
	    -- priority = 5,
			format = 'ORC',
			orc_compression = 'ZLIB',
			partitioned_by = ARRAY ['calendar_year', 'company_code', 'service_client_id'],
			bucketed_by = ARRAY ['policy_year','sex'],
			bucket_count = 25
			) AS
WITH
    rating_factors
    AS
        (SELECT *
           FROM (SELECT DISTINCT age_nrst_band
                   FROM mortality_sandbox.assa_new_gen_expected)
               ,(SELECT DISTINCT duration5
                   FROM mortality_sandbox.assa_new_gen_expected)
               ,(SELECT DISTINCT sex
                   FROM mortality_sandbox.assa_new_gen_expected)
               ,(SELECT DISTINCT smoking_status
                   FROM mortality_sandbox.assa_new_gen_expected)
               ,(SELECT DISTINCT se_class
                   FROM mortality_sandbox.assa_new_gen_expected)
               ,(SELECT DISTINCT accelerator_status
                   FROM mortality_sandbox.assa_new_gen_expected)
               ,(SELECT DISTINCT company_code
                   FROM mortality_sandbox.assa_new_gen_expected)),
    standard_factors
    AS
        (  SELECT rf.age_nrst_band
                 ,rf.duration5
                 ,rf.sex
                 ,rf.smoking_status
                 ,rf.se_class
                 ,rf.accelerator_status
                 ,rf.company_code
                 ,SUM (SUM (expyearscen_exact)) OVER (PARTITION BY rf.sex) / SUM (SUM (expyearscen_exact)) OVER ()                    sex_factor
                 ,SUM (SUM (expyearscen_exact)) OVER (PARTITION BY rf.age_nrst_band) / SUM (SUM (expyearscen_exact)) OVER ()          age_factor
                 ,SUM (SUM (expyearscen_exact)) OVER (PARTITION BY rf.duration5) / SUM (SUM (expyearscen_exact)) OVER ()              dur_factor
                 ,SUM (SUM (expyearscen_exact)) OVER (PARTITION BY rf.smoking_status) / SUM (SUM (expyearscen_exact)) OVER ()         smoking_factor
                 ,SUM (SUM (expyearscen_exact)) OVER (PARTITION BY rf.se_class) / SUM (SUM (expyearscen_exact)) OVER ()               se_factor
                 ,SUM (SUM (expyearscen_exact)) OVER (PARTITION BY rf.accelerator_status) / SUM (SUM (expyearscen_exact)) OVER ()     accel_factor
                 ,SUM (SUM (expyearscen_exact)) OVER (PARTITION BY rf.company_code) / SUM (SUM (expyearscen_exact)) OVER ()           company_factor
             FROM rating_factors rf
                  INNER JOIN mortality_sandbox.csi_mort_params ON csi_mort_params.param_name = 'standard_year'
                  LEFT JOIN mortality_sandbox.assa_new_gen_expected exps
                      ON     rf.age_nrst_band = exps.age_nrst_band
                         AND rf.duration5 = exps.duration5
                         AND rf.sex = exps.sex
                         AND rf.smoking_status = exps.smoking_status
                         AND rf.se_class = exps.se_class
                         AND rf.accelerator_status = exps.accelerator_status
                         AND rf.company_code = exps.company_code
                         AND exps.calendar_year = CAST(csi_mort_params.param_value AS BIGINT)
         GROUP BY rf.age_nrst_band
                 ,rf.duration5
                 ,rf.sex
                 ,rf.smoking_status
                 ,rf.se_class
                 ,rf.accelerator_status
                 ,rf.company_code)
SELECT exps.cy_grouped
      ,exps.cy_grouped2
      ,exps.policy_year
      ,exps.policy_number
      ,exps.life_number
      ,exps.sex
      ,exps.smoking_status
      ,exps.accelerator_status
      ,exps.accelerator_status_detailed
      ,exps.underwriter_loadings
      ,exps.loaded_vs_standard
      ,exps.type_of_underwriting
      ,exps.preferred_underwriting_class
      ,exps.se_class
      ,exps.type_of_assurance
      ,exps.is_new_generation
      ,exps.special_offer_marker
      ,exps.province
      ,exps.age_last_at_pa
      ,exps.age_nrst_at_pa
      ,exps.age_next_at_pa
      ,exps.age_last_at_jan
      ,exps.age_nrst_at_jan
      ,exps.age_next_at_jan
      ,exps.age_last_band
      ,exps.age_nrst_band
      ,exps.age_next_band
      ,exps.policy_date_of_entry
      ,exps.issue_year
      ,exps.policy_duration
      ,exps.duration2
      ,exps.duration3
      ,exps.duration5
      ,exps.begin_date
      ,exps.end_date
      ,exps.effective_date_of_change_movement
      ,exps.change_in_movement_code
      ,exps.sum_assured
      ,exps.exposure_days
      ,exps.expyearscen
      ,exps.expyearscen_exact
      ,exps.aar_weighted_exposure
      ,exps.aar_weighted_exposure_exact
      ,exps.expected_count
      ,exps.expected_count_exact
      ,exps.expected_amount
      ,exps.expected_amount_exact
      ,exps.actual_claim_cnt
      ,exps.actual_claim_amt
      ,exps.cause_of_death
      ,COALESCE (stds.sex_factor, 0)        sex_std_factor
      ,COALESCE (stds.age_factor, 0)        age_std_factor
      ,COALESCE (stds.dur_factor, 0)        dur_std_factor
      ,COALESCE (stds.smoking_factor, 0)    smoking_std_factor
      ,COALESCE (stds.se_factor, 0)         se_std_factor
      ,COALESCE (stds.accel_factor, 0)      accel_std_factor
      ,COALESCE (stds.company_factor, 0)    company_std_factor
      ,CASE
           WHEN exps.expyearscen_exact = 0
           THEN
               0
           ELSE
                 1
               / SUM (exps.expyearscen_exact)
                     OVER (PARTITION BY exps.sex
                                       ,exps.age_nrst_band
                                       ,exps.duration5
                                       ,exps.smoking_status
                                       ,exps.se_class
                                       ,exps.accelerator_status
                                       ,exps.company_code)
       END                                  exposure_share
      ,COUNT (*)
            OVER (PARTITION BY exps.sex
                            ,exps.age_nrst_band
                            ,exps.duration5
                            ,exps.smoking_status
                            ,exps.se_class
                            ,exps.accelerator_status
                            ,exps.company_code) 
                                            cell_size
      ,'ZAF' as country
      ,CAST(CAST(exps.calendar_year AS VARCHAR) || '-01-01' AS DATE) calendar_date 
      ,exps.calendar_year
      ,exps.company_code
      ,exps.company_code as service_client_id
  FROM mortality_sandbox.assa_new_gen_expected  exps
       LEFT JOIN standard_factors stds
           ON     exps.sex = stds.sex
              AND exps.age_nrst_band = stds.age_nrst_band
              AND exps.duration5 = stds.duration5
              AND exps.smoking_status = stds.smoking_status
              AND exps.se_class = stds.se_class
              AND exps.accelerator_status = stds.accelerator_status
              AND exps.company_code = stds.company_code
	      
  --UNION ALL select * from mortality_sandbox.assa_new_gen_expected_std_nam    need to update NAM code to match changes to SA code
  ;
