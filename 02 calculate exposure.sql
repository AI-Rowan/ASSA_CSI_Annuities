DROP TABLE IF EXISTS assa_sandbox.assa_new_gen_exposure;

CREATE TABLE assa_sandbox.assa_new_gen_exposure
    WITH (
            format = 'ORC'
            ,orc_compression = 'ZLIB'
            ,partitioned_by = ARRAY ['calendar_year', 'company_code']
            ,bucketed_by = ARRAY ['policy_year','sex']
            ,bucket_count = 25
            ) AS
/* -------------------------------------------------------------------------------------------------------
    First we calculate a list of all the years that we will have exposure for. 
    Note that we have to calculate each possible combination of calendar year and policy year 
    e.g. 2005 calendar year will be split between 2004 and 2005 policy years
    Easiest to get list of potential policy years and get result of adding 0 and 1 to get calendar years
  -------------------------------------------------------------------------------------------------------- */
WITH exposure_years AS (
        SELECT y.policy_year
            ,y.policy_year + o.offset AS calendar_year
        FROM (
            SELECT sequence(MIN(EXTRACT(YEAR FROM effective_date_of_change_movement)) - 1, MAX(EXTRACT(YEAR FROM effective_date_of_change_movement)))
            FROM assa_sandbox.assa_new_gen_movement
            ) AS years(policy_year)
        CROSS JOIN UNNEST(policy_year) AS y(policy_year)
            ,(
                VALUES (ARRAY [0,1])
                ) AS OFFSETS (offset)
        CROSS JOIN UNNEST(offset) AS o(offset)
        )
/* -------------------------------------------------------------------------------------------------------
    There are cases where there is a termination record (e.g. 30 = death) but new exposure later
    This doesn't make sense unless there is also a reversal record so we need to remove this late exposure
    This CTE looks takes the sum of all "direction_of_movement" entries for prior records with a termination
    I do it this way in order to try pick up reversals (they should make the sum 0)
    We also take the opportunity to check whether the current record is a termination (attempting to allow for reinstatement)
  -------------------------------------------------------------------------------------------------------- */
   ,termination_check
    AS
        (SELECT SUM (CASE
                         WHEN movement_code_clean IN ('30'
                                                     ,'43'
                                                     ,'44'
                                                     ,'50')
                         THEN
                             direction_of_movement
                         ELSE
                             0
                     END)
                    OVER (PARTITION BY company_code, policy_number, life_number
                          ORDER BY effective_date_of_change_movement, movementcounter
                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)    AS prior_termination
               ,SUM (CASE WHEN movement_code_clean = '30' THEN direction_of_movement ELSE 0 END)
                    OVER (PARTITION BY company_code, policy_number, life_number
                          ORDER BY effective_date_of_change_movement, movementcounter
                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)    AS prior_claim
               ,SUM (CASE WHEN movement_code_clean = '30' THEN direction_of_movement ELSE 0 END)
                    OVER (PARTITION BY company_code, policy_number, life_number
                          ORDER BY effective_date_of_change_movement, movementcounter
                          ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)    AS reversal_follows
               ,SUM (CASE WHEN movement_code_clean = '30' AND direction_of_movement = 1 THEN direction_of_movement ELSE 0 END)
                    OVER (PARTITION BY company_code, policy_number, life_number
                          ORDER BY effective_date_of_change_movement, movementcounter
                          ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)    AS following_reversals
               ,SUM (CASE WHEN movement_code_clean = '30' THEN direction_of_movement ELSE 0 END)
                    OVER (PARTITION BY company_code, policy_number, life_number
                          ORDER BY effective_date_of_change_movement, movementcounter
                          ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)    AS claim_follows
               ,SUM (CASE WHEN movement_code_clean = '30' AND direction_of_movement = -1 THEN direction_of_movement ELSE 0 END)
                    OVER (PARTITION BY company_code, policy_number, life_number
                          ORDER BY effective_date_of_change_movement, movementcounter
                          ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)    AS following_claims
               ,-SUM (CASE
                          WHEN movement_code_clean IN ('30'
                                                      ,'43'
                                                      ,'44'
                                                      ,'50')
                          THEN
                              direction_of_movement
                          ELSE
                              0
                      END)
                     OVER (PARTITION BY company_code
                                       ,policy_number
                                       ,life_number
                                       ,effective_date_of_change_movement)     AS current_termination
               ,assa_new_gen_movement.*
           FROM assa_sandbox.assa_new_gen_movement) 
    /* -------------------------------------------------------------------------------------------------------
    Next we need to find the next movement for this policy so that we know the end of the exposure period
    We cut off at the end of the study period, or if this is a termination stop where we are.
    We also use this step to remove the post-termination exposure that we identified in the last section
    -------------------------------------------------------------------------------------------------------- */                                              
   ,calc_next_movement
    AS
        (SELECT                                                                                              -- try to find next movement date for this policy
                LEAD (
                    effective_date_of_change_movement
                   ,1
                   -- If there is no next movement, the default will be...
                   ,CASE
                        WHEN current_termination != 0
                        THEN
                            effective_date_of_change_movement                                                      -- set it to termination date if available,
                        WHEN EXTRACT (YEAR FROM effective_date_of_change_movement) < EXTRACT (YEAR FROM date (param_value)) - 1
                        THEN
                            date_add (
                                'day'
                               ,CAST (
                                      date_diff ('day'
                                                ,effective_date_of_change_movement
                                                ,from_iso8601_date (CAST (EXTRACT (YEAR FROM effective_date_of_change_movement) + 1 AS VARCHAR) || '-01-01'))
                                    / 2.0 AS bigint)
                               ,effective_date_of_change_movement)
                        -- or assume termination half way through remainder of year
                        ELSE
                            date (param_value)
                    -- or end of investigation period if this is last full year,
                    END)
                    OVER (PARTITION BY company_code, policy_number, life_number ORDER BY effective_date_of_change_movement, movementcounter)    AS next_movement
               ,date (csi_mort_params.param_value)                                                                                              AS study_end_date
               ,termination_check.*
           FROM termination_check INNER JOIN assa_sandbox.csi_mort_params ON csi_mort_params.param_name = 'exposure_end_date'
          WHERE NOT ( -- We are conservative and try to include all claims even if there was an earlier termination record, unless that record was a claim too
                        (COALESCE (prior_termination, 0) < 0 AND movement_code_clean != '30')
                     OR (movement_code_clean = '30' AND COALESCE (following_reversals, 0) > 0))) 
    /* -------------------------------------------------------------------------------------------------------
    Now that we have the known end date, we need to split into individual calendar and policy years.
    This is necessary because of cases where there is no renewal record in a given calendar year
    We join each record to the policy years from exposure_years that fall between this movement and the next
    Each policy year has two calendar years associated with it, so this will normally create multiple records
    We do it that way so that we can split each calendar year into its constituent policy years
    -------------------------------------------------------------------------------------------------------- */
   ,create_all_years
    AS
        (SELECT exposure_years.*
               ,date_add ('year', policy_year - EXTRACT (YEAR FROM policy_date_of_entry), policy_date_of_entry)           AS policy_anniversary
               ,date_add ('year', calendar_year - EXTRACT (YEAR FROM policy_date_of_entry), policy_date_of_entry)         AS cy_anniversary
               ,date_add ('year', (policy_year + 1) - EXTRACT (YEAR FROM policy_date_of_entry), policy_date_of_entry)     AS next_policy_anniversary
               ,calc_next_movement.*
           FROM calc_next_movement
                FULL OUTER JOIN exposure_years
                    ON     exposure_years.calendar_year >= EXTRACT (YEAR FROM effective_date_of_change_movement)
                       AND exposure_years.calendar_year <=
                           COALESCE (EXTRACT (YEAR FROM GREATEST (date_add ('day', -1, next_movement), effective_date_of_change_movement)), 9999)) 
    /* -------------------------------------------------------------------------------------------------------
    Now we filter some of the newly created date records and calculate beginning and end dates
    to ensure that we have no overlapping exposure
    A reminder that we've basically made sure we have (at least) two records for each year:
    ** Before policy anniversary
    ** After policy anniversary
    Any existing records for each year would have effectively been duplicated,
    so we need to make sure to only keep the right records
    Then we set the beginning date to the latest of the prior policy anniversary, the start of this year, or the date of this movement
    End date becomes earliest of next policy anniversary, end of this year, or date of next movement
    -------------------------------------------------------------------------------------------------------- */
   ,exposure_boundary_dates
    AS
        (SELECT GREATEST (effective_date_of_change_movement, policy_anniversary, date_parse (CAST (calendar_year AS VARCHAR) || '-01-01', '%Y-%m-%d'))    AS begin_date
               ,LEAST (next_movement, next_policy_anniversary, date_parse (CAST (calendar_year + 1 AS VARCHAR) || '-01-01', '%Y-%m-%d'))                  AS end_date
               ,create_all_years.*
           FROM create_all_years
          WHERE (effective_date_of_change_movement < cy_anniversary AND policy_year < calendar_year) OR (policy_year >= calendar_year)) 
    /* -------------------------------------------------------------------------------------------------------
    Here we finally calculate the number of days applicable for the given record, and the number of days
    in the corresponding calendar year so that we can calculate the exposure in life years
    -------------------------------------------------------------------------------------------------------- */
   ,exposure_calc
    AS
        (SELECT date_diff ('year', policy_date_of_entry, policy_anniversary)                                               AS policy_duration
               ,date_diff ('day', begin_date, end_date)                                                                    AS exposure_days
               ,date_diff ('day'
                          ,date_trunc ('year', effective_date_of_change_movement)
                          ,date_trunc ('year', date_add ('year', 1, effective_date_of_change_movement)))                   AS days_in_year
               ,date_diff ('year', date_of_birth, policy_anniversary)                                                      AS age_last_at_pa
               ,date_diff ('year', date_of_birth, date_add ('month', 6, policy_anniversary))                               AS age_nrst_at_pa
               ,date_diff ('year', date_of_birth, date_parse (CAST (calendar_year AS VARCHAR) || '-01-01', '%Y-%m-%d'))    AS age_last_at_jan
               ,date_diff ('year', date_of_birth, date_parse (CAST (calendar_year AS VARCHAR) || '-07-01', '%Y-%m-%d'))    AS age_nrst_at_jan
               ,exposure_boundary_dates.*
           FROM exposure_boundary_dates
          WHERE end_date <= study_end_date AND begin_date < study_end_date AND begin_date <= end_date)
    /* -------------------------------------------------------------------------------------------------------
    Due to some incorrect movement codes some duplicate claims are still slipping through (particularly co30)
    Adding another step to flag problem claims
    --------------------------------------------------------------------------------------------------------- */
    ,final_claim_cleanup
     AS 
        (SELECT CASE 
                    WHEN SUM(CASE WHEN movement_code_clean = '30' AND current_termination != 0 AND direction_of_movement = -1 THEN 1 ELSE 0 END)
                            OVER (PARTITION BY company_code, policy_number, life_number
                                  ORDER BY effective_date_of_change_movement, movementcounter, end_date
                                  ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) >= 1
                    THEN 0
                    ELSE 
                        CASE WHEN movement_code_clean = '30' AND current_termination != 0 AND direction_of_movement = -1 THEN 1 ELSE 0 END
                END                                                                                                                                             AS claim_flag
               ,exposure_calc.*
           FROM exposure_calc)
SELECT CASE
           WHEN calendar_year <= 2006 THEN '≤ 2005'
           ELSE CAST (2 * (calendar_year / 2) AS VARCHAR) || ' - ' || CAST (1 + 2 * (calendar_year / 2) AS VARCHAR) -- Note that 2 * (cy / 2) rounds down to multiple of 2 (integer arithmetic)
       END                                                                                                                                                      AS cy_grouped
      ,CAST (-1 + 6 * ((calendar_year + 1) / 6) AS VARCHAR) || ' - ' || CAST (4 + 6 * ((calendar_year + 1) / 6) AS VARCHAR)                                     AS cy_grouped2
      ,policy_year
      ,policy_number
      ,life_number
      ,CASE sex_code WHEN 1 THEN 'M' WHEN 2 THEN 'F' ELSE 'U' END                                                                                               AS sex
      ,CASE smoking_category WHEN 1 THEN 'S' WHEN 2 THEN 'NS' ELSE 'U' END                                                                                      AS smoking_status
      ,CASE accelerator_marker WHEN 1 THEN 'Fully accelerated' WHEN 2 THEN 'Partially accelerated' WHEN 3 THEN 'No accelerator' ELSE 'Unspecified' END          AS accelerator_status
      ,CASE COALESCE (underwriter_loadings, -99)
           WHEN 1 THEN 'EM loading 1% - 50%'
           WHEN 2 THEN 'EM loading 51% - 100%'
           WHEN 3 THEN 'EM loading 101% - 150%'
           WHEN 4 THEN 'EM loading 151% - 200%'
           WHEN 5 THEN 'EM loading more than 200%'
           WHEN 6 THEN 'Flat loading < 200 per mille'
           WHEN 7 THEN 'Flat loading > 200 per mille'
           WHEN 8 THEN 'Underwriting exclusion'
           WHEN 0 THEN 'Not Loaded'
           WHEN -99 THEN 'Unspecified'
           ELSE 'Invalid code'
       END                                                                                                                                                      AS underwriter_loadings
      ,CASE COALESCE (underwriter_loadings, -99) WHEN -99 THEN 'Unspecified' WHEN 0 THEN 'Standard Rates' ELSE 'Loaded' END                                     AS loaded_vs_standard
      ,CASE type_of_medical_underwriting WHEN 1 THEN 'Medical' WHEN 2 THEN 'Non-Medical' ELSE 'Unspecified' END                                                 AS type_of_underwriting
      ,preferred_underwriting_class
      ,CASE company_code
           WHEN 12
           THEN
               CAST (MOD (preferred_underwriting_class, 10) AS VARCHAR)
           WHEN 6
           THEN
               CASE MOD (preferred_underwriting_Class, 10)
                   WHEN 1 THEN 'Best/Professional'
                   WHEN 2 THEN 'Best/Professional'
                   WHEN 3 THEN 'Second Best'
                   WHEN 4 THEN 'Third Best'
                   WHEN 5 THEN 'Worst'
                   ELSE 'Unknown'
               END
           ELSE
               CASE MOD (preferred_underwriting_Class, 10)
                   WHEN 1 THEN 'Best/Professional'
                   WHEN 2 THEN 'Second Best'
                   WHEN 3 THEN 'Third Best'
                   WHEN 4 THEN 'Worst'
                   ELSE 'Unknown'
               END
       END                                                                                                                                                      AS se_class
      ,CASE type_of_assurance
           WHEN 1 THEN 'Term Assurance'
           WHEN 2 THEN 'Retirement Annuities'
           WHEN 3 THEN 'Whole Life'
           WHEN 4 THEN 'Endowment Assurance'
           ELSE 'Unspecified'
       END                                                                                                                                                      AS type_of_assurance
      ,CASE is_new_generation WHEN 1 THEN 'New Gen' WHEN 2 THEN 'Not New Gen' ELSE 'Unspecified' END                                                            AS is_new_generation
      ,CASE special_offer_marker WHEN 1 THEN 'Special Offer' WHEN 2 THEN 'Not Special Offer' ELSE 'Unspecified' END                                             AS special_offer_marker
      ,CASE province
           WHEN 1 THEN 'Gauteng'
           WHEN 2 THEN 'Northern Province'
           WHEN 3 THEN 'Mpumalanga'
           WHEN 4 THEN 'North West'
           WHEN 5 THEN 'Kwa-Zulu Natal'
           WHEN 6 THEN 'Eastern Cape'
           WHEN 7 THEN 'Western Cape'
           WHEN 8 THEN 'Northern Cape'
           WHEN 9 THEN 'Free State'
           ELSE 'Unspecified'
       END                                                                                                                                                      AS province
      ,age_last_at_pa
      ,age_nrst_at_pa
      ,age_last_at_pa + 1                                                                                                                                       AS age_next_at_pa
      ,age_last_at_jan
      ,age_nrst_at_jan
      ,age_last_at_jan + 1                                                                                                                                      AS age_next_at_jan
      -- Adding spaces to these so they sort nicely on the charts:
      ,CASE 
           WHEN age_last_at_pa < 20 
           THEN 
               LPAD('', 26, ' ') || '0 - 19'
           WHEN age_last_at_pa >= 70
           THEN 
               '70+'
           ELSE
               LPAD('', 26 - (age_last_at_pa / 5), ' ') || CAST (5 * (age_last_at_pa / 5) AS VARCHAR) || ' - ' || CAST (4 + 5 * (age_last_at_pa / 5) AS VARCHAR)
       END                                                                                                                                                      AS age_last_band
      ,CASE 
           WHEN age_nrst_at_pa < 20 
           THEN 
               LPAD('', 26, ' ') || '0 - 19'
           WHEN age_nrst_at_pa >= 70
           THEN 
               '70+'
           ELSE
               LPAD('', 26 - (age_nrst_at_pa / 5), ' ') || CAST (5 * (age_nrst_at_pa / 5) AS VARCHAR) || ' - ' || CAST (4 + 5 * (age_nrst_at_pa / 5) AS VARCHAR)
       END                                                                                                                                                      AS age_nrst_band
      ,CASE 
           WHEN age_last_at_pa < 19 
           THEN 
               LPAD('', 26, ' ') || '0 - 19'
           WHEN age_last_at_pa >= 69
           THEN 
               '70+'
           ELSE
               LPAD('', 26 - (age_last_at_pa / 5), ' ') || CAST (5 * ((age_last_at_pa + 1) / 5) AS VARCHAR) || ' - ' || CAST (4 + 5 * ((age_last_at_pa + 1) / 5) AS VARCHAR)
       END                                                                                                                                                      AS age_next_band
      ,policy_date_of_entry
      ,EXTRACT (YEAR FROM policy_date_of_entry)                                                                                                                 AS issue_year
      ,policy_duration
      ,LEAST (policy_duration, 2)                                                                                                                               AS duration2
      ,LEAST (policy_duration, 3)                                                                                                                               AS duration3
      ,LEAST (policy_duration, 5)                                                                                                                               AS duration5
      ,begin_date
      ,end_date
      ,effective_date_of_change_movement
      ,movementcounter
      ,LPAD (movement_code_clean, 3, '0')                                                                                                                       AS change_in_movement_code
      ,sum_assured_in_rand                                                                                                                                      AS sum_assured
      -- We need at least one day of exposure to be associated with each claim record, otherwise standardisation becomes really hard:
      ,GREATEST (exposure_days, claim_flag)                                                                                                                     AS exposure_days
      ,GREATEST (exposure_days, claim_flag) / 365.25                                                                                                            AS expyearscen
      ,GREATEST (exposure_days, claim_flag)  / CAST (days_in_year AS DOUBLE)                                                                                    AS expyearscen_exact
      ,sum_assured_in_rand * GREATEST (exposure_days, claim_flag) / 365.25                                                                                      AS aar_weighted_exposure
      ,sum_assured_in_rand * GREATEST (exposure_days, claim_flag) / CAST (days_in_year AS DOUBLE)                                                               AS aar_weighted_exposure_exact
      ,claim_flag                                                                                                                                               AS actual_claim_cnt
      ,claim_flag * sum_assured_in_rand                                                                                                                         AS actual_claim_amt
      ,CASE
           WHEN claim_flag = 1 
           THEN
               CASE cause_of_death
                   WHEN 1 THEN 'Cancer'
                   WHEN 2 THEN 'Cardiac'
                   WHEN 3 THEN 'Cerebrovascular'
                   WHEN 4 THEN 'Respiratory Disease'
                   WHEN 5 THEN 'Accidental / Violent'
                   WHEN 6 THEN 'Definitely AIDS'
                   WHEN 7 THEN 'Suspected AIDS'
                   WHEN 8 THEN 'Multi-Organ failure'
                   WHEN 9 THEN 'Other (known, unlisted)'
                   WHEN 10 THEN 'Non-specific Non-natural'
                   WHEN 11 THEN 'Non-specific Natural'
                   WHEN 0 THEN 'Unspecified'
                   ELSE 'Unknown cause code: ' || CAST (cause_of_death AS VARCHAR)
               END
           ELSE
               NULL
       END                                                                                                                                                      AS cause_of_death
      ,calendar_year
      ,company_code
  FROM final_claim_cleanup
 WHERE exposure_days >= 0 AND policy_duration >= 0; 
 
/* ------------------------------------------------------------------------------------------------------------------------------------------------
    Now we convert company 18's data to a format consistent to be appended to the rest
   ----------------------------------------------------------------------------------------------------------------------------------------------- */
DROP TABLE IF EXISTS assa_sandbox.assa_new_gen_exposure_18;

CREATE TABLE assa_sandbox.assa_new_gen_exposure_18
    WITH (
            format = 'ORC'
            ,orc_compression = 'ZLIB'
            ,partitioned_by = ARRAY ['calendar_year', 'company_code']
            ,bucketed_by = ARRAY ['policy_year','sex']
            ,bucket_count = 25
            ) AS
WITH
    age_offsets(offset)
    AS
        (SELECT 0
         UNION ALL
         SELECT 1),
    dib_to_use 
    AS  
        (SELECT MAX(data_import_batch) AS dib_to_use
           FROM "assa-lake".v1_mortality_co18),
    ages_fix
    AS
        (SELECT dat.*, CAST(age_last AS BIGINT) AS age_last_fixed, CAST(age_last + age_offsets.offset AS BIGINT) AS age_nearest_fixed
           FROM "assa-lake".v1_mortality_co18 dat
          INNER JOIN dib_to_use ON dat.data_import_batch = dib_to_use.dib_to_use, age_offsets
          WHERE age_nearest IS NULL
         UNION ALL
         SELECT dat.*, CAST(age_nearest - age_offsets.offset AS BIGINT) AS age_last_fixed, CAST(age_nearest AS BIGINT) AS age_nearest_fixed
           FROM "assa-lake".v1_mortality_co18 dat
          INNER JOIN dib_to_use ON dat.data_import_batch = dib_to_use.dib_to_use, age_offsets
          WHERE age_last IS NULL)
SELECT CASE
           WHEN year_of_invest <= 2006 THEN '≤ 2005'
           ELSE CAST (2 * (year_of_invest / 2) AS VARCHAR) || ' - ' || CAST (1 + 2 * (year_of_invest / 2) AS VARCHAR)
       -- Note that 2 * (cy / 2) rounds down to multiple of 2 (integer arithmetic)
       END                                                                                                                                                AS cy_grouped
      ,CAST (-1 + 6 * ((year_of_invest + 1) / 6) AS VARCHAR) || ' - ' || CAST (4 + 6 * ((year_of_invest + 1) / 6) AS VARCHAR)                             AS cy_grouped2
      ,year_of_invest                                                                                                                                     AS policy_year
      ,CAST(NULL AS VARCHAR)                                                                                                                              AS policy_number
      ,CAST(NULL AS VARCHAR)                                                                                                                              AS life_number
      ,CASE sex_code WHEN 1 THEN 'M' WHEN 2 THEN 'F' ELSE 'U' END                                                                                         AS sex
      ,CASE smoking_category WHEN 1 THEN 'S' WHEN 2 THEN 'NS' ELSE 'U' END                                                                                AS smoking_status
      ,CASE accelerator_marker WHEN 1 THEN 'Fully accelerated' WHEN 2 THEN 'Partially accelerated' WHEN 3 THEN 'No accelerator' ELSE 'Unspecified' END    AS accelerator_status
      ,CASE COALESCE(underwriter_loadings,-99)
           WHEN 1 THEN 'EM loading 1% - 50%'
           WHEN 2 THEN 'EM loading 51% - 100%'
           WHEN 3 THEN 'EM loading 101% - 150%'
           WHEN 4 THEN 'EM loading 151% - 200%'
           WHEN 5 THEN 'EM loading more than 200%'
           WHEN 6 THEN 'Flat loading < 200 per mille'
           WHEN 7 THEN 'Flat loading > 200 per mille'
           WHEN 8 THEN 'Underwriting exclusion'
           WHEN 0 THEN 'Not Loaded'
           WHEN -99 THEN 'Unspecified'
           ELSE 'Invalid code'
       END                                                                                                                                                AS underwriter_loadings
      ,CASE COALESCE(underwriter_loadings,-99) WHEN -99 THEN 'Unspecified' WHEN 0 THEN 'Standard Rates' ELSE 'Loaded' END                                 AS loaded_vs_standard
      ,CASE type_of_medical_underwriting WHEN 1 THEN 'Medical' WHEN 2 THEN 'Non-Medical' ELSE 'Unspecified' END                                           AS type_of_underwriting
      ,preferred_underwriting_class
      ,CASE MOD (preferred_underwriting_Class, 10)
           WHEN 1 THEN 'Best/Professional'
           WHEN 2 THEN 'Second Best'
           WHEN 3 THEN 'Third Best'
           WHEN 4 THEN 'Worst'
           ELSE 'Unknown'
       END                                                                                                                                                AS se_class
      ,CASE type_of_assurance
           WHEN 1 THEN 'Term Assurance'
           WHEN 2 THEN 'Retirement Annuities'
           WHEN 3 THEN 'Whole Life'
           WHEN 4 THEN 'Endowment Assurance'
           ELSE 'Unspecified'
       END                                                                                                                                                AS type_of_assurance
      ,CASE is_new_generation WHEN 1 THEN 'New Gen' WHEN 2 THEN 'Not New Gen' ELSE 'Unspecified' END                                                      AS is_new_generation
      ,CASE special_offer_marker WHEN 1 THEN 'Special Offer' WHEN 2 THEN 'Not Special Offer' ELSE 'Unspecified' END                                       AS special_offer_marker
      ,CASE province
           WHEN 1 THEN 'Gauteng'
           WHEN 2 THEN 'Northern Province'
           WHEN 3 THEN 'Mpumalanga'
           WHEN 4 THEN 'North West'
           WHEN 5 THEN 'Kwa-Zulu Natal'
           WHEN 6 THEN 'Eastern Cape'
           WHEN 7 THEN 'Western Cape'
           WHEN 8 THEN 'Northern Cape'
           WHEN 9 THEN 'Free State'
           ELSE 'Unspecified'
       END                                                                                                                                                AS province
      ,age_last_fixed                                                                                                                                     AS age_last_at_pa
      ,age_nearest_fixed                                                                                                                                  AS age_nrst_at_pa
      ,age_last_fixed + 1                                                                                                                                 AS age_next_at_pa
      ,age_last_fixed                                                                                                                                     AS age_last_at_jan
      ,age_nearest_fixed                                                                                                                                  AS age_nrst_at_jan
      ,age_last_fixed + 1                                                                                                                                 AS age_next_at_jan
      ,CASE 
           WHEN age_last_fixed < 20 
           THEN 
               LPAD('', 26, ' ') || '0 - 19'
           WHEN age_last_fixed >= 70
           THEN 
               '70+'
           ELSE
               LPAD('', 26 - (age_last_fixed / 5), ' ') || CAST (5 * (age_last_fixed / 5) AS VARCHAR) || ' - ' || CAST (4 + 5 * (age_last_fixed / 5) AS VARCHAR)
       END                                                                                                                                                      AS age_last_band
      ,CASE 
           WHEN age_nearest_fixed < 20 
           THEN 
               LPAD('', 26, ' ') || '0 - 19'
           WHEN age_nearest_fixed >= 70
           THEN 
               '70+'
           ELSE
               LPAD('', 26 - (age_nearest_fixed / 5), ' ') || CAST (5 * (age_nearest_fixed / 5) AS VARCHAR) || ' - ' || CAST (4 + 5 * (age_nearest_fixed / 5) AS VARCHAR)
       END                                                                                                                                                      AS age_nrst_band
      ,CASE 
           WHEN age_last_fixed < 19 
           THEN 
               LPAD('', 26, ' ') || '0 - 19'
           WHEN age_last_fixed >= 69
           THEN 
               '70+'
           ELSE
               LPAD('', 26 - (age_last_fixed / 5), ' ') || CAST (5 * ((age_last_fixed + 1) / 5) AS VARCHAR) || ' - ' || CAST (4 + 5 * ((age_last_fixed + 1) / 5) AS VARCHAR)
       END                                                                                                                                                      AS age_next_band
      ,CAST(NULL AS DATE)                                                                                                                                 AS policy_date_of_entry
      ,year_of_invest - duration                                                                                                                          AS issue_year
      ,duration                                                                                                                                           AS policy_duration
      ,LEAST (duration, 2)                                                                                                                                AS duration2
      ,LEAST (duration, 3)                                                                                                                                AS duration3
      ,LEAST (duration, 5)                                                                                                                                AS duration5
      ,from_iso8601_date (CAST (year_of_invest AS VARCHAR) || '-01-01')                                                                                   AS begin_date
      ,from_iso8601_date (CAST (year_of_invest AS VARCHAR) || '-12-31')                                                                                   AS end_date
      ,CAST(NULL AS DATE)                                                                                                                                 AS effective_date_of_change_movement
      ,CAST(NULL AS BIGINT)                                                                                                                               AS movementcounter
      ,'999'                                                                                                                                              AS change_in_movement_code
      ,CAST(NULL AS DOUBLE)                                                                                                                               AS sum_assured
      ,exp_days_cen / 2.0                                                                                                                                 AS exposure_days
      ,exp_years_cen / 2.0                                                                                                                                AS expyearscen
      ,exp_years_cen / 2.0                                                                                                                                AS expyearscen_exact
      ,sa_exposure / 2.0                                                                                                                                  AS aar_weighted_exposure
      ,sa_exposure / 2.0                                                                                                                                  AS aar_weighted_exposure_exact
      ,no_of_deaths / 2.0                                                                                                                                 AS actual_claim_cnt
      ,sa_no_of_deaths / 2.0                                                                                                                              AS actual_claim_amt
      ,'Unspecified'                                                                                                                                      AS cause_of_death
      ,year_of_invest                                                                                                                                     AS calendar_year
      ,company_code
  FROM ages_fix
 INNER JOIN assa_sandbox.csi_mort_params ON csi_mort_params.param_name = 'exposure_end_date'
 WHERE     exp_days_cen >= 0 
       AND duration >= 0 
       AND year_of_invest < EXTRACT(YEAR FROM DATE(param_value)) 
       AND is_new_generation = 1;