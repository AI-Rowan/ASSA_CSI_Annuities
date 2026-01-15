/* ---------------------------------------------------------------------------------------------------------------------
    Given multiple versions of data submissions that have been made, we need to be able to tell the system
    which are old versions that should be ignored
   --------------------------------------------------------------------------------------------------------------------- */

DROP TABLE IF EXISTS mortality_sandbox.assa_new_gen_data_exclusions;

-- run the code in the Comodash online tool

CREATE TABLE mortality_sandbox.assa_new_gen_data_exclusions 
with (
  -- priority = 1,
  bucketed_by = array[],
  bucket_count = 0,
  partitioned_by = array[]
)
AS
SELECT * FROM 
    (VALUES (25, CAST(NULL AS VARCHAR), '2019-12-04'),
            (6, 'COMBINED_outfile_old_and_new_gen_6_2014.csv', '2020-05-15')) AS t(company_code, filename, data_import_batch)
;


/* ---------------------------------------------------------------------------------------------------------------------
    co25 SEC data is provided in the form of a separate income and education table.
    We need to first do a mapping step because the education is provided differently from the main movements data
    There are often multiple records for a single movement date in this income/edu table. 
        I assume that the highest SE class should be used in those cases

    The resulting SE classes are assumed to be unchanged between movement dates provided.
   --------------------------------------------------------------------------------------------------------------------- */
DROP TABLE IF EXISTS mortality_sandbox.co25_secs_mapped;

CREATE TABLE mortality_sandbox.co25_secs_mapped
    WITH (  -- priority = 1,
            format = 'ORC',
            orc_compression = 'ZLIB',
            partitioned_by = ARRAY ['country'],
            bucketed_by = ARRAY ['life_number','movement_date_min'],
            bucket_count = 50
            ) AS
WITH
    sec_mapping_table
    AS
        (
         SELECT t1.calendar_year                                                                                                                                                    apply_year
               ,t1.qualification                                                                                                                                                    edu_original
               ,t2.source_company_category                                                                                                                                          education
               ,t1.income_from                                                                                                                                                      income_min
               ,MIN (t1.income_from)
                    OVER (PARTITION BY t1.calendar_year, t1.qualification, t2.source_company_category ORDER BY t1.income_from ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)     income_max
               ,(105 - t1.irp_class)                                                                                                                                                sec_to_use -- SEs in table are backwards relative to what we need. We add 100 as spec expects SEs 100+ with first digit representing version
           FROM "mortality_lake".v1_co25_sec_mapping_table t1 INNER JOIN "mortality_lake".v1_co25_sec_translation_table t2 ON t1.qualification = t2.csi_category),
    inc_edu_clean
    AS
        (SELECT country
               ,life_number
               ,CAST( COALESCE (try (date_parse (CAST (movement_date AS VARCHAR), '%Y%m%d')), date_parse (CAST (movement_date AS VARCHAR), '%Y%m')) AS DATE)    movement_date
               ,gross_income
               ,education
           FROM "mortality_lake".v1_co25_newgen_se_data),
    translated_secs
    AS
        (SELECT inc_edu_clean.country
               ,inc_edu_clean.life_number
               ,inc_edu_clean.movement_date
               ,inc_edu_clean.gross_income
               ,inc_edu_clean.education
               ,sec_mapping_table.sec_to_use
           FROM inc_edu_clean
                LEFT JOIN sec_mapping_table
                    ON     CASE
                               WHEN EXTRACT (YEAR FROM inc_edu_clean.movement_date) > 2020 THEN 2020
                               WHEN EXTRACT (YEAR FROM inc_edu_clean.movement_date) < 2003 THEN 2003
                               ELSE EXTRACT (YEAR FROM inc_edu_clean.movement_date)
                           END =
                           sec_mapping_table.apply_year
                       AND inc_edu_clean.gross_income < COALESCE (sec_mapping_table.income_max, 9E16)
                       AND inc_edu_clean.gross_income >= sec_mapping_table.income_min
                       AND inc_edu_clean.education = sec_mapping_table.education)
  SELECT CAST(life_number AS VARCHAR)																											 life_number
        ,movement_date                                                                                                                           movement_date_min
        ,MIN (movement_date) OVER (PARTITION BY country, life_number ORDER BY movement_date ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)    movement_date_max
        ,MAX (sec_to_use)                                                                                                                        sec
        ,country
    FROM translated_secs
GROUP BY country, life_number, movement_date
;


/* ---------------------------------------------------------------------------------------------------------------------
    We create the exposure table based on the data provided by most companies (all except company 18)
    There are a number of assumptions made to clean up the data
   --------------------------------------------------------------------------------------------------------------------- */

DROP TABLE IF EXISTS mortality_sandbox.assa_new_gen_movement;

CREATE TABLE mortality_sandbox.assa_new_gen_movement
    WITH (  -- priority = 2,
            format = 'ORC',
            orc_compression = 'ZLIB',
            partitioned_by = ARRAY ['sourcefilename', 'company_code','year_of_data'],
            bucketed_by = ARRAY ['effective_date_of_change_movement','policy_number'],
            bucket_count = 25
            ) AS
WITH
    dib_to_use
    AS
        (  SELECT company_code
                 ,year_of_data
                 ,is_new_generation
                 ,MAX (substr (process_time_stamp, 1, 10))     AS dib_to_use -- This would have been data_import_batch but v2 table overwrote that with a single date for all files. process_time_stamp is equivalent, but with times added.
             FROM "mortality_lake".v2_assa_movement
         GROUP BY company_code, year_of_data, is_new_generation)
SELECT CASE
           WHEN v2_assa_movement.company_code = 6 AND v2_assa_movement.year_of_data <= 2013
           THEN
               COALESCE (co6_pn_mappings.policy_number, v2_assa_movement.policy_number)
           ELSE
               v2_assa_movement.policy_number
       END                                                                                                             AS policy_number
      , /*COALESCE(c25_life_data.life_number_to_use, COALESCE(TRY_CAST(TRY_CAST(v2_assa_movement.life_number AS INTEGER) AS VARCHAR), v2_assa_movement.
               life_number)) AS life_number*/
       COALESCE (TRY_CAST (TRY_CAST (v2_assa_movement.life_number AS INTEGER) AS VARCHAR), v2_assa_movement.life_number)       AS life_number
      ,change_in_movement_code
      ,CASE
           WHEN TRIM (change_in_movement_code) IN ('X0'
                                                  ,'XNULL'
                                                  ,'XXXX'
                                                  ,'X')
           THEN
               '00'                                                                                         -- Most of these bad cases seem to be new business
           ELSE
               SUBSTR (change_in_movement_code, -2)
       END                                                                                                             AS movement_code_clean
      ,CASE
           WHEN     EXTRACT (MONTH FROM date (effective_date_of_change_movement)) = 12
                AND EXTRACT (DAY FROM date (effective_date_of_change_movement)) = 31
                AND SUBSTR (change_in_movement_code, -2) = '10'
           THEN
               date_add ('day', 1, date (effective_date_of_change_movement))
           -- Just a minor cleanup where 31-Dec is being used for calendar year start
           ELSE
               date (effective_date_of_change_movement)
       END                                                                                                             AS effective_date_of_change_movement
      ,movementcounter
      ,direction_of_movement
      ,sum_assured_in_rand_before_movement
      ,sum_assured_in_rand_after_movement
      ,cause_of_death
      ,date (v2_assa_movement.policy_date_of_entry)                                                                    AS policy_date_of_entry
      --,DATE (COALESCE(c25_life_data.dob_to_use, v2_assa_movement.date_of_birth)) AS date_of_birth
      ,date (v2_assa_movement.date_of_birth)                                                                           AS date_of_birth
      ,sum_assured_in_rand
      ,sex_code
      ,type_of_assurance
      ,type_of_medical_underwriting
      ,smoking_category
      ,accelerator_marker
      ,province
      ,CASE WHEN v2_assa_movement.company_code = 25 THEN co25_secs_mapped.sec ELSE preferred_underwriting_class END    AS preferred_underwriting_class
      ,v2_assa_movement.is_new_generation
      ,special_offer_marker
      ,CASE
           WHEN COALESCE(underwriter_loadings, -99) = -99
           THEN
               CASE 
                   WHEN v2_assa_movement.company_code = 6 AND v2_assa_movement.year_of_data <= 2007 THEN 0
                   WHEN v2_assa_movement.company_code = 11 AND v2_assa_movement.year_of_data <= 2010 THEN 0
                   WHEN v2_assa_movement.company_code = 30 AND v2_assa_movement.year_of_data <= 2010 THEN 0
                   ELSE -99 
               END 
           ELSE 
               underwriter_loadings 
       END                                                                                                             AS underwriter_loadings              
      ,date_parse ( substr(process_time_stamp, 1, 19), '%Y-%m-%d %H:%i:%s')                                            AS process_time_stamp
      ,process_number
      ,sourcefilename
      ,v2_assa_movement.company_code
      ,v2_assa_movement.year_of_data
  FROM "mortality_lake".v2_assa_movement
       INNER JOIN dib_to_use
           ON     v2_assa_movement.year_of_data = dib_to_use.year_of_data
              AND v2_assa_movement.company_code = dib_to_use.company_code
              AND SUBSTRING (v2_assa_movement.process_time_stamp, 1, 10) = dib_to_use.dib_to_use
              AND v2_assa_movement.is_new_generation = dib_to_use.is_new_generation
       /*LEFT JOIN mortality_sandbox.assa_new_gen_data_exclusions excl ON v2_assa_movement.company_code = excl.company_code
                  AND v2_assa_movement.sourcefilename = COALESCE(excl.filename, v2_assa_movement.sourcefilename)
                 AND SUBSTRING (v2_assa_movement.process_time_stamp, 1, 10) = excl.data_import_batch*/
       /* -------------------------------------------------------------------------------------------------------------------------------------------------
          There are cases coming through for company 11 for which there is no exposure in 2003 - 2008,
          but then suddenly exposure in 2009-2011 despite the policies being issued prior to 2003.
          I am assuming that like similar cases these policies actually terminated prior to 2003, but then the system did something strange thereafter
          Seems there were only 28 cases so maybe didn't need to worry
          ------------------------------------------------------------------------------------------------------------------------------------------------- */
       LEFT JOIN (  SELECT company_code AS company_code_z, policy_number AS policy_number_z                /* ,life number -- life number is null for co 11 */
                      FROM "mortality_lake".v2_assa_movement
                     WHERE company_code = 11 AND date (policy_date_of_entry) < DATE '2003-01-01'
                  GROUP BY company_code, policy_number
                    HAVING MIN (year_of_data) = 2009) AS c11_check
           ON c11_check.company_code_z = v2_assa_movement.company_code AND c11_check.policy_number_z = v2_assa_movement.policy_number
       /* -------------------------------------------------------------------------------------------------------------------------------------------------
          Company 6 changed admin systems in 2014, and the format of their policy numbers changed with it
          We use a mapping table provided by them to convert old policy numbers to new format
          Note that mappings only exist for policies that were inforce both before and after the changeover
          ------------------------------------------------------------------------------------------------------------------------------------------------- */
       LEFT JOIN "mortality_lake".v1_co6_policy_number_mapping co6_pn_mappings
           ON CAST (co6_pn_mappings.mapped_policy_number AS VARCHAR) = v2_assa_movement.policy_number AND v2_assa_movement.company_code = 6
       /* -------------------------------------------------------------------------------------------------------------------------------------------------
          For company 25 the format of the life numbers changed around 2012
          Prior to that the DOBs were also wrong.
          Here we try to correct as far as we can by trying to calculate a uniquely identifiable life number and getting the latest DOB
          UPDATE: This seems to have been corrected in newer version of data so have disabled
          ------------------------------------------------------------------------------------------------------------------------------------------------- */
       /*LEFT JOIN (
           SELECT DISTINCT policy_number
               ,life_number
               ,date_of_birth
               ,policy_date_of_entry
               ,coalesce(regexp_extract(life_number, policy_number || '_(\d*)', 1), regexp_extract(life_number, '^(\d{1,3})_1$', 1)) AS life_number_to_use
               ,last_value(date_of_birth) OVER (
                   PARTITION BY policy_number
                   ,coalesce(regexp_extract(life_number, policy_number || '_(\d*)', 1), regexp_extract(life_number, '^(\d{1,3})_1$', 1)) ORDER BY DATE (effective_date_of_change_movement
                           ) RANGE BETWEEN UNBOUNDED PRECEDING
                           AND UNBOUNDED FOLLOWING
                   ) AS dob_to_use
           FROM "mortality_lake".v2_assa_movement
           WHERE company_code = 25
           ORDER BY policy_number
               ,coalesce(regexp_extract(life_number, policy_number || '_(\d*)', 1), regexp_extract(life_number, '^(\d{1,3})_1$', 1))
           ) AS c25_life_data ON c25_life_data.policy_number = v2_assa_movement.policy_number
           AND c25_life_data.date_of_birth = v2_assa_movement.date_of_birth
           AND c25_life_data.policy_date_of_entry = v2_assa_movement.policy_date_of_entry
           AND c25_life_data.life_number = v2_assa_movement.life_number
           AND v2_assa_movement.company_code = 25*/
       LEFT JOIN mortality_sandbox.co25_secs_mapped
           ON     v2_assa_movement.life_number = co25_secs_mapped.life_number
              AND date (effective_date_of_change_movement) < COALESCE (movement_date_max, DATE '2999-12-31')
              AND date (effective_date_of_change_movement) >= movement_date_min
              AND co25_secs_mapped.country = 'South Africa'
              AND v2_assa_movement.company_code = 25
 WHERE c11_check.policy_number_z IS NULL AND v2_assa_movement.is_new_generation = 1
--AND excl.data_import_batch IS NULL
;


/* ------------------------------------------------------------------------------------------------------------------------------------------------
    Next table is the SA85-90 rates
   ----------------------------------------------------------------------------------------------------------------------------------------------- */
DROP TABLE IF EXISTS mortality_sandbox.mortality_sa8590;
CREATE TABLE mortality_sandbox.mortality_sa8590
    WITH (  -- priority = 1,
            format = 'ORC',
            orc_compression = 'ZLIB',
            partitioned_by = ARRAY ['duration'],
            bucketed_by = ARRAY ['age'],
            bucket_count = 10
            )
AS
SELECT age
    ,mortality_rate_qx
    ,force_of_mortality_mux
    ,duration
FROM "mortality_lake".mortality_sa8590
;

/* ------------------------------------------------------------------------------------------------------------------------------------------------
    This creates a table containing parameters for the experience calculation
    > exposure_end_date : the cutoff date for the exposure calculation
    ----------------------------------------------------------------------------------------------------------------------------------------------- */
DROP TABLE IF EXISTS mortality_sandbox.csi_mort_params;

CREATE TABLE mortality_sandbox.csi_mort_params
    WITH (  -- priority = 1,
            format = 'ORC',
            orc_compression = 'ZLIB',
            partitioned_by = ARRAY ['param_name']
            ) AS

SELECT *
FROM (
    VALUES (
        '2020-01-01'
        ,'exposure_end_date'
        ),
        (
        '2013',
        'standard_year'
        )
    ) AS t(param_value, param_name);
