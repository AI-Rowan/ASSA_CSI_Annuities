/* ---------------------------------------------------------------------------------------------------------------------
	Given multiple versions of data submissions that have been made, we need to be able to tell the system
	which are old versions that should be ignored
   --------------------------------------------------------------------------------------------------------------------- */

DROP TABLE IF EXISTS assa_sandbox.assa_new_gen_data_exclusions;

CREATE TABLE assa_sandbox.assa_new_gen_data_exclusions (company_code, year_of_data, data_import_batch) AS 
SELECT * FROM 
	(VALUES (25, NULL, '2019-12-04'))


/* ---------------------------------------------------------------------------------------------------------------------
	We create the exposure table based on the data provided by most companies (all except company 18)
	There are a number of assumptions made to clean up the data
   --------------------------------------------------------------------------------------------------------------------- */

DROP TABLE IF EXISTS assa_sandbox.v1_assa_movement;

CREATE TABLE assa_sandbox.v1_assa_movement
	WITH (
			format = 'ORC'
			,orc_compression = 'ZLIB'
			,partitioned_by = ARRAY ['sourcefilename', 'company_code','year_of_data']
			,bucketed_by = ARRAY ['effective_date_of_change_movement','policy_number']
			,bucket_count = 25
			) AS
SELECT v1_assa_movement.policy_number
	,COALESCE(c25_life_data.life_number_to_use, COALESCE(TRY_CAST(TRY_CAST(v1_assa_movement.life_number AS INTEGER) AS VARCHAR), v1_assa_movement.
			life_number)) AS life_number
	,change_in_movement_code
	,CASE 
		WHEN TRIM(change_in_movement_code) IN (
				'X0'
				,'XNULL'
				,'XXXX'
				,'X'
				)
			THEN '00' -- Most of these bad cases seem to be new business
		ELSE SUBSTR(change_in_movement_code, - 2)
		END AS movement_code_clean
	,CASE 
		WHEN extract(MONTH FROM DATE (effective_date_of_change_movement)) = 12
			AND extract(DAY FROM DATE (effective_date_of_change_movement)) = 31
			AND SUBSTR(change_in_movement_code, - 2) = '10'
			THEN date_add('day', 1, DATE (effective_date_of_change_movement))
				-- Just a minor cleanup where 31-Dec is being used for calendar year start
		ELSE DATE (effective_date_of_change_movement)
		END AS effective_date_of_change_movement
	,movementcounter
	,direction_of_movement
	,sum_assured_in_rand_before_movement
	,sum_assured_in_rand_after_movement
	,cause_of_death
	,DATE (v1_assa_movement.policy_date_of_entry) AS policy_date_of_entry
	,DATE (COALESCE(c25_life_data.dob_to_use, v1_assa_movement.date_of_birth)) AS date_of_birth
	,sum_assured_in_rand
	,sex_code
	,type_of_assurance
	,type_of_medical_underwriting
	,smoking_category
	,accelerator_marker
	,province
	,preferred_underwriting_class
	,is_new_generation
	,special_offer_marker
	,underwriter_loadings
	,DATE_PARSE(process_time_stamp, '%Y-%m-%d %H:%i:%s') AS process_time_stamp
	,process_number
	,sourcefilename
	,company_code
	,year_of_data
FROM "assa-lake".v1_assa_movement
/* -------------------------------------------------------------------------------------------------------------------------------------------------
   There are cases coming through for company 11 for which there is no exposure in 2003 - 2008, 
   but then suddenly exposure in 2009-2011 despite the policies being issued prior to 2003.
   I am assuming that like similar cases these policies actually terminated prior to 2003, but then the system did something strange thereafter
   Seems there were only 28 cases so maybe didn't need to worry
   ------------------------------------------------------------------------------------------------------------------------------------------------- */
LEFT JOIN (
	SELECT company_code AS company_code_z
		,policy_number AS policy_number_z /* ,life number -- life number is null for co 11 */
	FROM "assa-lake".v1_assa_movement
	WHERE company_code = 11
		AND DATE (policy_date_of_entry) < DATE '2003-01-01'
	GROUP BY company_code
		,policy_number
	HAVING min(year_of_data) = 2009
	) AS c11_check ON c11_check.company_code_z = v1_assa_movement.company_code
	AND c11_check.policy_number_z = v1_assa_movement.policy_number
/* -------------------------------------------------------------------------------------------------------------------------------------------------
   For company 25 the format of the life numbers changed around 2012
   Prior to that the DOBs were also wrong. 
   Here we try to correct as far as we can by trying to calculate a uniquely identifiable life number and getting the latest DOB
   ------------------------------------------------------------------------------------------------------------------------------------------------- */
LEFT JOIN (
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
	FROM "assa-lake".v1_assa_movement
	WHERE company_code = 25
	ORDER BY policy_number
		,coalesce(regexp_extract(life_number, policy_number || '_(\d*)', 1), regexp_extract(life_number, '^(\d{1,3})_1$', 1))
	) AS c25_life_data ON c25_life_data.policy_number = v1_assa_movement.policy_number
	AND c25_life_data.date_of_birth = v1_assa_movement.date_of_birth
	AND c25_life_data.policy_date_of_entry = v1_assa_movement.policy_date_of_entry
	AND c25_life_data.life_number = v1_assa_movement.life_number
	AND v1_assa_movement.company_code = 25
WHERE c11_check.policy_number_z IS NULL
  AND is_new_generation = 1;

/* ------------------------------------------------------------------------------------------------------------------------------------------------
    Next table is the SA85-90 rates
   ----------------------------------------------------------------------------------------------------------------------------------------------- */
CREATE TABLE assa_sandbox.mortality_sa8590
	WITH (
			format = 'ORC'
			,orc_compression = 'ZLIB'
			,partitioned_by = ARRAY ['duration']
			,bucketed_by = ARRAY ['age']
			,num_buckets = 10
			)
AS
SELECT age
	,mortality_rate_qx
	,force_of_mortality_mux
	,duration
FROM "assa-lake".mortality_sa8590;

/* ------------------------------------------------------------------------------------------------------------------------------------------------
    This creates a table containing parameters for the experience calculation
	> exposure_end_date : the cutoff date for the exposure calculation
	----------------------------------------------------------------------------------------------------------------------------------------------- */
CREATE TABLE assa_sandbox.csi_mort_params
	WITH (
			format = 'ORC'
			,orc_compression = 'ZLIB'
			,partitioned_by = ARRAY ['param_name']
			) AS

SELECT *
FROM (
	VALUES (
		'2014-01-01'
		,'exposure_end_date'
		)
	) AS t(param_value, param_name);
