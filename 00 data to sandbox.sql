DROP TABLE assa_sandbox.v1_assa_movement;

CREATE TABLE assa_sandbox.v1_assa_movement
	WITH (
			format = 'ORC'
			,orc_compression = 'ZLIB'
			,partitioned_by = ARRAY ['sourcefilename', 'company_code','year_of_data']
			,bucketed_by = ARRAY ['effective_date_of_change_movement','policy_number']
			,bucket_count = 25
			) AS

SELECT policy_number
	,COALESCE(TRY_CAST(TRY_CAST(life_number AS INTEGER) AS VARCHAR), life_number) AS life_number
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
	,DATE (effective_date_of_change_movement) AS effective_date_of_change_movement
	,movementcounter
	,direction_of_movement
	,sum_assured_in_rand_before_movement
	,sum_assured_in_rand_after_movement
	,cause_of_death
	,DATE (policy_date_of_entry) AS policy_date_of_entry
	,DATE (date_of_birth) AS date_of_birth
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
	SELECT company_code as company_code_z
		,policy_number as policy_number_z /* ,life number -- life number is null for co 11 */
	FROM "assa-lake".v1_assa_movement
	WHERE company_code = 11
		AND DATE (policy_date_of_entry) < DATE '2003-01-01'
	GROUP BY company_code
		,policy_number
	HAVING min(year_of_data) = 2009
	) AS c11_check ON c11_check.company_code_z = v1_assa_movement.company_code
	AND c11_check.policy_number_z = v1_assa_movement.policy_number
WHERE c11_check.policy_number_z IS NULL;

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
