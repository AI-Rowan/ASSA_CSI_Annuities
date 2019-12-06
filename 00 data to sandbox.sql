CREATE TABLE assa_sandbox.v1_assa_movement
	WITH (
			format = 'ORC'
			,orc_compression = 'ZLIB'
			,partitioned_by = ARRAY ['sourcefilename', 'company_code','year_of_data']
			,bucketed_by = ARRAY ['effective_date_of_change_movement','policy_number']
			,bucket_count = 25
			) AS
SELECT policy_number
	,life_number
	,change_in_movement_code
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
	,DATE_PARSE (process_time_stamp, '%Y-%m-%d %H:%i:%s') AS process_time_stamp
	,process_number
	,sourcefilename
	,company_code
	,year_of_data
FROM "assa-lake".v1_assa_movement;
