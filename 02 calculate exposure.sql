--CREATE TABLE assa_sandbox.assa_exposure 
--AS
/* -------------------------------------------------------------------------------------------------------
   First we calculate a list of all the years that we will have exposure for. 
   Note that we have to calculate each possible combination of calendar year and policy year 
   e.g. 2005 calendar year will be split between 2004 and 2005 policy years
   Easiest to get list of potential policy years and get result of adding 0 and 1 to get calendar years
   -------------------------------------------------------------------------------------------------------*/
WITH exposure_years
AS (
	SELECT y.policy_year
		,y.policy_year + o.offset AS calendar_year
	FROM (
		SELECT sequence(MIN(EXTRACT(YEAR FROM effective_date_of_change_movement)) - 1, MAX(EXTRACT(YEAR FROM effective_date_of_change_movement)))
		FROM assa_sandbox.v1_assa_movement
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
   -------------------------------------------------------------------------------------------------------*/
	,termination_check
AS (
	SELECT sum(CASE 
				WHEN movement_code_clean IN (
						'30'
						,'43'
						,'44'
						,'50'
						)
					THEN direction_of_movement
				ELSE 0
				END) OVER (
			PARTITION BY company_code
			,policy_number
			,life_number ORDER BY effective_date_of_change_movement
				,movementcounter rows BETWEEN unbounded preceding
					AND 1 preceding
			) AS prior_termination
		,CASE 
			WHEN movement_code_clean IN (
					'30'
					,'43'
					,'44'
					,'50'
					)
				THEN - SUM(direction_of_movement) OVER (
						PARTITION BY company_code
						,policy_number
						,life_number
						,effective_date_of_change_movement
						)
			ELSE 0
			END AS current_termination
		,v1_assa_movement.*
	FROM assa_sandbox.v1_assa_movement
	)
	/* -------------------------------------------------------------------------------------------------------
   Next we need to find the next movement for this policy so that we know the end of the exposure period
   -------------------------------------------------------------------------------------------------------*/
	,calc_next_movement
AS (
	SELECT
		-- try to find next movement date for this policy
		lead(effective_date_of_change_movement, 1
			-- If there is no next movement... 
			, CASE 
				WHEN current_termination != 0
					THEN effective_date_of_change_movement -- set it to termination date if available, 
				WHEN EXTRACT(YEAR FROM effective_date_of_change_movement) < EXTRACT(YEAR FROM DATE (param_value)) - 1
					THEN date_add('day', CAST(date_diff('day', effective_date_of_change_movement, date_parse(CAST(EXTRACT(year FROM 
												effective_date_of_change_movement) + 1 AS VARCHAR) || '-01-01', '%Y-%m-%d')) / 2.0 AS BIGINT), 
							effective_date_of_change_movement)
						-- or assume termination half way through remainder of year
				ELSE DATE (param_value)
					-- or end of investigation period if this is last full year, 
				END) OVER (
			PARTITION BY company_code
			,policy_number
			,life_number ORDER BY effective_date_of_change_movement
				,movementcounter
			) AS next_movement
		,termination_check.*
	FROM termination_check
	INNER JOIN assa_sandbox.csi_mort_params ON csi_mort_params.param_name = 'exposure_end_date'
	WHERE NOT (
			-- For now we are only excluding late exposure for company 11
			company_code = 11
			AND COALESCE(prior_termination, 0) < 0
			)
	)
	/* -------------------------------------------------------------------------------------------------------
   Now that we have the known end date, we need to split into individual calendar and policy years.
   This is necessary because of cases where there is no renewal record in a given calendar year
   -------------------------------------------------------------------------------------------------------*/
	,create_all_years
AS (
	SELECT exposure_years.*
		,date_add('year', policy_year - EXTRACT(year FROM policy_date_of_entry), policy_date_of_entry) AS policy_anniversary
		,date_add('year', calendar_year - EXTRACT(year FROM policy_date_of_entry), policy_date_of_entry) AS cy_anniversary
		,date_add('year', (policy_year + 1) - EXTRACT(year FROM policy_date_of_entry), policy_date_of_entry) AS next_policy_anniversary
		,calc_next_movement.*
	FROM calc_next_movement
	FULL OUTER JOIN exposure_years ON exposure_years.calendar_year >= EXTRACT(YEAR FROM effective_date_of_change_movement)
		AND exposure_years.calendar_year <= COALESCE(EXTRACT(YEAR FROM date_add('day', - 1, next_movement)), 9999)
	)
	/* -------------------------------------------------------------------------------------------------------
   Now we filter some of the newly created date records and calculate beginning and end dates
   to ensure that we have no overlapping exposure
   -------------------------------------------------------------------------------------------------------*/
	,exposure_boundary_dates
AS (
	SELECT GREATEST(effective_date_of_change_movement, policy_anniversary, date_parse(CAST(calendar_year AS VARCHAR) || '-01-01', '%Y-%m-%d')) AS begin_date
		,LEAST(next_movement, next_policy_anniversary, date_parse(CAST(calendar_year + 1 AS VARCHAR) || '-01-01', '%Y-%m-%d')) AS end_date
		,create_all_years.*
	FROM create_all_years
	WHERE (
			effective_date_of_change_movement < cy_anniversary
			AND policy_year < calendar_year
			)
		OR (policy_year >= calendar_year)
	)
	/* -------------------------------------------------------------------------------------------------------
   Here we finally calculate the number of days applicable for the given record, and the number of days 
   in the corresponding calendar year so that we can calculate the exposure in life years
   -------------------------------------------------------------------------------------------------------*/
	,exposure_calc
AS (
	SELECT date_diff('year', policy_date_of_entry, policy_anniversary) AS policy_duration
		,date_diff('day', begin_date, end_date) AS exposure_days
		,date_diff('day', date_trunc('year', effective_date_of_change_movement), date_trunc('year', date_add('year', 1, effective_date_of_change_movement))) 
		AS days_in_year
		,date_diff('year', date_of_birth, policy_anniversary) AS age_last_at_pa
		,date_diff('year', date_of_birth, date_add('month', 6, policy_anniversary)) AS age_nrst_at_pa
		,date_diff('year', date_of_birth, date_parse(CAST(calendar_year AS VARCHAR) || '-01-01', '%Y-%m-%d')) AS age_last_at_jan
		,date_diff('year', date_of_birth, date_parse(CAST(calendar_year AS VARCHAR) || '-07-01', '%Y-%m-%d')) AS age_nrst_at_jan
		,exposure_boundary_dates.*
	FROM exposure_boundary_dates
	)
SELECT *
FROM exposure_calc
