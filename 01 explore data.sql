--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
-- Investigate data contents
-- Split of companies/source files?
SELECT sourcefilename
	,company_code
	,count(*)
FROM assa_sandbox.v1_assa_movement
GROUP BY sourcefilename
	,company_code
ORDER BY company_code
	,sourcefilename;

-- Movement codes for each company
-- Largely look okay
SELECT company_code
	,movement_code_clean
	,direction_of_movement
	,Count(*)
FROM assa_sandbox.v1_assa_movement
GROUP BY company_code
	,movement_code_clean
	,direction_of_movement
ORDER BY movement_code_clean
	,company_code
	--,change_in_movement_code
	;

-- Check reasonability of inforce count at beginning of each year
SELECT company_code
	,sourcefilename
	,count(*)
FROM assa_sandbox.v1_assa_movement
WHERE movement_code_clean = '10'
GROUP BY company_code
	,sourcefilename
ORDER BY company_code
	,sourcefilename;

-- Company 12 has very low number of policies for 2006 and 2007:
-- Nothing obviously problematic - maybe they just started selling new-gen late
SELECT *
FROM assa_sandbox.v1_assa_movement
WHERE company_code = 12
	AND sourcefilename IN (
		'dcs_input_newgen_2006.txt'
		,'dcs_input_newgen_2007.txt'
		)
ORDER BY policy_number
	,life_number
	,sourcefilename
	,effective_date_of_change_movement;

-- Let's see if we can get a reasonable number of deaths
-- OK, it's reasonable, but I don't understand how some of the numbers in the pivot are larger than in the raw data?
-- Looks like a lot of it is 2003 for Company 11
SELECT company_code
	,count(*)
FROM assa_sandbox.v1_assa_movement
WHERE substr(change_in_movement_code, - 2) IN (
		'30'
		,'43'
		,'44'
		)
	AND effective_date_of_change_movement <= DATE '2013-12-31'
GROUP BY company_code;

-- Check whether movement counter makes sense
SELECT company_code
	,policy_number
	,life_number
	,movementcounter
	,count(*)
FROM assa_sandbox.v1_assa_movement
GROUP BY company_code
	,policy_number
	,life_number
	,movementcounter
HAVING count(*) > 1;

-- ... lots of cases with duplicate movementcounter, so let's dig deeper
--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
SELECT a.*
FROM assa_sandbox.v1_assa_movement AS a
INNER JOIN (
	SELECT company_code
		,policy_number
		,life_number
		,movementcounter
	FROM assa_sandbox.v1_assa_movement
	GROUP BY company_code
		,policy_number
		,life_number
		,movementcounter
	HAVING count(*) > 1
	) using (
		company_code
		,policy_number
		,life_number
		,movementcounter
		)
WHERE a.company_code != 11
ORDER BY a.policy_number
	,a.life_number
	,a.sourcefilename
	,a.movementcounter;

-- Check that all movements have the correct direction
SELECT company_code
	,movement_code_clean
	,direction_of_movement
	,count(*)
FROM assa_sandbox.v1_assa_movement
GROUP BY company_code
	,movement_code_clean
	,direction_of_movement
ORDER BY company_code
	,movement_code_clean
	,direction_of_movement;

-- Company 11 has a lot of cases without an actual termination record:
SELECT count(*)
FROM (
	SELECT company_code
		,policy_number /* ,life_number -- life number is null for co 11 */
	FROM assa_sandbox.v1_assa_movement
	WHERE company_code = 11
	GROUP BY company_code
		,policy_number
	HAVING max(year_of_data) < 2013
		AND sum(CASE 
				WHEN movement_code_clean IN (
						'30'
						,'43'
						,'44'
						,'50'
						)
					THEN 1
				ELSE 0
				END) = 0
	);

SELECT year_of_data
	,movement_code_clean
	,count(*)
FROM assa_sandbox.v1_assa_movement
WHERE company_code = 11
GROUP BY year_of_data
	,movement_code_clean
ORDER BY year_of_data
	,movement_code_clean;

-- Doing the same check for other companies
-- Looking better after all of the cleanups built into the query creating v1_assa_movement in assa_sandbox
SELECT company_code
	,count(*)
	,sum(is_bad)
FROM (
	SELECT company_code
		,CASE 
			WHEN max(year_of_data) < 2013
				AND sum(CASE 
						WHEN movement_code_clean IN (
								'30'
								,'43'
								,'44'
								,'50'
								)
							THEN 1
						ELSE 0
						END) = 0
				THEN 1
			ELSE 0
			END AS is_bad
		,policy_number
		,date_of_birth -- I'm not convinced life_number is meaningful for some companies
	FROM assa_sandbox.v1_assa_movement
	--WHERE company_code = 11
	GROUP BY company_code
		,policy_number
		,date_of_birth
	)
GROUP BY company_code;

-- Company 30 is going to be a problem
SELECT *
FROM assa_sandbox.v1_assa_movement
WHERE company_code = 30
ORDER BY CAST(substring(life_number, 8, 10) AS BIGINT)
	,CAST(substring(policy_number, 6, 10) AS BIGINT)
	,effective_date_of_change_movement limit 500;

-- VEry large number for company 25 despite reasonable life numbers and policy numbers
SELECT *
FROM assa_sandbox.v1_assa_movement
INNER JOIN (
	SELECT company_code
		,CASE 
			WHEN max(year_of_data) < 2013
				AND sum(CASE 
						WHEN movement_code_clean IN (
								'30'
								,'43'
								,'44'
								,'50'
								)
							THEN 1
						ELSE 0
						END) = 0
				THEN 1
			ELSE 0
			END AS is_bad
		,policy_number
		,date_of_birth -- I'm no longer sure life_number is reliable
	FROM assa_sandbox.v1_assa_movement
	--WHERE company_code = 11
	GROUP BY company_code
		,policy_number
		,date_of_birth
	) AS pns ON pns.policy_number = v1_assa_movement.policy_number
	AND pns.date_of_birth = v1_assa_movement.date_of_birth
	AND pns.company_code = v1_assa_movement.company_code
	AND pns.is_bad = 1
WHERE pns.company_code = 25
ORDER BY pns.policy_number
	,pns.date_of_birth
	,year_of_data
	,effective_date_of_change_movement limit 500

-- Company 25's life identifiers changed in 2012. Let's see if we can come up with a reliable way to extract a life identifier:
-- This worked pretty well in the final query!
SELECT DISTINCT policy_number
	,life_number
	,coalesce(regexp_extract(life_number, policy_number || '_(\d*)', 1), regexp_extract(life_number, '^(\d{1,3})_1$', 1))
FROM assa_sandbox.v1_assa_movement
WHERE company_code = 25
ORDER BY policy_number
	,life_number

-- How to handle company 30?
-- Could be worse - maximum number sharing dates seems to be 17?
-- Looks like even that case is actually just one life which at some points is treated as two different lives for no good reason (two lives from 2007 to 2012, but one life in 2013, and no changes in policy amounts)
-- It does seem like relative ordering of policies is preserved, even if the absolute values of the policy number changes from one year to the next
-- Where we run into problems is when only one of the "sub-policies" is terminated (looks like this is generally combining the different lines)
SELECT year_of_data
	,date_of_birth
	,policy_date_of_entry
	,count(*)
FROM (
	SELECT DISTINCT date_of_birth
		,policy_date_of_entry
		,policy_number
		,life_number
		,year_of_data
	FROM assa_sandbox.v1_assa_movement
	WHERE company_code = 30
	)
GROUP BY date_of_birth
	,policy_date_of_entry
	,year_of_data
ORDER BY count(*) DESC


SELECT policy_number
	,life_number
	,date_of_birth
	,policy_date_of_entry
	,effective_date_of_change_movement
	,year_of_data
	,date_format(date_of_birth, '%Y%m%d') AS life_number_new
	,date_format(date_of_birth, '%Y%m%d') || '_' || date_format(policy_date_of_entry, '%Y%m%d') || '_' || cast(rank() OVER (
			PARTITION BY year_of_data
			,policy_date_of_entry
			,date_of_birth ORDER BY policy_number
			) AS VARCHAR) AS policy_number_new
FROM assa_sandbox.v1_assa_movement
WHERE company_code = 30
ORDER BY date_of_birth
	,policy_date_of_entry

-- This seems to be pretty intractable. Can we get away with a different solution? Maybe we can combine all policies issued at a given date with the same DOB
-- What's this look like for c11 - the company with the next largest number of records?
-- Not promising either
SELECT date_of_birth
	,policy_date_of_entry
	,count(*)
FROM (
	SELECT DISTINCT date_of_birth
		,policy_date_of_entry
		,life_number
		,policy_number
	FROM assa_sandbox.v1_assa_movement
	WHERE company_code = 11
	)
GROUP BY date_of_birth
	,policy_date_of_entry
ORDER BY count(*) DESC

-- Check what is being used as 'start of period' date for each company
-- Bit of variety.
-- In particular c25 is using start of any month.
-- Will probably just clean up c12 that using 31 Dec instead of 1 Jan
SELECT company_code
	,date_format(effective_date_of_change_movement, '%m%d')
	,count(*)
FROM assa_sandbox.v1_assa_movement
WHERE movement_code_clean = '10'
GROUP BY company_code
	,date_format(effective_date_of_change_movement, '%m%d')
ORDER BY company_code
	,date_format(effective_date_of_change_movement, '%m%d')



