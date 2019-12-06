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
	,substr(change_in_movement_code, - 2)
	,direction_of_movement
	,Count(*)
FROM assa_sandbox.v1_assa_movement
GROUP BY company_code
	,substr(change_in_movement_code, - 2)
	,direction_of_movement
ORDER BY substr(change_in_movement_code, - 2)
	,company_code
	--,change_in_movement_code
	;

-- Check reasonability of inforce count at beginning of each year
SELECT company_code
	,sourcefilename
	,count(*)
FROM assa_sandbox.v1_assa_movement
WHERE change_in_movement_code IN ('X010')
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
WHERE a.company_code <> 11
ORDER BY a.policy_number
	,a.life_number
	,a.sourcefilename
	,a.movementcounter;
