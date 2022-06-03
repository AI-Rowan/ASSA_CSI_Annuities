-- Check totals
SELECT company_code
	,calendar_year
	,sum(expyearscen)
	,sum(actual_claim_cnt)
FROM assa_sandbox.assa_exposure
GROUP BY company_code
	,calendar_year;

-- Check for dropped claims
SELECT a.company_code
	,a.policy_number
	,a.life_number
FROM (
	SELECT company_code
		,policy_number
		,life_number
	FROM assa_sandbox.v1_assa_movement
	WHERE movement_code_clean = '30'
	GROUP BY company_code
		,policy_number
		,life_number
	HAVING sum(direction_of_movement) != 0
	) AS a
LEFT JOIN (
	SELECT company_code
		,policy_number
		,life_number
	FROM assa_sandbox.assa_exposure
	WHERE actual_claim_cnt != 0
	) AS b ON a.policy_number = b.policy_number
	AND a.life_number = b.life_number
WHERE b.policy_number IS NULL;

-- Check for duplicate claims
SELECT company_code
	,policy_number
	,life_number
	,sum(actual_claim_cnt)
FROM assa_sandbox.assa_exposure
GROUP BY company_code
	,policy_number
	,life_number
HAVING sum(actual_claim_cnt) > 1
ORDER BY sum(actual_claim_cnt) DESC;

-- Check for bad policy durations
SELECT * from assa_sandbox.assa_exposure WHERE policy_duration < 0;

-- Current version of code has both some missing claims and some duplicates, but I think I have done a fair job of minimising both in the context of ambiguous source data
--
--
-- High level exposure check
SELECT company_code
	,year_of_data
	,sum(CASE 
			WHEN movement_code_clean = '00'
				THEN 0.5 * direction_of_movement
			WHEN movement_code_clean IN ('10')
				THEN 1
			WHEN movement_code_clean IN (
					'30'
					,'43'
					,'44'
					,'50'
					)
				THEN 0.5 * direction_of_movement
			ELSE 0
			END) AS exposure_estimate
FROM assa_sandbox.v1_assa_movement
GROUP BY company_code
	,year_of_data
ORDER BY company_code
	,year_of_data

-- Exposure Check 2
select company_code, year_of_data, status, count(*) from (
select company_code, year_of_data , policy_number, life_number, min(case when change_in_movement_code = '00' then '0NB' when change_in_movement_code IN ('30','43','44','50') then '1TERM' else '2IF' end) status 
from assa_sandbox.v1_assa_movement 
group by company_code, year_of_data, policy_number, life_number)
group by company_code, year_of_data, status