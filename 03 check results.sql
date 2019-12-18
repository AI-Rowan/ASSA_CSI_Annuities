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
