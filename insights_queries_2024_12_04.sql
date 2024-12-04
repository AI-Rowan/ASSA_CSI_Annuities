
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_assa_1_socio_economic_class;
CREATE TABLE "assa_sandbox".v1_assa_1_socio_economic_class
WITH (
	-- priority = 1,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array[]
)
AS
SELECT id, socio_economic_class_desc
FROM 
  (VALUES
    (1,'Worst'),
    (2,'3rd Best'),
    (3,'2nd Best'),
    (4,'Best')
    ) AS socio_economic_class(id, socio_economic_class_desc)
;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_assa_1_age_bands;
CREATE TABLE "assa_sandbox".v1_assa_1_age_bands
WITH (
	-- priority = 1,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array[]
)
AS
SELECT age_start_band, age_band_desc
FROM 
  (VALUES
    (-1,'Unknown'),
    (0,'<20'),
    (10,'<20'),
    (20,'20 - 29'),
    (30,'30 - 39'),
    (40,'40 - 49'),
    (50,'50 - 59'),
    (60,'60 - 69'),
    (70,'70+'),
    (80,'70+'),
    (90,'70+'),
    (100,'70+'),
    (110,'70+'),
    (120,'70+'),
    (130,'70+')
    ) AS age_band(age_start_band, age_band_desc)
;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_1_co6_covid_claims;
CREATE TABLE "assa_sandbox".v1_1_co6_covid_claims
WITH (
	-- priority = 2,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
WITH max_timestamp AS (SELECT MAX(process_time_stamp) as process_time_stamp
FROM "assa-lake".v1_co6_covid_claims_source_files)
SELECT
6 AS company_code,
covid_claims.data_import_batch,
gender AS sex,
REPLACE(smoker___status, 'Non-smoker', 'Non-Smoker') as smoker_status, 
age_band_desc as age_band,
class as socio_economic_class,
DATE_FORMAT(date_parse(effective_dt, '%Y-%m-%d'), '%Y%m') as effective_year_month,
coalesce(DATE_FORMAT(date_parse(NULLIF(notification_dt, ''), '%Y-%m-%d'), '%Y%m'), DATE_FORMAT(date_add('month', 2, date_parse(effective_dt, '%Y-%m-%d')), '%Y%m')) as notification_year_month, -- date_format(date_parse(notification_dt, '%Y-%m-%d'), '%Y%m') as notification_year,
cause_group, 
count(covid_claims.policy_number) AS count,
6 AS service_client_id
FROM "assa-lake".v1_co6_covid_claims_source_files AS covid_claims
INNER JOIN max_timestamp mts ON covid_claims.process_time_stamp = mts.process_time_stamp
INNER JOIN "assa_sandbox".v1_assa_1_age_bands ab ON ab.age_start_band = 
IF(
    (memb_dob_dt IS NULL) OR (effective_dt IS NULL), -1,
                                                            FLOOR(DATE_DIFF('year', cast(memb_dob_dt AS DATE), cast(effective_dt AS DATE))/10)*10)
INNER JOIN "assa-lake".v1_test_co6_covid_claim_cause cc on cc.claim_cause = covid_claims.claim_cause_desc
WHERE claim_cat_desc IN ('Partially Paid', 'Paid', 'Notified')
GROUP BY
6,
covid_claims.data_import_batch, gender, smoker___status, age_band_desc, class,
DATE_FORMAT(date_parse(effective_dt, '%Y-%m-%d'), '%Y%m'),
coalesce(DATE_FORMAT(date_parse(NULLIF(notification_dt, ''), '%Y-%m-%d'), '%Y%m'), DATE_FORMAT(date_add('month', 2, date_parse(effective_dt, '%Y-%m-%d')), '%Y%m')),
cause_group, 6

;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_2_co30_covid_exposure;
CREATE TABLE "assa_sandbox".v1_2_co30_covid_exposure
WITH (
	-- priority = 2,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id', 'data_import_batch']
)
AS
WITH latest_exp AS (SELECT MAX(process_time_stamp) AS process_time_stamp
FROM "assa-lake".v1_co30_exposure_source_files
)
SELECT 
CAST(30 AS BIGINT) as company_code,
all_exp.sex,
REPLACE(
REPLACE(all_exp.smoker_status, 'Non-smokers', 'Non-Smoker'),
'Smokers', 'Smoker') AS smoker_status,
CASE
    WHEN all_exp.age_band = '>70' THEN '70+'
    ELSE REGEXP_REPLACE (
      all_exp.age_band,
      '(?<rstart>\d+)-(?<rend>\d+)',
      '${rstart} - ${rend}'
    ) END AS age_band,
all_exp.socio_economic_group,
CAST(all_exp.effective_date AS TIMESTAMP) as effective_date,
all_exp.number_of_policies,
CAST(30 AS BIGINT) as service_client_id,
all_exp.data_import_batch
FROM "assa-lake".v1_co30_exposure_source_files all_exp
JOIN latest_exp lexp ON 
lexp.process_time_stamp = all_exp.process_time_stamp


;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_2_co18_covid_exposure;
CREATE TABLE "assa_sandbox".v1_2_co18_covid_exposure
WITH (
	-- priority = 2,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id', 'data_import_batch']
)
AS
WITH latest_exp as (SELECT MAX(process_time_stamp) as process_time_stamp, source_file_name
FROM "assa-lake".v1_co18_covid_exposure_source_files
WHERE data_import_batch >= '2024-05-28'
GROUP BY source_file_name
)
SELECT
CAST(18 AS BIGINT) AS company_code,
gender as sex,
REPLACE(
REPLACE(smoker_status, 'Non-smokers', 'Non-Smoker'),
'Smokers', 'Smoker') as smoker_status, -- replace with camel case
CASE
    WHEN ageband = '>70' THEN '70+'
    ELSE REGEXP_REPLACE (
      ageband,
      '(?<rstart>\d+)-(?<rend>\d+)',
      '${rstart} - ${rend}'
    ) END AS age_band,
se_class as socio_economic_class,
CAST(DATE_FORMAT(DATE_PARSE(
          concat(cast(year as varchar), '-', cast(month as varchar)), '%Y-%m') + interval '1' month - interval '1' day, '%Y-%m-%d') AS TIMESTAMP) AS effective_date,
CAST(ce.policy_count AS BIGINT) AS number_of_policies,
CAST(18 AS BIGINT) AS service_client_id,
ce.data_import_batch AS data_import_batch
FROM "assa-lake".v1_co18_covid_exposure_source_files ce
INNER JOIN latest_exp le ON
ce.process_time_stamp = le.process_time_stamp
AND le.source_file_name = ce.source_file_name

;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_1_co11_covid_claims;
CREATE TABLE "assa_sandbox".v1_1_co11_covid_claims
WITH (
	-- priority = 2,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
WITH max_timestamp as (SELECT MAX(process_time_stamp) as process_time_stamp
FROM "assa-lake".v1_co11_covid_claims_source_files
)
SELECT
11 as company_code,
cc11.data_import_batch as data_import_batch,
REPLACE(
REPLACE(cc11.sex, 'FEMALE', 'Female'), 'MALE', 'Male') as sex,
REPLACE(
REPLACE(cc11.smoker_stat, 'NON SMOKER', 'Non-Smoker'),
'SMOKER', 'Smoker') as smoker_status,
CASE
    WHEN cc11.ageband = '>=70' THEN '70+'
    ELSE REGEXP_REPLACE (
      cc11.ageband,
      '(?<rstart>\d+)-(?<rend>\d+)',
      '${rstart} - ${rend}'
    ) END AS age_band,
sec.socio_economic_class_desc as socio_economic_class,
DATE_FORMAT(DATE_PARSE(CONCAT(CAST(cc11.event_date AS VARCHAR),'01'), '%Y%m%d'), '%Y-%m-%d') as year_and_month_of_date_of_death,
DATE_FORMAT(DATE_PARSE(CONCAT(CAST(cc11.reported_date AS VARCHAR),'01'), '%Y%m%d'), '%Y-%m-%d') as year_and_month_of_date_reported,
SUM(CASE 
    WHEN cc11.event_cause = 'NATURAL' THEN cc11.claim_count ELSE 0 END) as natural_claim_count,
SUM(CASE 
    WHEN cc11.event_cause = 'UNNATURAL' THEN cc11.claim_count ELSE 0 END) as unnatural_claim_count,
SUM(CASE 
    WHEN cc11.event_cause = '' THEN cc11.claim_count ELSE 0 END) as unspecified_claim_count,
(select CAST(COALESCE(
ROUND(sum(CASE WHEN event_cause = 'NATURAL' THEN CAST(claim_count AS DOUBLE) END)/
(sum(CASE WHEN event_cause = 'UNNATURAL' THEN CAST(claim_count AS DOUBLE)  END)
+
sum(CASE WHEN event_cause = 'NATURAL' THEN CAST(claim_count AS DOUBLE)  END))
*100),0) AS BIGINT)
from "assa-lake".v1_co11_covid_claims_source_files) as percentage_natural,
SUM(cc11.covid_count) as covid_count,
11 AS service_client_id
FROM "assa-lake".v1_co11_covid_claims_source_files AS cc11
INNER JOIN max_timestamp mts ON cc11.process_time_stamp = mts.process_time_stamp
JOIN "assa_sandbox".v1_assa_1_socio_economic_class sec ON sec.id = cc11.irp_mkr
GROUP BY data_import_batch, sec.socio_economic_class_desc, cc11.sex, cc11.smoker_stat,
cc11.ageband, cc11.event_date, cc11.reported_date

;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_1a_co18_covid_claims;
CREATE TABLE "assa_sandbox".v1_1a_co18_covid_claims
WITH (
	-- priority = 2,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
WITH cc18_1 AS (
WITH
latest_claims AS (
    SELECT MAX(process_time_stamp) AS process_time_stamp, source_file_name
    FROM "assa-lake".v1_sandbox_co18_covid_claims
    WHERE data_import_batch >= '2024-05-28'
    GROUP BY source_file_name),
rownum AS (
    SELECT *,
    ROW_NUMBER() OVER (ORDER BY 1) AS rn
    FROM "assa-lake".v1_sandbox_co18_covid_claims cc
    JOIN latest_claims lc ON lc.process_time_stamp = cc.process_time_stamp
        AND lc.source_file_name = cc.source_file_name)
SELECT
18 as company_code,
data_import_batch,
gender as sex,
REPLACE(
REPLACE(smoker_status, 'Non-smokers', 'Non-Smoker'),
'Smokers', 'Smoker') as smoker_status,
se_class as socio_economic_class,
CASE
    WHEN ageband = '>70' THEN '70+'
    ELSE REGEXP_REPLACE (
      ageband,
      '(?<rstart>\d+)-(?<rend>\d+)',
      '${rstart} - ${rend}'
    ) END AS age_band,
cause_of_death as cause_group,
DATE_FORMAT(date_parse(process_date, '%d/%m/%Y'), '%Y-%m-%d') AS year_and_month_of_date_of_death,
DATE_FORMAT(date_parse(process_date, '%d/%m/%Y'), '%Y-%m-%d') AS year_and_month_of_date_reported,
number_of_deaths AS number_of_deaths,
18 as service_client_id
FROM rownum
GROUP BY data_import_batch, gender, smoker_status, se_class, ageband, cause_of_death,
DATE_FORMAT(date_parse(process_date, '%d/%m/%Y'), '%Y-%m-%d'),
DATE_FORMAT(date_parse(process_date, '%d/%m/%Y'), '%Y-%m-%d'), number_of_deaths)
,
cc18_2 AS (
WITH
latest_claims AS (
    SELECT MAX(process_time_stamp) AS process_time_stamp, source_file_name
    FROM "assa-lake".v1_sandbox_co18_covid_claims
    WHERE data_import_batch >= '2024-05-28'
    GROUP BY source_file_name),
rownum AS (
    SELECT *,
    ROW_NUMBER() OVER (ORDER BY 1) AS rn
    FROM "assa-lake".v1_sandbox_co18_covid_claims cc
    JOIN latest_claims lc ON lc.process_time_stamp = cc.process_time_stamp
        AND lc.source_file_name = cc.source_file_name)
SELECT
18 as company_code,
data_import_batch,
gender as sex,
REPLACE(
REPLACE(smoker_status, 'Non-smokers', 'Non-Smoker'),
'Smokers', 'Smoker') as smoker_status, -- replace with camel case
se_class as socio_economic_class,
CASE
    WHEN ageband = '>70' THEN '70+'
    ELSE REGEXP_REPLACE (
      ageband,
      '(?<rstart>\d+)-(?<rend>\d+)',
      '${rstart} - ${rend}'
    ) END AS age_band,
cause_of_death as cause_group,
DATE_FORMAT(DATE_ADD('month', -1, date_parse(process_date, '%d/%m/%Y')), '%Y-%m-%d') AS year_and_month_of_date_of_death,
DATE_FORMAT(date_parse(process_date, '%d/%m/%Y'), '%Y-%m-%d') AS year_and_month_of_date_reported,
number_of_deaths AS number_of_deaths,
18 as service_client_id
FROM rownum
GROUP BY data_import_batch, gender, smoker_status, se_class, ageband, cause_of_death,
DATE_FORMAT(DATE_ADD('month', -1, date_parse(process_date, '%d/%m/%Y')), '%Y-%m-%d'),
DATE_FORMAT(date_parse(process_date, '%d/%m/%Y'), '%Y-%m-%d'), number_of_deaths)
SELECT * FROM cc18_1
UNION ALL
SELECT * FROM cc18_2

;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_1b_co18_covid_claims;
CREATE TABLE "assa_sandbox".v1_1b_co18_covid_claims
WITH (
	-- priority = 2,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
WITH
latest_claims AS (
    SELECT MAX(process_time_stamp) AS process_time_stamp, source_file_name
    FROM "assa-lake".v1_sandbox_co18_covid_claims
    WHERE data_import_batch >= '2024-05-28'
    GROUP BY source_file_name),
raw AS (
SELECT
cc.gender AS sex,
REPLACE(
REPLACE(cc.smoker_status, 'Non-smokers', 'Non-Smoker'),
'Smokers', 'Smoker') as smoker_status,
cc.se_class AS socio_economic_class,
CASE
    WHEN cc.ageband = '>70' THEN '70+'
    ELSE REGEXP_REPLACE (
      cc.ageband,
      '(?<rstart>\d+)-(?<rend>\d+)',
      '${rstart} - ${rend}'
    ) END AS age_band,
DATE_FORMAT(date_parse(cc.process_date, '%d/%m/%Y'), '%Y-%m-%d') as process_date, 
CASE
    WHEN cc.cause_of_death = 'Natural' THEN CAST(sum(cc.number_of_deaths) AS DOUBLE)/2 ELSE 0
    END AS raw_natural,
CASE
    WHEN cc.cause_of_death = 'Unnatural' THEN CAST(sum(cc.number_of_deaths) AS DOUBLE)/2 ELSE 0
    END AS raw_unnatural,
CASE
    WHEN cc.cause_of_death = 'Unspecified' THEN CAST(sum(cc.number_of_deaths) AS DOUBLE)/2 ELSE 0
    END AS raw_unspecified,
18 as service_client_id
FROM "assa-lake".v1_sandbox_co18_covid_claims cc
JOIN latest_claims lc ON lc.process_time_stamp = cc.process_time_stamp
    AND lc.source_file_name = cc.source_file_name
GROUP BY
cc.gender,
cc.smoker_status,
cc.se_class,
cc.ageband,
cc.cause_of_death,
cc.process_date)
SELECT
raw.sex,
raw.smoker_status,
raw.socio_economic_class,
raw.age_band,
raw.process_date,
sum(raw_natural) as raw_natural,
sum(raw_unnatural) as raw_unnatural,
sum(raw_unspecified) as raw_unspecified,
cast(sum(raw_natural) as double) / coalesce(NULLIF(cast(sum(raw_natural) as double) + cast(sum(raw_unnatural) as double),0),1) AS nat_prop,
service_client_id
FROM raw
GROUP BY
raw.sex,
raw.smoker_status,
raw.socio_economic_class,
raw.age_band,
raw.process_date,
service_client_id

;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_1_co30_covid_claims;
CREATE TABLE "assa_sandbox".v1_1_co30_covid_claims
WITH (
	-- priority = 2,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
WITH latest_claims AS (SELECT MAX(process_time_stamp) AS process_time_stamp
FROM "assa-lake".v1_co30_covid_claims_source_files
)
SELECT 
30 as company_code,
cc30.sex,
cc30.data_import_batch,
REPLACE(
REPLACE(cc30.smoker_status, 'Non-smokers', 'Non-Smoker'),
'Smokers', 'Smoker') AS smoker_status,
CASE
    WHEN cc30.age_band = '>70' THEN '70+'
    ELSE REGEXP_REPLACE (
      cc30.age_band,
      '(?<rstart>\d+)-(?<rend>\d+)',
      '${rstart} - ${rend}'
    ) END AS age_band,
cc30.socio_economic_class,
cc30.date as year_and_month_of_date_of_death,
DATE_FORMAT(DATE_ADD('month', 1, DATE_PARSE(cc30.date, '%Y-%m-%d')), '%Y-%m-%d') AS year_and_month_of_date_reported,
0 as number_of_natural_deaths_excl_covid,
SUM(cc30.number_of_covid_deaths) AS number_of_covid_deaths,
0 AS number_of_unnatural_deaths,
SUM(cc30.number_of_covid_deaths) AS total_number_of_natural_deaths_incl_covid,
SUM(cc30.number_of_covid_deaths) AS total_claims,
30 as service_client_id
FROM "assa-lake".v1_co30_covid_claims_source_files cc30
JOIN latest_claims lc ON 
lc.process_time_stamp = cc30.process_time_stamp
GROUP BY
cc30.sex,
cc30.data_import_batch,
REPLACE(
REPLACE(cc30.smoker_status, 'Non-smokers', 'Non-Smoker'),
'Smokers', 'Smoker'),
CASE
    WHEN cc30.age_band = '>70' THEN '70+'
    ELSE REGEXP_REPLACE (
      cc30.age_band,
      '(?<rstart>\d+)-(?<rend>\d+)',
      '${rstart} - ${rend}'
    ) END,
cc30.socio_economic_class,
cc30.date,
DATE_FORMAT(DATE_ADD('month', 1, DATE_PARSE(cc30.date, '%Y-%m-%d')), '%Y-%m-%d')


;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_2_co11_covid_exposure;
CREATE TABLE "assa_sandbox".v1_2_co11_covid_exposure
WITH (
	-- priority = 2,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id', 'data_import_batch']
)
AS
WITH latest_exp as (SELECT MAX(process_time_stamp) as process_time_stamp
FROM "assa-lake".v1_co11_covid_exposure_source_files
)
SELECT
CAST(11 AS BIGINT) AS company_code,
REPLACE(
REPLACE(TRIM(ce11.sex), 'FEMALE', 'Female'), 'MALE', 'Male') as sex,
REPLACE(
REPLACE(TRIM(ce11.smoker_stat), 'NON SMOKER', 'Non-Smoker'),
'SMOKER', 'Smoker') as smoker_status,
CASE
    WHEN ce11.ageband = '>=70' THEN '70+'
    ELSE REGEXP_REPLACE (
      ce11.ageband,
      '(?<rstart>\d+)-(?<rend>\d+)',
      '${rstart} - ${rend}'
    ) END AS age_band,
sec.socio_economic_class_desc as socio_economic_class,
CAST(DATE_FORMAT(DATE_PARSE(CONCAT(CAST(ce11.expo_yr AS VARCHAR),'-12-31'), '%Y-%m-%d'), '%Y-%m-%d') AS TIMESTAMP) AS effective_date,
CAST(ce11.exp_count AS BIGINT) AS number_of_policies,
CAST(11 AS BIGINT) AS service_client_id,
ce11.data_import_batch AS data_import_batch
FROM "assa-lake".v1_co11_covid_exposure_source_files ce11
INNER JOIN latest_exp le ON
ce11.process_time_stamp = le.process_time_stamp
INNER JOIN "assa_sandbox".v1_assa_1_socio_economic_class sec ON sec.id = ce11.irp_mkr
WHERE TRIM(ce11.sex) <> ''
;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_2_co18_covid_claims;
CREATE TABLE "assa_sandbox".v1_2_co18_covid_claims
WITH (
	-- priority = 3,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
SELECT
CAST(company_code AS BIGINT) AS company_code,
CAST(data_import_batch AS TIMESTAMP) as date_data_received,
cca.sex,
cca.smoker_status,
cca.age_band as age_band,
cca.socio_economic_class,
CAST(cca.year_and_month_of_date_of_death AS TIMESTAMP) AS year_and_month_of_date_of_death,
CAST(cca.year_and_month_of_date_reported AS TIMESTAMP) AS year_and_month_of_date_of_reported,
SUM(CASE
    WHEN cca.cause_group = 'Natural' THEN raw_natural + (raw_unspecified * nat_prop) ELSE 0
    END) AS number_of_natural_deaths_excl_covid,
SUM(CASE
    WHEN cca.cause_group = 'COVID' THEN CAST(cca.number_of_deaths AS DOUBLE)/2 ELSE 0
    END) AS number_of_covid_deaths,
SUM(CASE
    WHEN cca.cause_group = 'Unnatural' THEN raw_unnatural + raw_unspecified * (1 - nat_prop)  ELSE 0
    END) AS number_of_unnatural_deaths,
(SUM(CASE
    WHEN cca.cause_group = 'Natural' THEN raw_natural + (raw_unspecified * nat_prop) ELSE 0
    END) +
SUM(CASE
    WHEN cca.cause_group = 'COVID' THEN cca.number_of_deaths/2 ELSE 0
    END))
    AS total_number_of_natural_deaths_incl_covid,
SUM(CAST(cca.number_of_deaths AS DOUBLE)/2) as total_claims,
CAST(cca.service_client_id AS BIGINT) AS service_client_id
FROM "assa_sandbox".v1_1a_co18_covid_claims cca
INNER JOIN "assa_sandbox".v1_1b_co18_covid_claims ccb
ON cca.sex = ccb.sex
AND cca.smoker_status = ccb.smoker_status
AND cca.age_band = ccb.age_band
AND cca.socio_economic_class = ccb.socio_economic_class
AND cca.year_and_month_of_date_reported = ccb.process_date
WHERE TRIM(cca.age_band) <> ''
GROUP BY company_code, data_import_batch, cca.sex,
cca.smoker_status,
cca.age_band,
cca.socio_economic_class,
cca.year_and_month_of_date_of_death,
cca.year_and_month_of_date_reported,
cca.service_client_id

;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_2_co11_covid_claims;
CREATE TABLE "assa_sandbox".v1_2_co11_covid_claims
WITH (
	-- priority = 3,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
SELECT
CAST(company_code AS BIGINT) AS company_code,
CAST(data_import_batch AS TIMESTAMP) AS date_data_received,
cc11.sex AS sex,
cc11.smoker_status AS smoker_status,
cc11.age_band AS age_band,
cc11.socio_economic_class as socio_economic_class,
CAST(year_and_month_of_date_of_death AS TIMESTAMP) AS year_and_month_of_date_of_death,
CAST(year_and_month_of_date_reported AS TIMESTAMP) AS year_and_month_of_date_reported,
CAST(natural_claim_count + ROUND(unspecified_claim_count * CAST(percentage_natural AS DOUBLE)/100) - covid_count AS DOUBLE) AS number_of_natural_deaths_excl_covid,
CAST(covid_count AS DOUBLE) AS number_of_covid_deaths,
ROUND(unnatural_claim_count + (unspecified_claim_count * ROUND(1 - CAST(percentage_natural AS DOUBLE)/100))) AS number_of_unnatural_deaths,
ROUND((natural_claim_count + (unspecified_claim_count * CAST(percentage_natural AS DOUBLE)/100) - covid_count) + covid_count) AS total_number_of_natural_deaths_incl_covid,
ROUND((natural_claim_count + (unspecified_claim_count * CAST(percentage_natural AS DOUBLE)/100) - covid_count) + covid_count + 
(unnatural_claim_count + (unspecified_claim_count * ROUND(1 - CAST(percentage_natural AS DOUBLE)/100)))) AS total_claims,
CAST(service_client_id AS BIGINT) AS service_client_id
FROM "assa_sandbox".v1_1_co11_covid_claims cc11
;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_2_co30_covid_claims;
CREATE TABLE "assa_sandbox".v1_2_co30_covid_claims
WITH (
	-- priority = 3,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
WITH latest_mortality AS (SELECT MAX(process_time_stamp) as process_time_stamp
FROM "assa-lake".v1_co30_mortality_history_source_files
),
covid_claims as (SELECT sex, age_band, year_and_month_of_date_of_death, year_and_month_of_date_reported, sum(number_of_covid_deaths) AS number_of_covid_deaths
from "assa_sandbox".v1_1_co30_covid_claims
group by sex, age_band, year_and_month_of_date_of_death, year_and_month_of_date_reported)
SELECT 
CAST(30 AS BIGINT) AS company_code,
CAST(mh30.data_import_batch AS TIMESTAMP) AS date_data_received,
mh30.sex,
'Unknown' AS smoker_status,
coalesce(cc30.age_band, REPLACE(REPLACE(mh30.age_band, '70 - 79', '70+'), '80+', '70+')) AS age_band,
'Unknown' AS socio_economic_class,
CAST(DATE_FORMAT(DATE_PARSE(CONCAT(CAST(mh30.year_month AS VARCHAR),'01'), '%Y%m%d'), '%Y-%m-%d') AS TIMESTAMP) AS year_and_month_of_date_of_death,
CAST(DATE_FORMAT(DATE_ADD('month', 1,  DATE_PARSE(CONCAT(CAST(mh30.year_month AS VARCHAR),'01'), '%Y%m%d')), '%Y-%m-%d') AS TIMESTAMP) as year_and_month_of_date_reported,
-- cc30.number_of_covid_deaths,
coalesce(CAST(SUM(CASE
    WHEN mh30.event_cause = 'Natural' THEN mh30.number_of_deaths ELSE 0 end) - cc30.number_of_covid_deaths AS DOUBLE), SUM(CASE
    WHEN mh30.event_cause = 'Natural' THEN mh30.number_of_deaths ELSE 0 end)) AS number_of_natural_deaths_excl_covid,
CAST(0 AS DOUBLE) AS number_of_covid_deaths,
COALESCE(CAST(SUM(CASE
    WHEN mh30.event_cause = 'Non-natural' THEN mh30.number_of_deaths ELSE 0 end) AS DOUBLE), SUM(CASE
    WHEN mh30.event_cause = 'Natural' THEN mh30.number_of_deaths ELSE 0 end)) AS number_of_unnatural_deaths,
coalesce(CAST(SUM(CASE
    WHEN mh30.event_cause = 'Natural' THEN mh30.number_of_deaths ELSE 0 end) - cc30.number_of_covid_deaths AS DOUBLE), SUM(CASE
    WHEN mh30.event_cause = 'Natural' THEN mh30.number_of_deaths ELSE 0 end)) AS total_number_of_natural_deaths_incl_covid,
coalesce(SUM(CASE
    WHEN mh30.event_cause = 'Natural' THEN mh30.number_of_deaths ELSE 0 end) - cc30.number_of_covid_deaths, SUM(CASE
    WHEN mh30.event_cause = 'Natural' THEN mh30.number_of_deaths ELSE 0 end))  + COALESCE(CAST(SUM(CASE
    WHEN mh30.event_cause = 'Non-natural' THEN mh30.number_of_deaths ELSE 0 end) AS DOUBLE), SUM(CASE
    WHEN mh30.event_cause = 'Natural' THEN mh30.number_of_deaths ELSE 0 end)) AS total_claims,
CAST(30 AS BIGINT) AS service_client_id
FROM "assa-lake".v1_co30_mortality_history_source_files mh30
JOIN latest_mortality lm ON
lm.process_time_stamp = mh30.process_time_stamp
LEFT JOIN covid_claims cc30
ON
CAST(mh30.year_month AS VARCHAR) = DATE_FORMAT(date_parse(cc30.year_and_month_of_date_of_death, '%Y-%m-%d'), '%Y%m') and
mh30.sex = cc30.sex
AND REPLACE(
REPLACE(mh30.age_band, '70 - 79', '70+'), '80+', '70+') = cc30.age_band
GROUP BY data_import_batch, mh30.sex, coalesce(cc30.age_band, REPLACE(REPLACE(mh30.age_band, '70 - 79', '70+'), '80+', '70+')), 
DATE_FORMAT(DATE_PARSE(CONCAT(CAST(mh30.year_month AS VARCHAR),'01'), '%Y%m%d'), '%Y-%m-%d'),
DATE_FORMAT(DATE_ADD('month', 1,  DATE_PARSE(CONCAT(CAST(mh30.year_month AS VARCHAR),'01'), '%Y%m%d')), '%Y-%m-%d'),
cc30.number_of_covid_deaths, service_client_id
UNION ALL
SELECT 
CAST(company_code AS BIGINT) AS company_code,
CAST(data_import_batch as TIMESTAMP) AS date_data_received,
sex,
smoker_status,
age_band,
socio_economic_class,
CAST(year_and_month_of_date_of_death AS TIMESTAMP),
CAST(year_and_month_of_date_reported AS TIMESTAMP) AS year_and_month_of_date_reported,
CAST(0 AS DOUBLE) as number_of_natural_deaths_excl_covid,
CAST(number_of_covid_deaths AS DOUBLE) AS number_of_covid_deaths,
CAST(0 AS DOUBLE) AS number_of_unnatural_deaths,
CAST(number_of_covid_deaths AS DOUBLE) AS total_number_of_natural_deaths_incl_covid,
CAST(number_of_covid_deaths AS DOUBLE) AS total_claims,
CAST(30 AS BIGINT) AS service_client_id
FROM "assa_sandbox".v1_1_co30_covid_claims


;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.covid_exposure;
CREATE TABLE "assa_sandbox".covid_exposure
WITH (
	-- priority = 3,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id', 'data_import_batch']
)
AS
WITH dib_to_use AS (
  SELECT
    company_code,
    date_parse (effective_date, '%Y-%m-%d') AS effective_date,
    MAX (data_import_batch) AS max_batch
  FROM
    "assa-lake".v1_covid_exposure
    WHERE company_code not in (11,30)
  GROUP BY
    company_code,
    date_parse (effective_date, '%Y-%m-%d')
),
dib_to_use_25 AS (
  SELECT
    year_month_of_file,
    MAX (process_time_stamp) AS max_batch
  FROM
    "assa-lake".v1_covid_inforce_25
  GROUP BY
    year_month_of_file
),
clean_data AS (
  SELECT
    company_code,
    sex,
    REPLACE(REPLACE(smoker_status, 'Non-smokers', 'Non-Smoker'),'Non-smoker', 'Non-Smoker') as smoker_status,
    CASE
      WHEN age_band = '60-70' THEN '60 - 69'
      WHEN age_band = '>=70' THEN '70+'
      ELSE REGEXP_REPLACE (
        age_band,
        '(?<rstart>\d+)-(?<rend>\d+)',
        '${rstart} - ${rend}'
      )
    END AS age_band,
    COALESCE (NULLIF (socio_economic_group, ''), 'UNKNOWN') AS socio_economic_group,
    date_parse (effective_date, '%Y-%m-%d') AS effective_date,
    number_of_policies,
    company_code AS service_client_id,
    data_import_batch
  FROM
    "assa-lake".v1_covid_exposure
)
SELECT
  clean_data.*
FROM
  clean_data
  INNER JOIN dib_to_use ON clean_data.company_code = dib_to_use.company_code
  AND clean_data.effective_date = dib_to_use.effective_date
  AND clean_data.data_import_batch = dib_to_use.max_batch
UNION
ALL
SELECT
  company_code,
  sex,
  smoker_status,
CASE
    WHEN age < 20 THEN '<20'
    WHEN age >= 70 THEN '70+'
    ELSE CAST (10 * FLOOR (age / 10) AS VARCHAR) || ' - ' || CAST (10 * (FLOOR (age / 10) + 1) - 1 AS VARCHAR)
  END age_band,
  education AS socio_economic_group,
  effective_date,
  COUNT (*) AS number_of_policies,
  company_code as service_client_id,
  data_import_batch
FROM
  (
    SELECT
      lake.*,
      date_diff (
        'year',
        date_parse (date_of_birth, '%Y-%m-%d'),
        eff_date
      ) AS age,
      eff_date AS effective_date
    FROM 
    (
      select 
        case year_month_of_file
          when '2020H1' THEN  date '2020-06-30'
          when '2020H2' THEN  date '2020-12-31'
          else last_day_of_month(date_parse(year_month_of_file, '%Y%m'))
        end as eff_date, 
        case gender 
          when '1' THEN 'Male'
          when '2' THEN 'Female'
          else gender 
        end as sex,
        *
      from "assa-lake".v1_covid_inforce_25
      ) lake
      INNER JOIN dib_to_use_25 ON lake.process_time_stamp = dib_to_use_25.max_batch
      AND lake.year_month_of_file = dib_to_use_25.year_month_of_file
  )
GROUP BY
  company_code,
  sex,
  smoker_status,
  CASE
    WHEN age < 20 THEN '<20'
    WHEN age >= 70 THEN '70+'
    ELSE CAST (10 * FLOOR (age / 10) AS VARCHAR) || ' - ' || CAST (10 * (FLOOR (age / 10) + 1) - 1 AS VARCHAR)
  END,
  education,
  effective_date,
  data_import_batch
  UNION ALL
  SELECT * FROM "assa_sandbox".v1_2_co11_covid_exposure
  UNION ALL
  SELECT * FROM "assa_sandbox".v1_2_co30_covid_exposure
  UNION ALL 
  SELECT * FROM "assa_sandbox".v1_2_co18_covid_exposure

;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.v1_2_co6_covid_claims;
CREATE TABLE "assa_sandbox".v1_2_co6_covid_claims
WITH (
	-- priority = 3,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
SELECT
CAST(company_code AS BIGINT) AS company_code,
CAST(data_import_batch AS TIMESTAMP) AS date_data_received,
cca.sex,
cca.smoker_status,
cca.age_band AS age_band,
cca.socio_economic_class,
CAST(DATE_FORMAT(DATE_PARSE(CONCAT(CAST(cca.effective_year_month AS VARCHAR),'01'), '%Y%m%d'), '%Y-%m-%d') AS TIMESTAMP) AS year_and_month_of_date_of_death,
CAST(DATE_FORMAT(DATE_PARSE(CONCAT(CAST(cca.notification_year_month AS VARCHAR),'01'), '%Y%m%d'), '%Y-%m-%d') AS TIMESTAMP) AS year_and_month_of_date_reported,
CAST(SUM(CASE
    WHEN cca.cause_group = 'Natural' THEN cca.count ELSE 0
    END) AS DOUBLE) AS number_of_natural_deaths_excl_covid,
CAST(SUM(CASE
    WHEN cca.cause_group = 'COVID' THEN cca.count ELSE 0
    END) AS DOUBLE) AS number_of_covid_deaths,
CAST(SUM(CASE
    WHEN cca.cause_group = 'Unnatural' THEN cca.count ELSE 0
    END) AS DOUBLE) AS number_of_unnatural_deaths,
CAST(SUM((CASE
    WHEN cca.cause_group = 'Natural' THEN cca.count ELSE 0
    END +
CASE
    WHEN cca.cause_group = 'COVID' THEN cca.count ELSE 0
    END)) AS DOUBLE)
    AS total_number_of_natural_deaths_incl_covid,
CAST(SUM((CASE
    WHEN cca.cause_group = 'Natural' THEN cca.count ELSE 0
    END +
CASE
    WHEN cca.cause_group = 'COVID' THEN cca.count ELSE 0
    END +
CASE
    WHEN cca.cause_group = 'Unnatural' THEN cca.count ELSE 0
    END)
        ) AS DOUBLE) AS total_claims,
CAST(cca.service_client_id AS BIGINT) AS service_client_id
FROM "assa_sandbox".v1_1_co6_covid_claims cca
GROUP BY company_code,
data_import_batch,
cca.sex,
cca.smoker_status,
cca.age_band,
cca.socio_economic_class,
DATE_FORMAT(DATE_PARSE(CONCAT(CAST(cca.effective_year_month AS VARCHAR),'01'), '%Y%m%d'), '%Y-%m-%d'),
DATE_FORMAT(DATE_PARSE(CONCAT(CAST(cca.notification_year_month AS VARCHAR),'01'), '%Y%m%d'), '%Y-%m-%d'),
service_client_id

;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.covid_claims;
CREATE TABLE "assa_sandbox".covid_claims
WITH (
	-- priority = 4,
	bucket_count = 0,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
WITH dib_to_use AS (
  SELECT
    company_code,
    MAX (data_import_batch) data_import_batch
  FROM
    "assa-lake".v2_covid_claims
      WHERE company_code not in (6,11,30)
  GROUP BY
    company_code
),
dib_to_use_25_if AS (
  SELECT
    year_month_of_file,
    MAX (data_import_batch) AS max_batch
  FROM
    "assa-lake".v1_covid_inforce_25
  -- #####
  where data_import_batch != '2022-05-10'
  -- ####
  GROUP BY
    year_month_of_file
),
dib_to_use_25_clm AS (
  SELECT
    MAX (data_import_batch) AS max_batch
  FROM
    "assa-lake".v1_covid_claims_25
  -- #####
  where data_import_batch != '2022-05-10'
  -- ####   
)
SELECT
  v2_covid_claims.company_code,
  date_parse (date_data_received, '%Y-%m-%d') AS date_date_received,
  sex,
  REPLACE (smoker_status, 'smoker', 'Smoker') AS smoker_status,
CASE
    WHEN age_band = '60-70' THEN '60 - 69'
    ELSE REGEXP_REPLACE (
      age_band,
      '(?<rstart>\d+)-(?<rend>\d+)',
      '${rstart} - ${rend}'
    )
  END AS age_band,
  -- Make sure dashes have surrounding spaces
  socio_economic_class,
  date_parse (year_and_month_of_date_of_death, '%Y-%m-%d') AS year_and_month_of_date_of_death,
  date_parse (
    NULLIF (year_and_month_of_date_reported, ''),
    '%Y-%m-%d'
  ) AS year_and_month_of_date_reported,
  number_of_natural_deaths_excl_covid,
  number_of_covid_deaths,
  number_of_unnatural_deaths,
  total_number_of_natural_deaths_incl_covid,
  total_claims,
  v2_covid_claims.company_code AS service_client_id
FROM
  "assa-lake".v2_covid_claims
  INNER JOIN dib_to_use ON v2_covid_claims.company_code = dib_to_use.company_code
  AND v2_covid_claims.data_import_batch = dib_to_use.data_import_batch
UNION
ALL
SELECT
  company_code,
  date_parse (data_import_batch, '%Y-%m-%d') date_data_received,
  gender AS sex,
  smoker_status,
CASE
    WHEN age < 20 THEN '<20'
    WHEN age >= 70 THEN '70+'
    ELSE CAST (10 * FLOOR (age / 10) AS VARCHAR) || ' - ' || CAST (10 * (FLOOR (age / 10) + 1) - 1 AS VARCHAR)
  END age_band,
  education AS socio_economic_group,
  date_parse (date_of_death, '%Y-%m-%d') AS year_and_month_of_date_of_death,
  date_parse (date_reported, '%Y-%m-%d') AS year_and_month_of_date_reported,
  SUM (
    CASE
      WHEN natural_or_unnatural = 'Natural'
      AND covid19_ind = 'N' THEN 1
      ELSE 0
    END
  ) + SUM (
    CASE
      WHEN natural_or_unnatural = 'Unknown'
      AND covid19_ind = 'N' THEN 1
      ELSE 0
    END * nat_prop
  ) AS number_of_natural_deaths_excl_covid,
  SUM (
    CASE
      WHEN covid19_ind = 'Y' THEN 1
      ELSE 0
    END
  ) number_of_covid_deaths,
  SUM (
    CASE
      WHEN natural_or_unnatural = 'Unnatural'
      AND covid19_ind = 'N' THEN 1
      ELSE 0
    END
  ) + SUM (
    CASE
      WHEN natural_or_unnatural = 'Unknown'
      AND covid19_ind = 'N' THEN 1
      ELSE 0
    END * (1 - nat_prop)
  ) AS number_of_unnatural_deaths,
  SUM (
    CASE
      WHEN natural_or_unnatural = 'Natural'
      OR covid19_ind = 'Y' THEN 1
      ELSE 0
    END
  ) + SUM (
    CASE
      WHEN natural_or_unnatural = 'Unknown'
      AND covid19_ind = 'N' THEN 1
      ELSE 0
    END * nat_prop
  ) AS number_of_natural_deaths_incl_covid,
  COUNT (*) AS total_claims,
  company_code AS service_client_id
FROM
  (
    SELECT
      /* Allocate Unknowns to Natural/Unnatural in proportion to business as a whole, at gender/age_band granularity */
      dat0.*,
      SUM (
        CASE
          WHEN natural_or_unnatural = 'Natural' THEN 1.0
          ELSE 0.0
        END
      ) OVER (
        PARTITION BY gender,
        LEAST (GREATEST (FLOOR (age / 10), 19), 70)
      ) / SUM (
        CASE
          WHEN natural_or_unnatural != 'Unknown' THEN 1.0
          ELSE 0.0
        END
      ) OVER (
        PARTITION BY gender,
        LEAST (GREATEST (FLOOR (age / 10), 19), 70)
      ) AS nat_prop
    FROM
      (
        SELECT
          clm.company_code,
          clm.data_import_batch,
          lake.policy_number,
          lake.life_number,
          lake.year_month_of_file AS if_ym,
          lake.data_import_batch AS if_dib,
          lake.gender,
          date_of_death,
          date_reported,
          covid19_ind,
          natural_or_unnatural,
          education,
          REPLACE (smoker_status, 'smoker', 'Smoker') AS smoker_status,
          date_diff (
            'year',
            date_parse (date_of_birth, '%Y-%m-%d'),
            date_parse (date_of_death, '%Y-%m-%d')
          ) AS age,
          date_parse (date_of_death, '%Y-%m-%d') AS effective_date
        FROM
          (
            "assa-lake".v1_covid_claims_25 clm
            INNER JOIN dib_to_use_25_clm dib1 ON clm.data_import_batch = dib1.max_batch
          )
          LEFT JOIN (
            "assa-lake".v1_covid_inforce_25 lake
            INNER JOIN dib_to_use_25_if dib2 ON lake.data_import_batch = dib2.max_batch
            AND lake.year_month_of_file = dib2.year_month_of_file
          ) ON lake.policy_number = clm.policy_number
          AND lake.life_number = clm.life_number
        WHERE
          lake.policy_number IS NOT NULL
          and lower(clm.decision) IN ('pending', 'approved')
      ) dat0
  ) dat
  INNER JOIN (
    SELECT
      policy_number,
      life_number,
      MAX (year_month_of_file) max_file,
      MAX(data_import_batch) max_dib
    FROM
      "assa-lake".v1_covid_inforce_25
      -- #####
        where data_import_batch != '2022-05-10'
      -- ####   
    GROUP BY
      policy_number,
      life_number
  ) fil ON dat.policy_number = fil.policy_number
  AND dat.life_number = fil.life_number
  AND dat.if_ym = fil.max_file
  AND dat.if_dib = fil.max_dib
GROUP BY
  company_code,
  date_parse (data_import_batch, '%Y-%m-%d'),
  gender,
  smoker_status,
CASE
    WHEN age < 20 THEN '<20'
    WHEN age >= 70 THEN '70+'
    ELSE CAST (10 * FLOOR (age / 10) AS VARCHAR) || ' - ' || CAST (10 * (FLOOR (age / 10) + 1) - 1 AS VARCHAR)
  END,
  education,
  date_parse (date_of_death, '%Y-%m-%d'),
  date_parse (date_reported, '%Y-%m-%d')
 UNION ALL
 SELECT * FROM "assa_sandbox".v1_2_co6_covid_claims
 UNION ALL
 SELECT * FROM "assa_sandbox".v1_2_co11_covid_claims
 UNION ALL
 SELECT * FROM "assa_sandbox".v1_2_co30_covid_claims
  UNION ALL
 SELECT * FROM "assa_sandbox".v1_2_co18_covid_claims

;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.covid_actual_and_expected_deaths;
CREATE TABLE "assa_sandbox".covid_actual_and_expected_deaths
WITH (
	-- priority = 5,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
WITH
        date_bounds
        AS
            (  SELECT                                            /* Split by company code and use analytic min/max to only use months available for all cos */
                      company_code
                     ,MIN (year_and_month_of_date_of_death)    AS min_death
                     ,MAX (year_and_month_of_date_of_death)    AS max_death
                     ,sequence (MIN (date_trunc ('month', year_and_month_of_date_of_death))
                               ,                                 --MAX (MIN (date_trunc ('month', year_and_month_of_date_of_death))) OVER (PARTITION BY NULL),
                                MAX (date_trunc ('month', year_and_month_of_date_of_death))
                               ,                                 --MIN (MAX (date_trunc ('month', year_and_month_of_date_of_death))) OVER (PARTITION BY NULL),
                                INTERVAL '1' MONTH)            AS month_seq
                 FROM assa_sandbox.covid_claims
             GROUP BY company_code),
        dates
        AS
            (SELECT DISTINCT clm_month
               FROM date_bounds CROSS JOIN unnest(month_seq) as t(clm_month)),
        avail_dates
        AS
            (  SELECT d1.company_code
                     ,clm_month
                     ,MAX (d1.effective_date)                                                                                AS prior_date
                     ,MIN (d2.effective_date)                                                                                AS next_date
                     ,date_diff ('month', date_trunc ('month', MAX (d1.effective_date)) + INTERVAL '1' MONTH, clm_month)     AS prior_lag
                     ,date_diff ('month', clm_month, date_trunc ('month', MIN (d2.effective_date)) + INTERVAL '1' MONTH)     AS next_lag
                 FROM dates
                      INNER JOIN assa_sandbox.covid_exposure d1 ON date_trunc ('month', d1.effective_date) + INTERVAL '1' MONTH <= clm_month
                      LEFT JOIN assa_sandbox.covid_exposure d2
                          ON date_trunc ('month', d2.effective_date) + INTERVAL '1' MONTH > clm_month AND d1.company_code = d2.company_code
             GROUP BY d1.company_code, clm_month),
        smoker_pcts
        AS
            (  SELECT claims.sex
                     ,claims.age_band
                     ,CASE
                          WHEN SUM (COALESCE (total_claims, 0)) = 0
                          THEN
                              0
                          ELSE
                                SUM (CASE WHEN smoker_status = 'Smoker' THEN CAST (COALESCE (total_claims, 0) AS DOUBLE) ELSE 0 END)
                              / SUM (COALESCE (total_claims, 0))
                      END    AS clm_smoker_pct
                 FROM assa_sandbox.covid_claims claims
                WHERE claims.smoker_status IN ('Smoker', 'Non-Smoker')
             GROUP BY claims.sex, claims.age_band),
        exposure
        AS
            (SELECT cur_year.company_code
                   ,cur_year.sex
                   ,cur_year.smoker_status
                   ,cur_year.age_band
                   ,cur_year.socio_economic_group
                   ,avail_dates.clm_month
                   ,  (1.0 / 12.0)
                    * CASE
                          WHEN next_year.effective_date IS NULL THEN cur_year.number_of_policies
                          ELSE (cur_year.number_of_policies * next_lag + next_year.number_of_policies * prior_lag) / (prior_lag + next_lag)
                      END    AS life_years
               FROM avail_dates
                    INNER JOIN assa_sandbox.covid_exposure cur_year
                        ON avail_dates.prior_date = cur_year.effective_date AND avail_dates.company_code = cur_year.company_code
                    LEFT JOIN assa_sandbox.covid_exposure next_year
                        ON     cur_year.company_code = next_year.company_code
                           AND cur_year.sex = next_year.sex
                           AND cur_year.smoker_status = next_year.smoker_status
                           AND cur_year.age_band = next_year.age_band
                           AND cur_year.socio_economic_group = next_year.socio_economic_group
                           AND avail_dates.next_date = next_year.effective_date)
    SELECT exposure.company_code
          ,exposure.sex
          ,exposure.smoker_status
          ,exposure.age_band
          ,exposure.socio_economic_group
          ,clm_month
          ,life_years
          ,0.0                       AS natural_deaths_excl_covid
          ,0.0                       AS covid_deaths
          ,0.0                       AS unnatural_deaths
          ,0.0                       AS total_deaths
          ,'exp'                     AS rec_type
          ,exposure.company_code     AS service_client_id
      FROM exposure
     WHERE lower(smoker_status) IN ('smoker', 'non-smoker')
    UNION ALL
    SELECT claims.company_code
          ,claims.sex
          ,claims.smoker_status
          ,claims.age_band
          ,claims.socio_economic_class                                      AS socio_economic_group
          ,date_trunc ('month', claims.year_and_month_of_date_of_death)     AS clm_month
          ,0                                                                AS life_years
          ,COALESCE (number_of_natural_deaths_excl_covid, 0)                AS natural_deaths_excl_covid
          ,COALESCE (number_of_covid_deaths, 0)                             AS covid_deaths
          ,COALESCE (number_of_unnatural_deaths, 0)                         AS unnatural_deaths
          ,COALESCE (total_claims, 0)                                       AS total_deaths
         ,'clm'                                                            AS rec_type
          ,claims.company_code                                              AS service_client_id
      FROM assa_sandbox.covid_claims claims
     WHERE lower(claims.smoker_status) IN ('smoker', 'non-smoker')
    /* Append reallocated unknown smoker_status claims to smoker and non-smoker */
    UNION ALL
    SELECT claims.company_code
          ,claims.sex
          ,'Smoker'                                                               AS smoker_status
          ,claims.age_band
          ,claims.socio_economic_class                                            AS socio_economic_group
          ,date_trunc ('month', claims.year_and_month_of_date_of_death)           AS clm_month
          ,0                                                                      AS life_years
          ,COALESCE (number_of_natural_deaths_excl_covid, 0) * clm_smoker_pct     AS natural_deaths_excl_covid
          ,COALESCE (number_of_covid_deaths, 0) * clm_smoker_pct                  AS covid_deaths
          ,COALESCE (number_of_unnatural_deaths, 0) * clm_smoker_pct              AS unnatural_deaths
          ,COALESCE (total_claims, 0) * clm_smoker_pct                            AS total_deaths
          ,'clm'                                                                  AS rec_type
          ,claims.company_code                                                    AS service_client_id
      FROM assa_sandbox.covid_claims claims INNER JOIN smoker_pcts ON claims.age_band = smoker_pcts.age_band AND claims.sex = smoker_pcts.sex
     WHERE lower(claims.smoker_status) NOT IN ('smoker', 'non-smoker')
    UNION ALL
    SELECT claims.company_code
          ,claims.sex
          ,'Non-Smoker'                                                                 AS smoker_status
          ,claims.age_band
          ,claims.socio_economic_class                                                  AS socio_economic_group
          ,date_trunc ('month', claims.year_and_month_of_date_of_death)                 AS clm_month
          ,0                                                                            AS life_years
          ,COALESCE (number_of_natural_deaths_excl_covid, 0) * (1 - clm_smoker_pct)     AS natural_deaths_excl_covid
          ,COALESCE (number_of_covid_deaths, 0) * (1 - clm_smoker_pct)                  AS covid_deaths
          ,COALESCE (number_of_unnatural_deaths, 0) * (1 - clm_smoker_pct)              AS unnatural_deaths
          ,COALESCE (total_claims, 0) * (1 - clm_smoker_pct)                            AS total_deaths
          ,'clm'                                                                        AS rec_type
          ,claims.company_code                                                          AS service_client_id
      FROM assa_sandbox.covid_claims claims INNER JOIN smoker_pcts ON claims.age_band = smoker_pcts.age_band AND claims.sex = smoker_pcts.sex
     WHERE lower(claims.smoker_status) NOT IN ('smoker', 'non-smoker')


;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.covid_standardised_ave;
CREATE TABLE "assa_sandbox".covid_standardised_ave
WITH (
	-- priority = 6,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
WITH
        covid_months
        AS
            (SELECT DISTINCT clm_month
               FROM assa_sandbox.covid_actual_and_expected_deaths
              WHERE clm_month >= DATE '2020-01-01'), -- DATE '2020-03-01'),
       covid_groups
        AS
            (SELECT DISTINCT company_code
                            ,sex
               ,smoker_status
                            ,age_band
                            ,socio_economic_group
             FROM assa_sandbox.covid_actual_and_expected_deaths),
        monthly_expected_rates
        AS
            (  SELECT company_code
                     ,EXTRACT (MONTH FROM clm_month)      month_of_year
                     ,sex
 ,age_band
                     ,smoker_status
                     ,SUM (total_deaths)                  total_deaths
                     ,SUM (smoothed_total_deaths)         smoothed_total_deaths
                     ,SUM (unnatural_deaths)              unnatural_deaths
                     ,SUM (smoothed_unnatural_deaths)     smoothed_unnatural_deaths
                     ,SUM (life_years)                    life_years
                     ,SUM (smoothed_life_years)           smoothed_life_years
                 FROM (  SELECT company_code
                               ,clm_month
                               ,sex
                               ,age_band
                             ,smoker_status
                               ,SUM (total_deaths)                                                     total_deaths
                               ,SUM (SUM (total_deaths))
                                    OVER (PARTITION BY company_code, sex, age_band, smoker_status ORDER BY clm_month ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)    smoothed_total_deaths
                               ,SUM (unnatural_deaths)                                           unnatural_deaths
                               ,SUM (SUM (unnatural_deaths))
                                    OVER (PARTITION BY company_code, sex, age_band, smoker_status ORDER BY clm_month ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)    smoothed_unnatural_deaths
                               ,SUM (life_years)                                     life_years
                               ,SUM (SUM (life_years))
                                    OVER (PARTITION BY company_code, sex, age_band, smoker_status ORDER BY clm_month ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)    smoothed_life_years
                           FROM assa_sandbox.covid_actual_and_expected_deaths
                       WHERE clm_month < DATE '2020-01-01' -- DATE '2020-03-01'
                       GROUP BY company_code
               ,clm_month
                               ,sex
                               ,age_band
                           ,smoker_status
                         HAVING SUM (life_years) > 0)
             GROUP BY company_code
           ,EXTRACT (MONTH FROM clm_month)
                     ,sex
                     ,age_band
                     ,smoker_status),
        covid_actuals
        AS
            (  SELECT covid_months.clm_month
                     ,EXTRACT (MONTH FROM covid_months.clm_month)                           month_of_year
                     ,exps.company_code
                     ,exps.sex
                     ,exps.smoker_status
                     ,exps.age_band
 ,exps.socio_economic_group
                     ,SUM (COALESCE (life_years, 0))                                       life_years
                     ,SUM (COALESCE (total_deaths, 0))                                      total_deaths
 ,SUM (COALESCE (natural_deaths_excl_covid, 0))                         natural_deaths_excl_covid
                     ,SUM (COALESCE (covid_deaths, 0))                                      covid_deaths
                     ,SUM (COALESCE (unnatural_deaths, 0))                                  unnatural_deaths
                     ,SUM (SUM (total_deaths)) OVER (PARTITION BY exps.company_code, covid_months.clm_month)  cont_total_claims
                 FROM (covid_months CROSS JOIN covid_groups)
                      LEFT JOIN assa_sandbox.covid_actual_and_expected_deaths exps
 ON     exps.clm_month = covid_months.clm_month
                             AND exps.company_code = covid_groups.company_code
                             AND exps.sex = covid_groups.sex
                             AND exps.smoker_status = covid_groups.smoker_status
                             AND exps.age_band = covid_groups.age_band
           AND exps.socio_economic_group = covid_groups.socio_economic_group
             GROUP BY covid_months.clm_month
     ,exps.company_code
                     ,exps.sex
                     ,exps.smoker_status
                     ,exps.age_band
                     ,exps.socio_economic_group)
                     --select sex, smoker_status, age_band, sum(total_Deaths), sum(life_years) from covid_actuals where company_code = 6 group by sex, smoker_status, age_Band
      SELECT act.company_code
            ,act.sex
            ,act.smoker_status
            ,act.age_band
   ,act.socio_economic_group
            ,act.clm_month
            ,act.clm_month                                     AS month_final
            ,SUM (act.life_years)                                                      AS life_years
 ,SUM (act.natural_deaths_excl_covid)                                       AS natural_deaths_excl_covid
            ,SUM (act.covid_deaths)                                                    AS covid_deaths
            ,SUM (act.unnatural_deaths)                                                AS unnatural_deaths
            ,SUM (act.total_deaths)   AS total_deaths
            ,SUM (exps.total_deaths) / SUM (exps.life_years)                           AS expected_death_rate
            ,SUM (exps.smoothed_total_deaths) / SUM (exps.smoothed_life_years)         AS smoothed_total_death_rate
            ,SUM (exps.unnatural_deaths) / SUM (exps.life_years)                       AS expected_unnatural_rate
            ,SUM (exps.smoothed_unnatural_deaths) / SUM (exps.smoothed_life_years)     AS smoothed_unnatural_death_rate
            ,CAST(act.company_code AS VARCHAR)                                         AS service_client_id
 FROM covid_actuals act
             LEFT JOIN monthly_expected_rates exps
                 ON act.company_code = exps.company_code AND act.sex = exps.sex AND act.age_band = exps.age_band AND act.month_of_year = exps.month_of_year AND act.smoker_status = exps.smoker_status
       WHERE act.sex IS NOT NULL AND act.cont_total_claims >= 50 -- Exclude months where very few claims have been reported yet
    GROUP BY act.company_code
            ,act.sex
            ,act.smoker_status
            ,act.age_band
            ,act.socio_economic_group
            ,act.clm_month
       --,exps.total_deaths
            --,exps.unnatural_deaths
            --,exps.smoothed_total_deaths
            --,exps.smoothed_unnatural_deaths
            --,exps.life_years
            --,exps.smoothed_life_years
   UNION ALL
      SELECT 901                                                                       AS company_code
            ,act.sex
            ,act.smoker_status
            ,act.age_band
            ,act.socio_economic_group
           ,act.clm_month
            ,act.clm_month                                                             AS month_final
            ,SUM (act.life_years)                                                      AS life_years
            ,SUM (act.natural_deaths_excl_covid)                                       AS natural_deaths_excl_covid
            ,SUM (act.covid_deaths)                                                    AS covid_deaths
            ,SUM (act.unnatural_deaths)                                                AS unnatural_deaths
            ,SUM (act.total_deaths)       AS total_deaths
            ,SUM (exps.total_deaths) / SUM (exps.life_years)                           AS expected_death_rate
            ,SUM (exps.smoothed_total_deaths) / SUM (exps.smoothed_life_years)         AS smoothed_total_death_rate
            ,SUM (exps.unnatural_deaths) / SUM (exps.life_years)                       AS expected_unnatural_rate
            ,SUM (exps.smoothed_unnatural_deaths) / SUM (exps.smoothed_life_years)     AS smoothed_unnatural_death_rate
            ,'901'                                                                     AS service_client_id
 FROM covid_actuals act
             INNER JOIN (SELECT company_code, MIN(MAX(clm_month)) OVER () last_month FROM covid_actuals WHERE total_deaths > 0 GROUP BY company_code) max_month
                 ON act.company_code = max_month.company_code AND act.clm_month <= max_month.last_month
             LEFT JOIN monthly_expected_rates exps
             ON act.company_code = exps.company_code AND act.sex = exps.sex AND act.age_band = exps.age_band AND act.month_of_year = exps.month_of_year AND act.smoker_status = exps.smoker_status
       WHERE act.sex IS NOT NULL AND act.clm_month <= DATE '2023-07-31' --temporary limit
    GROUP BY act.sex
            ,act.smoker_status
            ,act.age_band
            ,act.socio_economic_group
            ,act.clm_month
            --,exps.total_deaths
            --,exps.unnatural_deaths
            --,exps.smoothed_total_deaths
            --,exps.smoothed_unnatural_deaths
            --,exps.life_years
            --,exps.smoothed_life_years
    -- HAVING SUM(act.life_years) > 0


;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.samrc_excess;
CREATE TABLE "assa_sandbox".samrc_excess
WITH (
	-- priority = 7,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array['service_client_id']
)
AS
WITH initdata AS (
    SELECT company_code
          ,'Population Deaths Relative to Expected'                 AS series
          ,date_parse (dth_date, '%Y-%m-%d') + INTERVAL '3' DAY     AS clm_month
          ,excess_deaths
          ,pred_deaths
          ,month_lims.service_client_id
      FROM "assa-lake".v1_samrc_excess
           CROSS JOIN (SELECT DISTINCT company_code, service_client_id
                         FROM assa_sandbox.covid_standardised_ave) month_lims
     WHERE data_import_batch = (SELECT MAX (data_import_batch) FROM "assa-lake".v1_samrc_excess)
    UNION ALL
    SELECT company_code
          ,'Baseline'                 AS series
          ,date_parse (dth_date, '%Y-%m-%d') + INTERVAL '3' DAY     AS clm_month
          ,0                                                        AS excess_deaths
          ,1                                                        AS pred_deaths
          ,month_lims.service_client_id
      FROM "assa-lake".v1_samrc_excess
           CROSS JOIN (SELECT DISTINCT company_code, service_client_id
                         FROM assa_sandbox.covid_standardised_ave) month_lims
     WHERE data_import_batch = (SELECT MAX (data_import_batch) FROM "assa-lake".v1_samrc_excess)
    UNION ALL
    SELECT company_code
                     ,'Insured Deaths Relative to Expected'                             AS series
                     ,month_final + INTERVAL '15' DAY                                   AS clm_month
                     ,SUM (total_deaths - (life_years * smoothed_total_death_rate))     AS excess_deaths
                     ,SUM (life_years * smoothed_total_death_rate)                      AS pred_deaths
                     ,service_client_id
                 FROM assa_sandbox.covid_standardised_ave
             GROUP BY company_code, month_final, service_client_id),
daterange AS (
    SELECT sequence(MIN(clm_month), MAX(clm_month), INTERVAL '1' DAY) AS dr FROM initdata
),
dateinterp AS (
    SELECT  pd.company_code
           ,pd.series
           ,ind_date
           ,pd.service_client_id
           ,MAX(pd.clm_month) AS prior_date
           ,MIN(nd.clm_month) AS next_date
           ,MAX_BY(pd.excess_deaths, pd.clm_month) AS prior_excess
           ,MAX_BY(pd.pred_deaths, pd.clm_month) AS prior_pred
           ,MIN_BY(nd.excess_deaths, nd.clm_month) AS next_excess
           ,MIN_BY(nd.pred_deaths, nd.clm_month) AS next_pred
           FROM daterange
           CROSS JOIN UNNEST(daterange.dr) AS ind_dates(ind_date)
           LEFT JOIN (initdata AS pd INNER JOIN initdata AS nd
           ON pd.company_code = nd.company_code AND pd.series = nd.series)
           ON ind_dates.ind_date >= pd.clm_month AND ind_dates.ind_date <= nd.clm_month
           GROUP BY pd.company_code, pd.series, ind_date, pd.service_client_id)
    SELECT company_code
          ,series
          ,ind_date                                                                                                                                AS clm_month
          ,prior_excess
          ,next_excess
          ,prior_pred
          ,next_pred
          ,prior_date
          ,next_date
          ,CASE
               WHEN prior_date != next_date THEN to_milliseconds (ind_date - prior_date) / CAST (to_milliseconds (next_date - prior_date) AS DOUBLE)
               ELSE 0
           END         AS interp_fac
          ,service_client_id
      FROM dateinterp


;
----------------------------------------------------
DROP TABLE IF EXISTS `assa_sandbox`.latest_process_times_per_company;
CREATE TABLE "assa_sandbox".latest_process_times_per_company
WITH (
	-- priority = 20,
	bucket_count = 20,
	bucketed_by = array[],
	partitioned_by = array[]
)
AS
select 
  '6' as company_code, 
  max(process_number) as latest_process_time
from "assa-lake".v1_co6_covid_claims_source_files
UNION ALL 
select 
  '30' as company_code, 
  max(process_number) as latest_process_time
from "assa-lake".v1_co30_covid_claims_source_files
UNION ALL 
select 
  '11' as company_code, 
  max(process_number) as latest_process_time
from "assa-lake".v1_co11_covid_claims_source_files
UNION ALL 
select 
  '18' as company_code, 
  max(process_number) as latest_process_time
from "assa-lake".v1_sandbox_co18_covid_claims
UNION ALL 
select 
  '25' as company_code, 
  max(process_number) as latest_process_time
from "assa-lake".v1_covid_claims_25
UNION ALL 
select 
  'samrc' as company_code, 
  max(process_number) as latest_process_time
from "assa-lake".v1_samrc_excess

;