create table mortality_sandbox.assa_new_gen_Expected_std_w_sa_bands
	WITH (
			format = 'ORC'
			,orc_compression = 'ZLIB'
			,partitioned_by = ARRAY ['calendar_year', 'company_code', 'service_client_id']
			,bucketed_by = ARRAY ['policy_year','sex']
			,bucket_count = 25
			) AS
with cpi_idx as (
    select
        *
    from
        UNNEST(
            map_from_entries(
                ARRAY [
        (2000, 0.567),
        (2001, 0.602),
        (2002, 0.649),
        (2003, 0.694),
        (2004, 0.700),
        (2005, 0.721),
        (2006, 0.756),
        (2007, 0.809),
        (2008, 0.897),
        (2009, 0.963),
        (2010, 1.000),
        (2011, 1.051),
        (2012, 1.111),
        (2013, 1.170),
        (2014, 1.251),
        (2015, 1.308),
        (2016, 1.394),
        (2017, 1.464),
        (2018, 1.528),
        (2019, 1.595),
        (2020, 1.630),
        (2021, 1.713),
        (2022, 1.840),
        (2023, 1.945),
        (2024, 2.041),
        (2025, 2.103)
      ]
            )
        ) as t (calendar_year, cpi)
),
sa_bands as (
    select
        company_code,
        policy_number,
        life_number,
        sum_assured,
        sum_assured / cpi.cpi adjusted_sum_assured
    from
        (
            select
                company_code,
                calendar_year,
                policy_number,
                coalesce(life_number, 'blank') life_number,
                sum_assured,
                row_number() over (
                    partition by company_code,
                    policy_number,
                    life_number
                    order by
                        effective_date_of_change_movement
                ) rn
            FROM
                mortality_insights.assa_new_gen_expected_std
            where
                company_code != 18
        ) data
        left join cpi_idx cpi on cpi.calendar_year = data.calendar_year
    where
        rn = 1
)
select
    least(
        floor(sa_bands.adjusted_sum_assured / 500000) * 500000,
        7000000
    ) sa_band,
    data.*
from
    mortality_insights.assa_new_gen_expected_std data
    left join sa_bands on data.company_code = sa_bands.company_code
    and data.policy_number = sa_bands.policy_number
    and coalesce(data.life_number, 'blank') = sa_bands.life_number