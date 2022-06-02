This file describes manipulations and assumptions made to transform the raw data for the CSI study and calculate exposure

# 00 data to sandbox

These apply to companies other than 18

* Clean up bad movement codes such as `X0`, `XNULL`, `XXXX` etc. These are assumed to indicate new business (code `000`). Although the spec uses three digit movement codes, internally the SQL only keeps the last two digits.
* For movement code `010` (new calendar year on existing policy), movement dates of 31-Dec are moved to 1-Jan.
* Company 11 - certain policies issued prior to 2003 have no exposure before 2009, but then suddenly appear. These are assumed to be errors and removed.
* Company 25 - life numbers and dates of birth changed around 2012. It was assumed that the suffix part of the life number remained consistent, although there are some inconsistencies
* Company 25 - changes to data format in new New Gen files!?!?!
* 

# 02 calculate exposure

## Companies other than 18:

The movement records provided are usually pretty messy, with backdated, simultaneous, inconsistent, or missing records being frequent. There are many violations of the data spec in terms of movement timing - in particular notes (e) and (h) in the movements spec. In order to calculate exposure correctly we need to try to deal with all of these issues. The following algorithm is used:
1. First try to find termination markers:
   1. Set `prior_termination` to sum of `direction_of_movement` for all prior records (in terms of `movementcounter`) with a termination movement code (`030`, `043`, `044`, `050`). This will, for example, give -1 if there was any termination record before the current record. A subsequent reinstatement will have a `direction_of_movement` of 1, so if both termination and reinstatement records exist then `prior_termination` should equal 0.
   2. Set `prior_claim` in the same way, but only considering code `030`. 
   3. Set `reversal_follows` in the same way as `prior_claim`, but considering *future* records. This lets us determine if a current claim might have been reversed at a future date (if the value is positive)
   4. Set `current_termination` in the same way as `prior_termination`, but instead of considering records earlier in the count, consider all records with the same `effective_date_of_change_movement`. If the current record is a termination, this allows us to check that whether it was also reversed with the same effective date (0), 
2. For each movement, find `next_movement` - this gives us the latest possible end of the current exposure period.
   1. First, remove any records where `prior_termination` above is negative (so there are more termination records than reinstatements for this policy), unless the current record is a claim. If `prior_claim` is negative, always remove the record (as we can't have multiple claims)
   2. Then, find the next movement, ordering by `effective_date_of_change_movement` and `movementcounter`, in that order.
   3. If there is no subsequent record by the above criterion, but `current_termination` is negative, then this is effectively the last record, so use current `effective_date_of_change_movement`
   4. If this last available record is not in the last year of the study then we assume it must have terminated some time between `effective_date_of_change_movement` and the end of the associated year. We take the half-way point as the movement date.
   5. If this is the last year of the study and there are no more records, then assume the policy was still in force at the end of the year, and use the study end date as the next movement.
3. For each record returned by the prior step, find all calendar and policy years falling between `effective_date_of_change_movement` and `next_movement`. This backfills for missing years (eg `effective_date_of_change_movement` = '2015-03-01' and `next_movement` = '2018-03-01' will lead to records for 2015, 2016, 2017 and 2018). There will be additional duplication as each calendar year is associated with two policy years - the anniversary in the previous calendar year as well as the anniversary in the current year. We do this because each calendar year will usually need to be split into at least two rate intervals - before and after policy anniversary.
4. Calculate the following for each resulting record:
   * `policy_anniversary`: Date of most recent policy anniversary
   * `cy_anniversary`: Date of policy anniversary in current calendar year
   * `next_policy_anniversary`: `policy_anniversary` plus 1 year.
5. For each record returned by the prior step we determine an exposure period that will not overlap with prior or following exposure periods (noting the "duplicate" records created in the prior step). We also want to make sure we are using separate records after each 1 Jan and each policy anniversary. We calculate:
   * `begin_date`: latest of `effective_date_of_change_movement`, `policy_anniversary` and 1 Jan of current calendar year.
   * `end_date`: earliest of `next_movement`, `next_policy_anniversary` and 1 Jan of following calendar year
6. Exposure in days associated with each record is then `end_date - begin_date`.

## Company 18:

* Used the provided age_last and age_nearest for both age as at 1 Jan and age as at PA.
* Used exposure figures provided for both exact and '365.25 days in year exposure'
* Treated sa_exposure as if exposure was central exposed to risk
* All claims labeled as Unspecified cause
* Issue Year calculated as `year_of_invest - duration`
