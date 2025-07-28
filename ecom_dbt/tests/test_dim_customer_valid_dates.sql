-- Test that customer SCD2 dates are valid (valid_from < valid_to)
select *
from {{ ref('dim_customer') }}
where valid_from >= valid_to
  and valid_to is not null 