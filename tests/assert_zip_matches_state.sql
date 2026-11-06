{{ config(severity='warn') }}

select u.user_id, u.state, u.zip_code
from {{ ref('stg_users') }} u
left join {{ ref('zip_ranges') }} z
  on u.state = z.state
 and cast(u.zip_code as integer) between z.zip_min and z.zip_max
where z.state is null
