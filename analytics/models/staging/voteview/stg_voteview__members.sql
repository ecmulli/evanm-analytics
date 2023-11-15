with source as (
      select * from {{ source('voteview', 'members') }}
),
renamed as (
    select
        {{ adapter.quote("congress") }},
        {{ adapter.quote("chamber") }},
        {{ adapter.quote("icpsr") }},
        {{ adapter.quote("state_abbrev") }},
        cast({{ adapter.quote("district_code") }} as int64) as district_code,
        {{ adapter.quote("bioname") }},
        {{ adapter.quote("party_code") }}

    from source
)
select * from renamed
  