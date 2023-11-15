with source as (
      select * from {{ source('voteview', 'votes') }}
),
renamed as (
    select
        {{ adapter.quote("congress") }},
        {{ adapter.quote("chamber") }},
        {{ adapter.quote("rollnumber") }},
        cast({{ adapter.quote("icpsr") }} as int64) as icpsr,
        cast({{ adapter.quote("cast_code") }} as int64) as cast_code,
        {{ adapter.quote("prob") }},
        {{ adapter.quote("vote_detail") }},
        {{ adapter.quote("vote") }}

    from source
)
select * from renamed
  