with source as (
      select * from {{ source('voteview', 'parties') }}
),
renamed as (
    select
        {{ adapter.quote("congress") }},
        {{ adapter.quote("chamber") }},
        {{ adapter.quote("party_code") }},
        {{ adapter.quote("party_name") }},
        {{ adapter.quote("n_members") }}

    from source
)
select * from renamed
  