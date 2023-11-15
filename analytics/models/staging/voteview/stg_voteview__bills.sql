with source as (
      select * from {{ source('voteview', 'bills') }}
),
renamed as (
    select
        {{ adapter.quote("congress") }},
        {{ adapter.quote("chamber") }},
        {{ adapter.quote("rollnumber") }},
        {{ adapter.quote("date") }},
        timestamp({{ adapter.quote("date") }}) as date_ts,
        {{ adapter.quote("bill_number") }},
        {{ adapter.quote("vote_result") }},
        {{ adapter.quote("vote_desc") }},
        {{ adapter.quote("vote_question") }},
        {{ adapter.quote("dtl_desc") }}

    from source
)
select * from renamed
  