with source as (

    select * from "FBS_DB"."CRM"."Users"

),

renamed as (

    select
        "User_UserId"       as user_id,
        "User_FirstName"    as first_name,
        "User_LastName"     as last_name,
        "User_Logon"        as logon_id,
        concat("User_FirstName", ' ', "User_LastName") as full_name,
        regexp_replace("User_Logon"::text, '[^0-9]', '', 'g') as employee_id
    from source

)

select * from renamed