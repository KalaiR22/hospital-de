{%test phone_number_check(model, column_name)%}
select * from {{model}}
where length({{column_name}}) != 10
{%endtest%}