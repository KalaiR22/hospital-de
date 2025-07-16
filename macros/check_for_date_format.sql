{%test check_for_date_format(model, column_name,date_format)%}
select * from {{model}}
where {{column_name}} !=  to_date({{ column_name }})
{%endtest%}