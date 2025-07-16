{%test check_for_mail_format(model, column_name,mail_format)%}
select * from {{model}}
where {{column_name}} not like '%{{mail_format}}'
{%endtest%}