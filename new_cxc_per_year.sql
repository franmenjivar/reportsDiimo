select month(x.accepted_at), sum(interesDevengado), sum(InteresPagado) from (
select l.id,l.accepted_at, l.status, l.interest_type, l.capital, l.commission,
case when lr.CapitalPagado is null then 0 else lr.CapitalPagado end as CapitalPagado,
case when lr.InteresPagado is null then  0 else lr.InteresPagado end  as InteresPagado,
case when lr.IVAInteresPagado is null then 0 else lr.IVAInteresPagado end as IVAInteresPagado,
case when lr.moraPagada is null then 0 else lr.moraPagada end as moraPagada,
case when lr.IVAMoraPagada is null then 0 else lr.IVAMoraPagada end as IVAMoraPagada,
case when lr.comisionPagada is null then 0 else lr.comisionPagada end as comisionPagada,
case when lr.IVAComisionPagada is null then 0 else lr.IVAComisionPagada end as IVAComisionPagada,

case when l.status = 5 then lr.CapitalPagado when lr.CapitalPagado is null then ld.capitalAPagar else  round(lr.CapitalPagado + ld.capitalAPagar,3) end as capitalDevengado,
case when l.status = 5 then lr.InteresPagado when lr.InteresPagado is null then ld.interesAPagar  else  round(lr.InteresPagado + ld.interesAPagar,3) end as interesDevengado,
case when l.status = 5 then lr.IVAInteresPagado when lr.IVAInteresPagado is null then ld.IVAInteresAPagar  else  round(lr.IVAInteresPagado + ld.IVAInteresAPagar,3) end as IVAInteresDevengado,
case when l.status = 5 then lr.moraPagada when lr.moraPagada is null then round(ld.MoraConIvaApagar/1.13,3) else  round(lr.moraPagada + ld.MoraConIvaApagar/1.13,3) end as moraDevengada,
case when l.status = 5 then lr.IVAMoraPagada when lr.IVAMoraPagada is null then round((ld.MoraConIvaApagar-ld.MoraConIvaApagar/1.13),3)   else  round(lr.IVAMoraPagada + (ld.MoraConIvaApagar-ld.MoraConIvaApagar/1.13),3) end as IVAMoraDevengado,
case when l.status = 5 then lr.comisionPagada when lr.comisionPagada is null then ld.comisionAPagar  else  round(lr.comisionPagada + ld.comisionAPagar,3) end as comisionDevengada,
case when l.status = 5 then lr.IVAComisionPagada when lr.IVAComisionPagada is null then  ld.IVAComisionAPagar else  round(lr.IVAComisionPagada + ld.IVAComisionAPagar,3) end as IVAComisionDevengada

From (
select l.id,l.accepted_at, l.status, l.interest_type, round(l.amount/100,0) as capital, round(l.commission/100,2) as commission  from diimo_core.loans as l
where l.accepted_at is not null and l.status in (3,5,6,11,12) and accepted_at > '2023-01-01' and accepted_at < '2024-01-01'
) as l
left join (
select loan_id, round(sum(lr.principal_amount)/100,3) as CapitalPagado,
round(sum(lr.interest_amount)/100,3) as InteresPagado,
round(sum(lr.interest_tax_amount)/100,3) as IVAInteresPagado,
round(sum(lr.overdue_amount)/100,3) as moraPagada,
round(sum(lr.overdue_tax_amount)/100,3) as IVAMoraPagada,
round(sum(lr.commission_amount)/100,3) as comisionPagada,
round(sum(lr.commission_tax_amount)/100,3) as IVAComisionPagada
from diimo_core.loan_repayments as lr
where deleted_at is null and lr.repayment_date >= '2023-01-01' and lr.repayment_date < '2024-01-01'
group by lr.loan_id
) as lr on lr.loan_id = l.id

left join (

select ld.loan_id,
case when sum(lr1.CapitalPagado) is null then round(sum(ld.principal_amount)/100  + sum(principal_expected)/100,3) else round(sum(ld.principal_amount)/100  + sum(principal_expected)/100 -sum(lr1.CapitalPagado),3) end as capitalAPagar,
case when round(sum(ld.interest_amount)/100 + (sum(interest_expected)/1.13)/100-sum(lr1.InteresPagado),3) is null then round(sum(ld.interest_amount)/100 + (sum(interest_expected)/1.13)/100,3) 
when sum(lr1.InteresPagado) = 0 and sum(lr1.CapitalPagado) > 0 then 0
else round(sum(ld.interest_amount)/100 + (sum(interest_expected)/1.13)/100-sum(lr1.InteresPagado),3) end as interesAPagar,
case when round(sum(ld.iva_interest_amount)/100 +  (sum(interest_expected)-(sum(interest_expected)/1.13))/100-sum(lr1.IVAInteresPagado),3) is null then round(sum(ld.iva_interest_amount)/100 +  (sum(interest_expected)-(sum(interest_expected)/1.13))/100,3)
when sum(lr1.IVAInteresPagado) = 0 and sum(lr1.CapitalPagado) > 0 then 0
else round(sum(ld.iva_interest_amount)/100 +  (sum(interest_expected)-(sum(interest_expected)/1.13))/100-sum(lr1.IVAInteresPagado),3) end as IVAInteresAPagar,
case when round(sum(ld.commission_amount)/100-sum(lr1.comisionPagada),3) is null then round(sum(ld.commission_amount)/100,3) else round(sum(ld.commission_amount)/100-sum(lr1.comisionPagada),3) end as comisionAPagar,
case when round(sum(ld.iva_commission_amount)/100-sum(lr1.IVAComisionPagada),3) is null then round(sum(ld.iva_commission_amount)/100,3) else round(sum(ld.iva_commission_amount)/100-sum(lr1.IVAComisionPagada),3) end as IVAComisionAPagar,
case when round(sum(ld.due)/100-sum(ld.debt_paid)/100,3) is null then round(sum(ld.due)/100,3) else round(sum(ld.due)/100-sum(ld.debt_paid)/100,3) end  as MoraConIvaApagar
from diimo_core.loan_details as ld
left join (
select 
lr.loan_id,lr.installment_number, round(sum(lr.principal_amount)/100,3) as CapitalPagado,
round(sum(lr.interest_amount)/100,3) as InteresPagado,
round(sum(lr.interest_tax_amount)/100,3) as IVAInteresPagado,
round(sum(lr.overdue_amount)/100,3) as moraPagada,
round(sum(lr.overdue_tax_amount)/100,3) as IVAMoraPagada,
round(sum(lr.commission_amount)/100,3) as comisionPagada,
round(sum(lr.commission_tax_amount)/100,3) as IVAComisionPagada
from diimo_core.loan_repayments as lr
where lr.deleted_at is null and lr.repayment_date >= '2023-01-01' and lr.repayment_date < '2024-01-01'
group by lr.loan_id,lr.installment_number 
) as lr1 on lr1.loan_id = ld.loan_id and ld.number_fee = lr1.installment_number
where ld.expire_at < '2024-01-01'  
group by ld.loan_id


) as ld on ld.loan_id = l.id ) as x 
group by month(x.accepted_at)
-- where  l.id = 'dbb5c975-ded2-412b-aee8-38e88efe6208'



select 
lr.loan_id as loan_id,
loans.status,
sum(interest_amount)/100 as InteresPagado,
sum(ex.interest_expected) devengadoSinPagar, 
((sum(interest_amount)/100) +sum(ex.interest_expected)) as TotalInteresDevengado,
sum(interest_tax_amount)/100 as IvaInteresPagado,
sum(ex.interest_expected)*0.13 as IvaInteresDevengadoSinPagar,
((sum(interest_tax_amount)/100)+ sum(ex.interest_expected)*0.13) as totalIVAInteresDevengado,
sum(overdue_amount)/100 as moraPagada,
sum(overdue_tax_amount)/100 as IVAMoraPagada,
sum(commission_amount)/100 as comisionPagada,
sum(commission_tax_amount)/100 as IVAComisionPagada,
ex.created_at
from diimo_core.loan_repayments as lr
left join (
select loan_id, sum(interest_expected)/100 as interest_expected, min(created_at) as created_at  from diimo_core.loan_details
where expire_at between '2020-01-01' and '2023-12-31'
and (paid_at is null or paid_at >'2023-12-31')
group by loan_id
) as ex on ex.loan_id = lr.loan_id
left join diimo_core.loans on loans.id = lr.loan_id
where deleted_at is null and repayment_date between '2023-01-01' and '2023-12-31' -- and ex.created_at >='2023-01-01'
group by lr.loan_id

select 
lr.loan_id as loan_id,NumCuotas,
case when dev_lkup.MaximaCuotaPagada = NumCuotas then 5 else  loans.status end as status,
sum(principal_amount)/100 as CapitalPagado,
sum(interest_amount)/100 as InteresPagado,
sum(ex.interest_expected) devengadoSinPagar, 
((sum(interest_amount)/100) +sum(ex.interest_expected)) as TotalInteresDevengado,
sum(interest_tax_amount)/100 as IvaInteresPagado,
sum(ex.interest_expected)*0.13 as IvaInteresDevengadoSinPagar,
((sum(interest_tax_amount)/100)+ sum(ex.interest_expected)*0.13) as totalIVAInteresDevengado,
sum(overdue_amount)/100 as moraPagada,
sum(overdue_tax_amount)/100 as IVAMoraPagada,
sum(commission_amount)/100 as comisionPagada,
sum(commission_tax_amount)/100 as IVAComisionPagada,
ex.created_at
from diimo_core.loan_repayments as lr
left join (
select loan_id, sum(interest_expected)/100 as interest_expected, min(created_at) as created_at  from diimo_core.loan_details
where expire_at between '2020-01-01' and '2023-12-31'
and (paid_at is null or paid_at >'2023-12-31')
group by loan_id
) as ex on ex.loan_id = lr.loan_id
left join diimo_core.loans on loans.id = lr.loan_id
left join (
select loans.id,date(loans.accepted_at) as accepted_at,amount/100 as Monto,
case when interest_type = 'fixed' then ((commission/100)/periods.repeat)*mcd.MaximaCuotaDevengada  else 0 end  as lkp_ComisionDevengada,
case when interest_type = 'fixed' then ((interest/100)/periods.repeat)*mcd.MaximaCuotaDevengada  else 0 end  as lkp_interesDevengada,
case when interest_type = 'fixed' then ((iva_interest/100)/periods.repeat)*mcd.MaximaCuotaDevengada  else 0 end  as lkp_iva_interesDevengada,
interest_type, periods.repeat as NumCuotas,mcp.MaximaCuotaPagada,mcd.MaximaCuotaDevengada
from diimo_core.loans
left join diimo_core.periods on periods.id = loans.period_id
left join (
select loan_id, max(number_fee) as MaximaCuotaPagada from diimo_core.loan_details 
where paid_at is not null and paid_at < '2024-01-1'
group by loan_id
)as mcp on mcp.loan_id = loans.id
left join  (
select loan_id, max(number_fee) as MaximaCuotaDevengada from diimo_core.loan_details 
where expire_at < '2024-01-1' or paid_at < '2024-01-01'
group by loan_id
) as mcd on mcd.loan_id = loans.id
where accepted_at is not null
and accepted_at between '2020-01-01' and '2023-12-31'  
) as dev_lkup on dev_lkup.id = loans.id
where deleted_at is null and repayment_date between '2023-01-01' and '2023-12-31' -- and ex.created_at >='2023-01-01'
group by lr.loan_id