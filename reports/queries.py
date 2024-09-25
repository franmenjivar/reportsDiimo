import polars as pl
from datetime import  timedelta
from decouple import config

class ReportsQueries():
    user=config('USER')
    password=config('PASSWORD')
    host=config('HOST')
    port=config('PORT')
    db=config('DATA_BASE')
    conn = f"mysql://{user}:{password}@{host}:{port}/{db}"

    def reporteDesembolsos(self,date1,date2, *args, **kwargs):
        query = f'''
        select y.id, z.capital,z.FechaAceptacion,z.FechaVencimiento, z.TipoPeriocidad,z.MontoCuota,z.numCuotas  from (
        select loans.id from diimo_core.loans
        where accepted_at between '{date1}' and '{date2}' and status in (3,5,6,11,12)
        ) as y
        left join (
        select x.id as sub_id,x.capital,x.FechaAceptacion,x.FechaVencimiento, x.TipoPeriocidad,x.MontoCuota,x.numCuotas from (
        select  loans.id,
        loans.amount/100 as Capital, 
                Date(loans.accepted_at) as FechaAceptacion, loan_details.expire_at as FechaVencimiento,
                case 
                when   periocidad.TipoPeriocidad = 'Quincenal' then 4
                when   periocidad.TipoPeriocidad = 'Mensual' then 5
                when   periocidad.TipoPeriocidad = 'Semanal' then 2
                end as TipoPeriocidad,
                (loan_details.capital+ loan_details.interes)/100 as MontoCuota,loan_details.numCuotas from diimo_core.loans
                left join diimo_core.wallets on wallets.id = loans.wallet_id
                left join diimo_core.client_info on client_info.client_id = wallets.client_id
                left join diimo_core.clients on clients.id = client_info.client_id
                left join (
                    select loan_details.loan_id, max(loan_details.expire_at) as expire_at, max(number_fee) as NumCuotas,
                    max(case when number_fee = 1 then principal_expected end ) as capital,
                    max(case when number_fee = 1 then interest_expected end ) as interes
                    from diimo_core.loan_details
                    group by loan_details.loan_id
                ) as  loan_details on loan_details.loan_id = loans.id
                left join (
                    select periods.id,period_types.name as TipoPeriocidad from diimo_core.periods
                    left join diimo_core.period_types on period_types.id = periods.period_type_id
                ) as periocidad on periocidad.id = loans.period_id
                where diimo_core.loans.status in (3,5,6,11,12) and accepted_at between '{date1}' and '{date2}'
                order by accepted_at) as x
        ) as z on z.sub_id = y.id

        '''
        df = pl.read_sql(query,self.conn)
        return df
    
    def reportecxc(self,date, *args, **kwargs):
        date2 = date+timedelta(days=1)
        query = f'''
            select 
            Nombre, Dui, IDPrestamo, Estado, due_rate, date(accepted_at) as accepted_at, Capital, comision, numCuotas, MaximaCuotaPagada, 
            case 
            when indicadorInteresCero = 0 and t_final.accepted_at >'2023-01-01' then CapitalPorCuota 
            else CapitalPorCuota
            end as CapitalPorCuota,
            ComisionTotal, ComisionPorCuota, CuotaDevengada, CapitalYComissionPagado, 
            case 
            when indicadorInteresCero = 0 and t_final.accepted_at >'2023-01-01' and InteresPagado = 0 then 0 else
            InteresDevengado end as InteresDevengado,
            case 
            when indicadorInteresCero = 0 and t_final.accepted_at >'2023-01-01' and InteresPagado = 0 then 0
            -- Added----------------------------------------------------------------------------
            when ((iva_interest_Devengado is null and IvaInteresPagado> 0) or iva_interest_Devengado <IvaInteresPagado) then IvaInteresPagado
            when iva_interest_Devengado is null and iva_interest_amount is not null then iva_interest_amount
            else iva_interest_Devengado
            -- added -------------------------------------------------------------------------
            end iva_interest_Devengado, InteresPagado, IvaInteresPagado, MoraDevengada, MoraPagada, IVAMoraPagada, ComisionPagada, 
            IvaComision, DiasEnMora from (
            SELECT Nombre, Dui,IDPrestamo as IDPrestamo, Estado,x.due_rate,x.accepted_at, Capital,comision,numCuotas,MaximaCuotaPagada,CapitalPorCuota,ComisionTotal,ComisionPorCuota,Comision_Pagada,CuotaDevengada,
            case 
            when y.CapitalYComissionPagado is not null then y.CapitalYComissionPagado 
            else cap_com_alt.CapitalYComissionPagado end as CapitalYComissionPagado,       
            indicadorInteresCero,
            CASE WHEN InteresDevengado<y.InteresPagado then y.InteresPagado 
            when InteresDevengado is null and y.InteresPagado is not null then y.InteresPagado  
            when InteresDevengado is  null then 0
            else InteresDevengado end as InteresDevengado,iva_interest_amount,
            CASE WHEN x.accepted_at >'2023-01-13' then (
            CASE WHEN InteresDevengado<y.InteresPagado then y.InteresPagado else InteresDevengado end)*0.13 
            when iva_interest_amount<y.IvaInteresPagado  THEN y.IvaInteresPagado ELSE  iva_interest_amount
            END AS iva_interest_Devengado,
            case when y.InteresPagado is not null then y.InteresPagado
            else cap_com_alt.InteresPagado end as InteresPagado,
            case when y.IvaInteresPagado is not null then y.IvaInteresPagado
            else cap_com_alt.IvaInteresPagado end as IvaInteresPagado,
            MoraDevengada,
            case when y.MoraPagada is not null then y.MoraPagada
            else cap_com_alt.MoraPagada end as MoraPagada,
            case when y.IVAMoraPagada is not null then y.IVAMoraPagada
            else cap_com_alt.IVAMoraPagada end as IVAMoraPagada,               
            y.ComisionPagada,y.IvaComision,
            CASE WHEN Estado = 5 THEN 0 ELSE z.DiasEnMora END AS DiasEnMora
            from (
            SELECT concat(client_info.first_name,' ',client_info.last_name) as Nombre,client_info.dui as Dui,indicadorInteresCero,
            loans.id as IDPrestamo,loans.status as Estado,loans.due_rate,loans.accepted_at, ROUND(loans.amount/100,0) as Capital, ROUND(loans.commission/100,2) as comision, 
            case when
            loans.repeat is null then periods.repeat else
            loans.repeat end as numCuotas,
            max(loan_details.number_fee)  as MaximaCuotaPagada,
            case when
            loans.repeat is null then sum(ROUND((loans.amount/periods.repeat)/100,2)) else
            sum(ROUND((loans.amount/loans.repeat)/100,2)) end as CapitalPorCuota,
            case when
            loans.repeat is null then sum(ROUND((loans.commission/periods.repeat)/100,3))  else
            sum(ROUND((loans.commission/loans.repeat)/100,3)) end  as ComisionTotal,
            case when
            loans.repeat is null then ROUND((loans.commission/periods.repeat)/100,2) else
            ROUND((loans.commission/loans.repeat)/100,2) end as ComisionPorCuota, 
            case when
            loans.repeat is null then  ROUND((loans.commission/periods.repeat)/100,2)*max(loan_details.number_fee)  else
            ROUND((loans.commission/loans.repeat)/100,2)*max(loan_details.number_fee) end as Comision_Pagada,
            case when loans.accepted_at >'2023-01-13' and NEWINTERES.InteresALaFecha is not null
            then NEWINTERES.InteresALaFecha
            else (
            case when loans.repeat is not null then 
            ROUND((loans.interest/loans.repeat)/100,2)*max(loan_details.number_fee)
            else ROUND((loans.interest/periods.repeat)/100,2)*max(loan_details.number_fee) end)
            end as InteresDevengado,
            #ROUND((loans.interest/loans.repeat)/100,2)*max(loan_details.number_fee) as InteresDevengado,
            case when
            loans.repeat is null then ROUND((loans.iva_interest/periods.repeat)/100,2)*max(loan_details.number_fee)  else
            ROUND((loans.iva_interest/loans.repeat)/100,2)*max(loan_details.number_fee) end as iva_interest_amount,
            detalleMora.MoraDevengada as MoraDevengada,
            detalleMora.CuotaDevengada as CuotaDevengada
            FROM diimo_core.loans
            left join diimo_core.periods on periods.id = loans.period_id
            left join diimo_core.wallets on wallets.id = loans.wallet_id
            left join diimo_core.client_info on client_info.client_id = wallets.client_id
            left join 
            (
            select * from diimo_core.loan_details
            where (loan_details.expire_at <= '{date}' or  (loan_details.expire_at >= '{date2}' and loan_details.paid_at is not null and loan_details.paid_at< '{date2}'))
            ) as loan_details on loan_details.loan_id = loans.id
            left join (
            SELECT 
                loan_details.loan_id,
                ROUND((SUM(loan_details.due) + sum(loan_details.debt_paid))/ 100, 2) AS MoraDevengada,
                max(loan_details.number_fee) as CuotaDevengada
            FROM
                diimo_core.loan_details
                where expire_at < '{date2}'
                group by loan_details.loan_id
            ) as detalleMora on detalleMora.loan_id = loans.id
            LEFT JOIN (
            select loans.id,
            (loans.interest+loans.iva_interest)/100 as Interes,
            sum(interest_amount+iva_interest_amount)/100 as InteresALaFecha,
            max(interest_expected) as indicadorInteresCero
            from  diimo_core.loans
            left join diimo_core.loan_details on loan_details.loan_id =loans.id
            where loans.status  in (3,5,6,11,12)  and loans.accepted_at >'2023-01-01' and  loans.accepted_at is not null
            -- Added --------------------------------------------------------------------------------------------
            and (loans.interest_type = 'daily' or (loans.interest_type = 'fixed' and loans.accepted_at < '2023-01-13'))
            group by loans.id
            ) AS NEWINTERES ON NEWINTERES.id = loans.id
            where 
            loans.accepted_at >= '2020-05-01'  and loans.accepted_at <  '{date2}' and loans.accepted_at is not null and loans.status in (3,5,6,11,12)  
            group by concat(client_info.first_name,client_info.last_name) ,client_info.dui,loans.id) as x
            left join (
            SELECT loan_repayments.loan_id,ROUND(sum(loan_repayments.principal_amount)/100,2) as CapitalYComissionPagado,ROUND(sum(loan_repayments.interest_amount)/100,2) as InteresPagado,
            ROUND(sum(loan_repayments.interest_tax_amount)/100,2) as IvaInteresPagado,ROUND(sum(loan_repayments.overdue_amount)/100,2) as MoraPagada,
            ROUND(sum(loan_repayments.overdue_tax_amount)/100,2) as IVAMoraPagada,
            ROUND(sum(loan_repayments.commission_amount)/100,2) as ComisionPagada, 
            ROUND(sum(loan_repayments.commission_tax_amount)/100,2) as IvaComision
            FROM diimo_core.loan_repayments 
            where loan_repayments.repayment_date < '{date2}' and loan_repayments.deleted_at is null
            group by loan_repayments.loan_id
            ) as y on y.loan_id = x.IDPrestamo
            left join (
            select loan_id, ifnull(datediff(curdate(),min(expire_at)),0) as DiasEnMora from diimo_core.loan_details where  paid_at is null and loan_details.expire_at < '{date2}'
            group by loan_id
            ) as z on z.loan_id = x.IDPrestamo
                    left join (
            select loan_id,
            (sum(principal_amount)+sum(commission_amount)+sum(iva_commission_amount))/100 as CapitalYComissionPagado,
            sum(interest_amount)/100 as InteresPagado,
            sum(iva_interest_amount)/100 as IvaInteresPagado,
            (sum(debt_paid)-(sum(debt_paid)*.13))/100 as MoraPagada,
            (sum(debt_paid)*.13)/100 as IVAMoraPagada
            from diimo_core.loan_details 
                where   paid_at is not null
                group by loan_id
            ) as cap_com_alt on cap_com_alt.loan_id  = x.IDPrestamo
            order by x.accepted_at) as t_final;
        '''
        df = pl.read_sql(query,self.conn)
        return df
    
    def reporte_colectado(self,date, *args, **kwargs):
        date2 = date+timedelta(days=1)
        query ='''
        select sum(Capital) as Capital, sum(Interes) as Interes,sum(IvaInteres) as IvaInteres,
        sum(Mora) as Mora, sum(IvaMora) as IvaMora, sum(Comision) as Comision, sum(IvaComision) as IvaComision,sum(refund_amount)as Reintegrado,
        (sum((Capital+Interes+ Mora+Comision+IvaInteres+IvaMora+IvaComision))- sum(refund_amount)) as TotalColectado
        from (
        select repayment_date,sum(principal_amount)/100 as Capital, sum(interest_amount)/100 as Interes,
        sum(interest_tax_amount)/100 as IvaInteres, sum(overdue_amount)/100 as Mora, sum(overdue_tax_amount)/100 as IvaMora, 
        sum(commission_amount)/100 as Comision, sum(commission_tax_amount)/100 as IvaComision,
        sum(refund_amount)/100 as refund_amount
        from diimo_core.loan_repayments
        where repayment_date >= '2024-01-01' and repayment_date <'2024-02-01' and deleted_at is null
        group by repayment_date) as x -- group by month(repayment_date)
        '''
        
        df = pl.read_sql(query, self.conn)
        return df 