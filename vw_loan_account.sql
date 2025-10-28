CREATE OR REPLACE VIEW vw_loan_account AS
SELECT a.customer_id                                        customer_no
     , a.customer_id                                        cust_no
     , a.currency
     , d.field_char_4                                       loaihinhvay
     , d.field_char_2                                       mucdichvay
     --decode(a.application_num,null, a.account_number,a.application_num) contract_no,
     , a.account_number                                     contract_no
     , b.outstanding
     , c.resolved_value
     , d.user_defined_status
     , NULL                                                 fee
     , e.value_date                                         disbursementdate
     , e.value_date
     , d.maturity_date                                      maturitydate
     , d.maturity_date
     , 'CUSTOMER POLICY'                                    policy
     , 'CUSTOMER POLICY'                                    customerpolicy
     , a.amount_disbursed
     , g.amount                                             expectedinterest
     , h.amount                                             paidinterest
     , (g.amount - h.amount)                                outinterest
     , i.amount                                             liquidation
     , ROUND(MONTHS_BETWEEN(a.maturity_date, a.value_date)) terms
     , ROUND(MONTHS_BETWEEN(a.maturity_date, a.value_date)) term
     , a.product_code
     , a.product_code                                       product
     , 'Chuyen vien quan he khach hang'                     rmname
     , t.overall_limit                                      limit
     , (a.amount_financed - a.amount_disbursed)             remaininglimit
     , ' '                                                  paymentmethod
     , ' '                                                  paymentterm
     --a.amount_disbursed*fn_get_exchange(a.CURRENCY,a.branch_code) total_disbursed,
     , 0 AS                                                 total_disbursed
     --i.amount*fn_get_exchange(a.CURRENCY,a.branch_code) total_PRINCIPAL,
     , 0 AS                                                 total_principal
     --h.amount*fn_get_exchange(a.CURRENCY,a.branch_code) total_INTEREST,
     , 0 AS                                                 total_interest
     --b.outstanding*fn_get_exchange(a.CURRENCY,a.branch_code) total_outstanding,
     , 0 AS                                                 total_outstanding
     , ''                                                   collateral_type
     , ''                                                   collateral_value
     --i.amount*fn_get_exchange(a.CURRENCY,a.branch_code) total,
     , 0 AS                                                 total
     , d.product_category
     , d.account_status
     , a.customer_id
     , d.branch_code
     , t.customer_type
     , a.maker_id
     , k.duedate                                            int_pay_date
     , k.orig_amount_due                                    int_pay_amount
     , d.goi_vay_uu_dai                                     promotion_campaign
     , a.application_num
     , a.etl_log_date
FROM cltb_account_apps_master a
   , (SELECT (SUM(NVL(amount_due, 0)) - SUM(NVL(amount_settled, 0))) outstanding, account_number
      FROM cltb_account_schedules
      WHERE component_name IN ('PRINCIPAL', 'SBODP')
      GROUP BY account_number) b
   , (SELECT DISTINCT x.resolved_value, x.account_number
      FROM cltb_account_ude_values x
      WHERE x.ude_id = 'INTEREST_RATE'
        AND effective_date = (SELECT MAX(y.effective_date)
                              FROM cltb_account_ude_values y
                              WHERE y.account_number = x.account_number
                                AND y.ude_id = 'INTEREST_RATE'
                                AND effective_date <= SYSDATE)) c
   , cltb_account_master d
   , (SELECT MIN(value_date) value_date, account_number
      FROM cltb_event_entries
      WHERE event_code IN ('DSBR', 'VAMI')
      GROUP BY account_number) e
   , (SELECT SUM(NVL(accrued_amount, 0)) amount, account_number
      FROM cltb_account_schedules
      WHERE component_name IN ('MAIN_INT', 'PODP_INT')
      GROUP BY account_number) g
   , (SELECT SUM(NVL(amount_paid, 0)) amount, account_number
      FROM cltb_amount_paid
      WHERE component_name IN ('MAIN_INT', 'PODP_INT', 'PODI_INT', 'SBACRINT', 'SBODI')
      GROUP BY account_number) h
   ,
/*     (
    select distinct no_of_schedules unit,account_number
    from CLTB_ACCOUNT_COMP_SCH@COREVRB a
    where  a.no_of_schedules = (select max(no_of_schedules) from CLTB_ACCOUNT_COMP_SCH@COREVRB
                                where account_number = a.account_number
                                and schedule_type = 'P'
                                and COMPONENT_NAME = 'PRINCIPAL')
    and unit != 'B'
     ) k,
*/
    (SELECT SUM(NVL(amount_paid, 0)) amount, account_number
     FROM cltb_amount_paid
     WHERE component_name IN ('PRINCIPAL', 'SBODP')
     GROUP BY account_number) i
   , (SELECT accno, duedate, formula_name, orig_amount_due
      FROM clvw_schedule
      WHERE TO_CHAR(duedate, 'MM') = TO_CHAR(SYSDATE, 'MM')
        AND TO_CHAR(duedate, 'YYYY') = TO_CHAR(SYSDATE, 'YYYY')
        AND orig_amount_due <> 0
        AND rownum = 1) k
   , sttm_customer t
WHERE a.account_number = b.account_number
  AND a.account_number = c.account_number
  AND a.account_number = d.account_number
  AND a.account_number = e.account_number
  AND a.account_number = g.account_number(+)
  AND a.account_number = h.account_number(+)
  AND a.account_number = i.account_number(+)
  AND a.account_number = k.accno(+)
  AND a.account_number <> '999N12A151420002'
  AND a.customer_id = t.customer_no
/

