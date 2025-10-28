create or replace NONEDITIONABLE PACKAGE BODY pkg_etl_crm_job_daily AS
    PROCEDURE excute_etl_crm_job_daily(t VARCHAR2)
    AS
    BEGIN
        --TODO: Start daily ETL
        INSERT
        INTO etl_log (log_date, status)
        VALUES (SYSDATE, 'START_DAILY');
        COMMIT;

        --DELETE temp data
        pkg_etl_crm_clean_job.clean_crm_tables;

        --PROC_CRM_PARTNER
        pkg_etl_crm_job_daily.proc_crm_partner_daily;
        --CRM_BOUNDARY_ADDRESS
        pkg_etl_crm_job_daily.proc_crm_boundary_address_daily;
        --LOAN
        pkg_etl_crm_job_daily.proc_etl_bank_loan_daily;
        -- CASA
        pkg_etl_crm_job_daily.proc_etl_bank_casa_daily;
        --FD
        pkg_etl_crm_job_daily.proc_etl_bank_fd_daily;
        --OVERDRAFT
        pkg_etl_crm_job_daily.proc_etl_bank_ovd_daily;
        --LCBG
        pkg_etl_crm_job_daily.proc_etl_bank_lcbg_daily;
        --CREDIT_CARD
        pkg_etl_crm_job_daily.proc_etl_bank_credit_card_daily;
        --IB MB
        pkg_etl_crm_job_daily.proc_etl_bank_ibmb_daily;
        --TRANSACTION
        pkg_etl_crm_job_daily.proc_etl_bank_transaction_daily;
        --COLLATERAL
        pkg_etl_crm_job_daily.proc_etl_bank_collateral_daily;
        --CRM_PARTNER_HOLDING
        pkg_etl_crm_job_daily.proc_crm_partner_holding_daily;
        --CRM_PARTNER_CONTRACT
        pkg_etl_crm_job_daily.proc_crm_partner_contract_daily;
        --CRM_PARTNER_CONTRACT_ASSIGN
        pkg_etl_crm_job_daily.proc_crm_partner_contract_assign_daily;

    END excute_etl_crm_job_daily;

    PROCEDURE proc_etl_bank_casa_daily
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        BEGIN

            MERGE
            INTO crm_partner_bank_account_casa casa
            USING
                (SELECT b.partner_id                                      AS partner_id
                      , a.cust_ac_no                                      AS account_no
                      , DECODE(a.ac_class_type, 'U', 'CURRENT', 'SAVING') AS account_type
                      , a.balance                                         AS balance
                      , a.ccy                                             AS currency
                      , a.ac_open_date                                    AS open_date
                      , a.closed_date                                     AS close_date
                      , a.description                                     AS description
                      , NULL                                              AS rate
                      , a.term                                            AS term
                      , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))           AS created_date
                      , a.branch_code                                     AS branch_code
                      , a.record_stat                                     AS account_status
                      , a.acy_blocked_amount                              AS amount_block
                      , a.acy_avl_bal                                     AS available_balance
                      , a.lcy_curr_balance                                AS lcy_curr_balance
                 FROM crmstaging.vw_sttm_cust_account@"Dbstaging580.Localdomain" a
                    , crm_partner b
                    , crm_partner_bank_account_casa c
                 WHERE a.cust_no = b.cif_no (+)
                   AND a.account_class != 'OI01'
                   AND a.cust_ac_no = c.account_no(+)
                   AND a.ac_class_type = 'U'
                   AND TRUNC(a.etl_log_date) = TRUNC(SYSDATE)) daily_casa
            ON (casa.account_no = daily_casa.account_no)
            WHEN MATCHED THEN
                UPDATE
                SET partner_id        = daily_casa.partner_id
                  , account_type      = daily_casa.account_type
                  , balance           = daily_casa.balance
                  , currency          = daily_casa.currency
                  , open_date         = daily_casa.open_date
                  , close_date        = daily_casa.close_date
                  , description       = daily_casa.description
                  , rate              = daily_casa.rate
                  , term              = daily_casa.term
                  , created_date      = daily_casa.created_date
                  , branch_code       = daily_casa.branch_code
                  , account_status    = daily_casa.account_status
                  , type              = 'CASA'
                  , amount_block      = daily_casa.amount_block
                  , available_balance = daily_casa.available_balance
                  , lcy_curr_balance  = daily_casa.lcy_curr_balance
            WHEN NOT MATCHED THEN
                INSERT ( account_casa_id, partner_id, account_no, account_type, balance, currency, open_date
                       , close_date, description, rate, term, created_date, branch_code
                       , account_status, type, amount_block, available_balance, lcy_curr_balance)
                VALUES ( crm_partner_bank_account_casa_seq.nextval
                       , daily_casa.partner_id
                       , daily_casa.account_no
                       , daily_casa.account_type
                       , daily_casa.balance
                       , daily_casa.currency
                       , daily_casa.open_date
                       , daily_casa.close_date
                       , daily_casa.description
                       , daily_casa.rate
                       , daily_casa.term
                       , daily_casa.created_date
                       , daily_casa.branch_code
                       , daily_casa.account_status
                       , 'CASA'
                       , daily_casa.amount_block
                       , daily_casa.available_balance
                       , daily_casa.lcy_curr_balance);
            row_count := SQL%ROWCOUNT;

            MERGE
            INTO crm_partner_bank_account_casa t1
            USING (SELECT cust_ac_no, rate, lcy_curr_balance
                   FROM crmstaging.bo_rptw_deposit_new@"Dbstaging580.Localdomain"
                   WHERE TRUNC(backup_date) = TRUNC(SYSDATE - 1)) t2
            ON (t1.account_no = t2.cust_ac_no)
            WHEN MATCHED THEN
                UPDATE
                SET t1.rate             = t2.rate
                  , t1.lcy_curr_balance = t2.lcy_curr_balance;

            INSERT
            INTO etl_log (log_date, table_name, row_affected)
            VALUES (SYSDATE, 'CRM_PARTNER_BANK_ACCOUNT_CASA', row_count);
            COMMIT;
            
        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                error_code := SQLCODE;
                error_message := SQLERRM;
                INSERT
                INTO error_log (error_proc, error_code, error_message, error_line, error_date)
                VALUES ( 'proc_etl_bank_casa_daily', error_code, error_message
                       , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
                COMMIT;
        END;
    END proc_etl_bank_casa_daily;

    PROCEDURE proc_etl_bank_loan_daily
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        BEGIN

            MERGE
            INTO crm_partner_bank_account_loan loan
            USING
                (SELECT a.partner_id                                          AS partner_id
                      , ln.contract_no                                        AS contract_no
                      , ln.currency                                           AS currency
                      , ln.amount_disbursed                                   AS disbursement_amount
                      , ln.maturity_date                                      AS maturity_date
                      , ln.value_date                                         AS open_date
                      , ln.outstanding                                        AS outstanding
                      , ln.liquidation                                        AS paid_amount
                      , ln.int_pay_amount                                     AS pay_amount
                      , ln.int_pay_date                                       AS pay_date
                      , ln.resolved_value                                     AS rate
                      , ln.term                                               AS term
                      , 'LOAN'                                                AS type
                      , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))               AS created_date
                      , ln.user_defined_status                                AS debt_group
                      , ln.promotion_campaign                                 AS promotion_campaign
                      , ln.branch_code                                        AS branch_code
                      , b.field_val_40                                        AS limit
                      , ln.application_num                                    AS description
                      , ln.loaihinhvay                                        AS loai_hinh_vay
                      , ln.mucdichvay                                         AS muc_dich_vay
                      , ln.account_status                                     AS account_status
                      , CASE
                            WHEN ln.currency <> 'VND' THEN ln.outstanding * (SELECT mid_rate
                                                                             FROM (SELECT DISTINCT mid_rate, ccy1
                                                                                   FROM crmstaging.cytm_rates@"Dbstaging580.Localdomain"
                                                                                   WHERE rate_type = 'STANDARD')
                                                                             WHERE ccy1 = ln.currency)
                                                      ELSE ln.outstanding END AS lcy_curr_balance
                 FROM crm_partner a
                    , vw_loan_account ln
                    , crmstaging.cstm_function_userdef_fields@"Dbstaging580.Localdomain" b
                 WHERE a.cif_no = ln.customer_no
                   AND (a.cif_no || '~' = b.rec_key
                     OR ln.branch_code || '~' || ln.contract_no || '~' = b.rec_key)
                   AND ln.account_status IN ('A', 'L')
                   AND TRUNC(ln.etl_log_date) = TRUNC(SYSDATE)) daily_loan
            ON (loan.contract_no = daily_loan.contract_no)
            WHEN MATCHED THEN
                UPDATE
                SET partner_id          = daily_loan.partner_id
                  , currency            = daily_loan.currency
                  , disbursement_amount = daily_loan.disbursement_amount
                  , maturity_date       = daily_loan.maturity_date
                  , open_date           = daily_loan.open_date
                  , outstanding         = daily_loan.outstanding
                  , paid_amount         = daily_loan.paid_amount
                  , pay_amount          = daily_loan.pay_amount
                  , pay_date            = daily_loan.pay_date
                  , rate                = daily_loan.rate
                  , term                = daily_loan.term
                  , type                = daily_loan.type
                  , created_date        = daily_loan.created_date
                  , debt_group          = daily_loan.debt_group
                  , promotion_campaign  = daily_loan.promotion_campaign
                  , branch_code         = daily_loan.branch_code
                  , limit               = daily_loan.limit
                  , description         = daily_loan.description
                  , muc_dich_vay        = daily_loan.muc_dich_vay
                  , loai_hinh_vay       = daily_loan.loai_hinh_vay
                  , account_status      = daily_loan.account_status
                  , lcy_curr_balance    = daily_loan.lcy_curr_balance
            WHEN NOT MATCHED THEN
                INSERT ( account_loan_id, partner_id, contract_no, currency, disbursement_amount, maturity_date
                       , open_date
                       , outstanding
                       , paid_amount, pay_amount, pay_date, rate, term, type, created_date
                       , debt_group
                       , promotion_campaign
                       , branch_code
                       , limit
                       , description
                       , muc_dich_vay
                       , loai_hinh_vay
                       , account_status
                       , lcy_curr_balance)
                VALUES ( crm_partner_bank_account_loan_seq.nextval
                       , daily_loan.partner_id
                       , daily_loan.contract_no
                       , daily_loan.currency
                       , daily_loan.disbursement_amount
                       , daily_loan.maturity_date
                       , daily_loan.open_date
                       , daily_loan.outstanding
                       , daily_loan.paid_amount
                       , daily_loan.pay_amount
                       , daily_loan.pay_date
                       , daily_loan.rate
                       , daily_loan.term
                       , 'LOAN'
                       , daily_loan.created_date
                       , daily_loan.debt_group
                       , daily_loan.promotion_campaign
                       , daily_loan.branch_code
                       , daily_loan.limit
                       , daily_loan.description
                       , daily_loan.muc_dich_vay
                       , daily_loan.loai_hinh_vay
                       , daily_loan.account_status
                       , daily_loan.lcy_curr_balance);
            row_count := SQL%ROWCOUNT;

            INSERT
            INTO etl_log (log_date, table_name, row_affected)
            VALUES (SYSDATE, 'CRM_PARTNER_BANK_ACCOUNT_LOAN', row_count);
            COMMIT;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                error_code := SQLCODE;
                error_message := SQLERRM;
                INSERT
                INTO error_log (error_proc, error_code, error_message, error_line, error_date)
                VALUES ( 'proc_etl_bank_loan_daily', error_code, error_message
                       , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
                COMMIT;
        END;
    END proc_etl_bank_loan_daily;

    PROCEDURE proc_etl_bank_fd_daily
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        BEGIN

            MERGE
            INTO crm_partner_bank_account_fd fd
            USING
                (SELECT b.partner_id                                      AS partner_id
                      , a.cust_ac_no                                      AS account_no
                      , DECODE(a.ac_class_type, 'U', 'CURRENT', 'SAVING') AS account_type
                      , a.balance                                         AS balance
                      , a.ccy                                             AS currency
                      , a.ac_open_date                                    AS open_date
                      , a.closed_date                                     AS close_date
                      , a.description                                     AS description
                      , NULL                                              AS rate
                      , a.term                                            AS term
                      , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))           AS created_date
                      , a.branch_code                                     AS branch_code
                      , a.record_stat                                     AS account_status
                      , a.lcy_curr_balance                                AS lcy_curr_balance
                 FROM crmstaging.vw_sttm_cust_account@"Dbstaging580.Localdomain" a
                    , crm_partner b
                    , crm_partner_bank_account_fd c
                 WHERE a.cust_no = b.cif_no (+)
                   AND a.account_class != 'OI01'
                   AND a.cust_ac_no = c.account_no(+)
                   AND a.ac_class_type != 'U'
                   AND TRUNC(a.etl_log_date) = TRUNC(SYSDATE)) daily_fd
            ON (fd.account_no = daily_fd.account_no)
            WHEN MATCHED THEN
                UPDATE
                SET partner_id       = daily_fd.partner_id
                  , account_type     = daily_fd.account_type
                  , balance          = daily_fd.balance
                  , currency         = daily_fd.currency
                  , open_date        = daily_fd.open_date
                  , close_date       = daily_fd.close_date
                  , description      = daily_fd.description
                  , rate             = daily_fd.rate
                  , term             = daily_fd.term
                  , created_date     = daily_fd.created_date
                  , branch_code      = daily_fd.branch_code
                  , account_status   = daily_fd.account_status
                  , type             = 'FD'
                  , lcy_curr_balance = daily_fd.lcy_curr_balance
            WHEN NOT MATCHED THEN
                INSERT ( account_fd_id, partner_id, account_no, account_type, balance, currency, open_date
                       , close_date, description, rate, term, created_date
                       , branch_code
                       , account_status
                       , type, lcy_curr_balance)
                VALUES ( crm_partner_bank_account_fd_seq.nextval
                       , daily_fd.partner_id
                       , daily_fd.account_no
                       , daily_fd.account_type
                       , daily_fd.balance
                       , daily_fd.currency
                       , daily_fd.open_date
                       , daily_fd.close_date
                       , daily_fd.description
                       , daily_fd.rate
                       , daily_fd.term
                       , daily_fd.created_date
                       , daily_fd.branch_code
                       , daily_fd.account_status
                       , 'FD'
                       , daily_fd.lcy_curr_balance);
            row_count := SQL%ROWCOUNT;

            MERGE
            INTO crm_partner_bank_account_fd t1
            USING (SELECT cust_ac_no, rate, lcy_curr_balance
                   FROM crmstaging.bo_rptw_deposit_new@"Dbstaging580.Localdomain"
                   WHERE TRUNC(backup_date) = TRUNC(SYSDATE - 1)) t2
            ON (t1.account_no = t2.cust_ac_no)
            WHEN MATCHED THEN
                UPDATE
                SET t1.rate             = t2.rate
                  , t1.lcy_curr_balance = t2.lcy_curr_balance;

            INSERT
            INTO etl_log (log_date, table_name, row_affected)
            VALUES (SYSDATE, 'CRM_PARTNER_BANK_ACCOUNT_FD', row_count);
            COMMIT;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                error_code := SQLCODE;
                error_message := SQLERRM;
                INSERT
                INTO error_log (error_proc, error_code, error_message, error_line, error_date)
                VALUES ( 'proc_etl_bank_fd_daily', error_code, error_message
                       , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
                COMMIT;

        END;
    END proc_etl_bank_fd_daily;

    PROCEDURE proc_etl_bank_ovd_daily
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        BEGIN

            MERGE
            INTO crm_partner_bank_account_overdraft ovd
            USING
                (SELECT c.partner_id                               AS                          partner_id
                      , a.cust_ac_no                               AS                          account_no
                      , 'OVERDRAFT'                                AS                          account_type
                      , a.balance                                  AS                          balance
                      , a.ccy                                      AS                          currency
                      , a.ac_open_date                             AS                          open_date
                      , CASE
                            WHEN a.account_class = 'OI01' THEN
                                (SELECT line_expiry_date
                                 FROM crmstaging.lmtm_limits@"Dbstaging580.Localdomain" lmtm_limits
                                 WHERE lmtm_limits.liab_id = a.cust_no
                                   AND SUBSTR(a.line_id, 1, 9) = lmtm_limits.line_cd
                                   AND rownum = 1)
                                                          ELSE
                                b.maturity_date
                        END                                                                    close_date
                      , a.description                              AS                          description
                      , NULL                                                                   rate
                      , TO_NUMBER(ROUND(MONTHS_BETWEEN(b.maturity_date, b.int_start_date), 0)) term
                      , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))    AS                          created_date
                      , a.record_stat                              AS                          account_status
                      , a.branch_code                              AS                          branch_code
                      , d.limit_amount + d.collateral_contribution AS                          overdraft_limit
                 FROM crmstaging.vw_sttm_cust_account@"Dbstaging580.Localdomain" a
                    , crmstaging.ictm_acc@"Dbstaging580.Localdomain" b
                    , crm_partner c
                    , crmstaging.lmtm_limits@"Dbstaging580.Localdomain" d
                    , (SELECT MAX(checker_dt_stamp) checker_dt_stamp
                            , liab_id
                       FROM crmstaging.lmtm_limits@"Dbstaging580.Localdomain"
                       GROUP BY liab_id) e
                 WHERE a.cust_ac_no = b.acc
                   AND a.cust_no = d.liab_id
                   AND a.cust_no = c.cif_no(+)
                   AND a.account_class = 'OI01'
                   AND d.checker_dt_stamp = e.checker_dt_stamp
                   AND d.liab_id = e.liab_id
                   AND TRUNC(a.etl_log_date) = TRUNC(SYSDATE)) daily_ovd
            ON (ovd.account_no = daily_ovd.account_no)
            WHEN MATCHED THEN
                UPDATE
                SET partner_id     = daily_ovd.partner_id
                  , account_type   = daily_ovd.account_type
                  , balance        = daily_ovd.balance
                  , currency       = daily_ovd.currency
                  , open_date      = daily_ovd.open_date
                  , close_date     = daily_ovd.close_date
                  , description    = daily_ovd.description
                  , rate           = daily_ovd.rate
                  , term           = daily_ovd.term
                  , created_date   = daily_ovd.created_date
                  , branch_code    = daily_ovd.branch_code
                  , account_status = daily_ovd.account_status
                  , type           = 'OVERDRAFT'
            WHEN NOT MATCHED THEN
                INSERT ( account_overdraft_id, partner_id, account_no, account_type, balance, currency, open_date
                       , close_date, description, rate, term, created_date
                       , branch_code
                       , account_status
                       , type)
                VALUES ( crm_partner_bank_account_overdraft_seq.nextval
                       , daily_ovd.partner_id
                       , daily_ovd.account_no
                       , daily_ovd.account_type
                       , daily_ovd.balance
                       , daily_ovd.currency
                       , daily_ovd.open_date
                       , daily_ovd.close_date
                       , daily_ovd.description
                       , daily_ovd.rate
                       , daily_ovd.term
                       , daily_ovd.created_date
                       , daily_ovd.branch_code
                       , daily_ovd.account_status
                       , 'OVERDRAFT');
            row_count := SQL%ROWCOUNT;

            MERGE
            INTO crm_partner_bank_account_overdraft t1
            USING (SELECT cust_ac_no, rate
                   FROM crmstaging.bo_rpvw_thau_chi_new@"Dbstaging580.Localdomain"
                   WHERE TRUNC(trn_dt) = TRUNC(SYSDATE - 1)) t2
            ON (t1.account_no = t2.cust_ac_no)
            WHEN MATCHED THEN
                UPDATE
                SET t1.rate = t2.rate
                WHERE TRUNC(t1.created_date) = TRUNC(SYSDATE);

            UPDATE crm_partner_bank_account_overdraft
            SET type = 'SERCURED_OVERDRAFT'
            WHERE account_no IN
                  (SELECT a.cust_ac_no
                   FROM crmstaging.sttm_cust_account@"Dbstaging580.Localdomain" a
                      , crmstaging.lmtb_pool_link@"Dbstaging580.Localdomain" bb
                   WHERE a.account_class = 'OI01'
                     AND a.ac_open_date <= TO_DATE(SYSDATE)
                     AND SUBSTR(a.line_id, 1, 9) = SUBSTR(bb.util_code, 10, 9));

            UPDATE crm_partner_bank_account_overdraft
            SET type = 'UNSERCURED_OVERDRAFT'
            WHERE account_no NOT IN
                  (SELECT a.cust_ac_no
                   FROM crmstaging.sttm_cust_account@"Dbstaging580.Localdomain" a
                      , crmstaging.lmtb_pool_link@"Dbstaging580.Localdomain" bb
                   WHERE a.account_class = 'OI01'
                     AND a.ac_open_date <= TO_DATE(SYSDATE)
                     AND SUBSTR(a.line_id, 1, 9) = SUBSTR(bb.util_code, 10, 9));

            MERGE
            INTO crm_partner_bank_account_overdraft ovd
            USING
                (SELECT p.partner_id                               AS partner_id
                      , c.account_number                           AS account_no
                      , 'OVERDRAFT'                                AS account_type
                      , c.amount_financed                          AS balance
                      , c.currency                                 AS currency
                      , c.value_date                               AS open_date
                      , c.maturity_date                            AS close_date
                      , c.application_num                          AS description
                      , NULL                                       AS rate
                      , NULL                                       AS term
                      , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))    AS created_date
                      , c.account_status                           AS account_status
                      , 'UNSERCURED_OVERDRAFT'                     AS type
                      , c.branch_code                              AS branch_code
                      , d.limit_amount + d.collateral_contribution AS overdraft_limit
                 FROM crmstaging.cltb_account_master@"Dbstaging580.Localdomain" c
                    , crm_partner p
                    , crmstaging.lmtm_limits@"Dbstaging580.Localdomain" d
                    , (SELECT MAX(checker_dt_stamp) checker_dt_stamp
                            , liab_id
                       FROM crmstaging.lmtm_limits@"Dbstaging580.Localdomain"
                       GROUP BY liab_id) e
                 WHERE c.customer_id = p.cif_no
                   AND c.customer_id = d.liab_id
                   AND c.field_char_15 IN ('XV.78.THAU_CHI_TIN_CHAP')
                   AND d.checker_dt_stamp = e.checker_dt_stamp
                   AND d.liab_id = e.liab_id
                   AND TRUNC(c.etl_log_date) = TRUNC(SYSDATE)) daily_ovd
            ON (ovd.account_no = daily_ovd.account_no)
            WHEN MATCHED THEN
                UPDATE
                SET partner_id     = daily_ovd.partner_id
                  , account_type   = daily_ovd.account_type
                  , balance        = daily_ovd.balance
                  , currency       = daily_ovd.currency
                  , open_date      = daily_ovd.open_date
                  , close_date     = daily_ovd.close_date
                  , description    = daily_ovd.description
                  , rate           = daily_ovd.rate
                  , term           = daily_ovd.term
                  , created_date   = daily_ovd.created_date
                  , branch_code    = daily_ovd.branch_code
                  , account_status = daily_ovd.account_status
                  , type           = daily_ovd.type
            WHEN NOT MATCHED THEN
                INSERT ( account_overdraft_id, partner_id, account_no, account_type, balance, currency, open_date
                       , close_date, description, rate, term, created_date
                       , branch_code
                       , account_status
                       , type)
                VALUES ( crm_partner_bank_account_overdraft_seq.nextval
                       , daily_ovd.partner_id
                       , daily_ovd.account_no
                       , daily_ovd.account_type
                       , daily_ovd.balance
                       , daily_ovd.currency
                       , daily_ovd.open_date
                       , daily_ovd.close_date
                       , daily_ovd.description
                       , daily_ovd.rate
                       , daily_ovd.term
                       , daily_ovd.created_date
                       , daily_ovd.branch_code
                       , daily_ovd.account_status
                       , daily_ovd.type);
            row_count := row_count + SQL%ROWCOUNT;

            MERGE
            INTO crm_partner_bank_account_overdraft ovd
            USING
                (SELECT p.partner_id                               AS partner_id
                      , c.account_number                           AS account_no
                      , 'OVERDRAFT'                                AS account_type
                      , c.amount_financed                          AS balance
                      , c.currency                                 AS currency
                      , c.value_date                               AS open_date
                      , c.maturity_date                            AS close_date
                      , c.application_num                          AS description
                      , NULL                                       AS rate
                      , NULL                                       AS term
                      , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))    AS created_date
                      , c.account_status                           AS account_status
                      , 'SERCURED_OVERDRAFT'                       AS type
                      , c.branch_code                              AS branch_code
                      , d.limit_amount + d.collateral_contribution AS overdraft_limit
                 FROM crmstaging.cltb_account_master@"Dbstaging580.Localdomain" c
                    , crm_partner p
                    , crmstaging.lmtm_limits@"Dbstaging580.Localdomain" d
                    , (SELECT MAX(checker_dt_stamp) checker_dt_stamp
                            , liab_id
                       FROM crmstaging.lmtm_limits@"Dbstaging580.Localdomain"
                       GROUP BY liab_id) e
                 WHERE c.customer_id = p.cif_no
                   AND c.customer_id = d.liab_id
                   AND c.field_char_15 IN ('XV.79.THAUCHI_KD_CO_TSDB', 'XV.80.THAUCHI_TD_CO_TSDB')
                   AND d.checker_dt_stamp = e.checker_dt_stamp
                   AND d.liab_id = e.liab_id
                   AND TRUNC(c.checker_dt_stamp) = TRUNC(SYSDATE - 1)) daily_ovd
            ON (ovd.account_no = daily_ovd.account_no)
            WHEN MATCHED THEN
                UPDATE
                SET partner_id     = daily_ovd.partner_id
                  , account_type   = daily_ovd.account_type
                  , balance        = daily_ovd.balance
                  , currency       = daily_ovd.currency
                  , open_date      = daily_ovd.open_date
                  , close_date     = daily_ovd.close_date
                  , description    = daily_ovd.description
                  , rate           = daily_ovd.rate
                  , term           = daily_ovd.term
                  , created_date   = daily_ovd.created_date
                  , branch_code    = daily_ovd.branch_code
                  , account_status = daily_ovd.account_status
                  , type           = daily_ovd.type
            WHEN NOT MATCHED THEN
                INSERT ( account_overdraft_id, partner_id, account_no, account_type, balance, currency, open_date
                       , close_date, description, rate, term, created_date
                       , branch_code
                       , account_status
                       , type)
                VALUES ( crm_partner_bank_account_overdraft_seq.nextval
                       , daily_ovd.partner_id
                       , daily_ovd.account_no
                       , daily_ovd.account_type
                       , daily_ovd.balance
                       , daily_ovd.currency
                       , daily_ovd.open_date
                       , daily_ovd.close_date
                       , daily_ovd.description
                       , daily_ovd.rate
                       , daily_ovd.term
                       , daily_ovd.created_date
                       , daily_ovd.branch_code
                       , daily_ovd.account_status
                       , daily_ovd.type);
            row_count := row_count + SQL%ROWCOUNT;


            UPDATE crm_partner_bank_account_overdraft
            SET term = CAST((close_date - open_date) AS NUMBER);


            INSERT
            INTO etl_log (log_date, table_name, row_affected)
            VALUES (SYSDATE, 'CRM_PARTNER_BANK_ACCOUNT_OVERDRAFT', row_count);
            COMMIT;


        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                error_code := SQLCODE;
                error_message := SQLERRM;
                INSERT
                INTO error_log (error_proc, error_code, error_message, error_line, error_date)
                VALUES ( 'proc_etl_bank_ovd_daily', error_code, error_message
                       , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
                COMMIT;

        END;
    END proc_etl_bank_ovd_daily;

    PROCEDURE proc_etl_bank_lcbg_daily
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        --TODO: Insert LC BG
        MERGE
        INTO crm_partner_bank_account_lcbg lcbg
        USING
            (SELECT DISTINCT b.partner_id                                              AS partner_id
                           , lcbg.balance                                              AS balance
                           , lcbg.contract_ref_no                                      AS contract_no
                           , lcbg.contract_ccy                                         AS currency
                           , lcbg.closure_date                                         AS maturity_date
                           , lcbg.issue_date                                           AS open_date
                           , NULL                                                      AS remark
                           , 'LC BG'                                                   AS type
                           , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))                   AS created_date
                           , NULL                                                      AS updated_date
                           , lcbg.branch                                               AS branch_code
                           , lcbg.closure_date                                         AS closure_date
                           , 'LC'                                                      AS account_type
                           , lcbg.contract_status                                      AS contract_status
                           , CASE
                                 WHEN lcbg.contract_ccy <> 'VND' THEN lcbg.balance * (SELECT mid_rate
                                                                                      FROM (SELECT DISTINCT mid_rate, ccy1
                                                                                            FROM crmstaging.cytm_rates@"Dbstaging580.Localdomain"
                                                                                            WHERE rate_type = 'STANDARD')
                                                                                      WHERE ccy1 = lcbg.contract_ccy)
                                                                 ELSE lcbg.balance END AS lcy_curr_balance
             FROM crmstaging.vw_lcbg@"Dbstaging580.Localdomain" lcbg
                , crm_partner b
             WHERE lcbg.customer_no = b.cif_no) daily_lcbg
        ON (lcbg.contract_no = daily_lcbg.contract_no)
        WHEN MATCHED THEN
            UPDATE
            SET partner_id       = daily_lcbg.partner_id
              , balance          = daily_lcbg.balance
              , currency         = daily_lcbg.currency
              , maturity_date    = daily_lcbg.maturity_date
              , open_date        = daily_lcbg.open_date
              , remark           = daily_lcbg.remark
              , type             = daily_lcbg.type
              , created_date     = daily_lcbg.created_date
              , branch_code      = daily_lcbg.branch_code
              --, created_date     = daily_lcbg.created_date
              --, branch_code      = daily_lcbg.branch_code
              , account_type     = daily_lcbg.account_type
              , contract_status  = daily_lcbg.contract_status
              , lcy_curr_balance = daily_lcbg.lcy_curr_balance
        WHEN NOT MATCHED THEN
            INSERT ( account_lcbg_id, partner_id, balance, contract_no, currency
                   , maturity_date, open_date, remark
                   , type, created_date, branch_code, account_type, contract_status
                   , lcy_curr_balance)
            VALUES ( crm_partner_bank_account_lcbg_seq.nextval
                   , daily_lcbg.partner_id
                   , daily_lcbg.balance
                   , daily_lcbg.contract_no
                   , daily_lcbg.currency
                   , daily_lcbg.maturity_date
                   , daily_lcbg.open_date
                   , daily_lcbg.remark
                   , daily_lcbg.type
                   , daily_lcbg.created_date
                   , daily_lcbg.branch_code
                   , daily_lcbg.account_type
                   , daily_lcbg.contract_status
                   , daily_lcbg.lcy_curr_balance);
        row_count := SQL%ROWCOUNT;


        MERGE
        INTO crm_partner_bank_account_lcbg lcbg
        USING
            (SELECT DISTINCT b.partner_id                                         AS partner_id
                           , bg.amount                                            AS amount
                           , bg.contract_ref_no                                   AS contract_no
                           , bg.contract_ccy                                      AS currency
                           , bg.closure_date                                      AS maturity_date
                           , bg.issue_date                                        AS open_date
                           , NULL                                                 AS remark
                           , 'LC BG'                                              AS type
                           , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))              AS created_date
                           , NULL                                                 AS updated_date
                           , bg.branch                                            AS branch_code
                           , bg.closure_date                                      AS closure_date
                           , 'BG'                                                 AS account_type
                           , bg.contract_status                                   AS contract_status
                           , CASE
                                 WHEN bg.contract_ccy <> 'VND' THEN bg.amount * (SELECT mid_rate
                                                                                 FROM (SELECT DISTINCT mid_rate, ccy1
                                                                                       FROM crmstaging.cytm_rates@"Dbstaging580.Localdomain"
                                                                                       WHERE rate_type = 'STANDARD')
                                                                                 WHERE ccy1 = bg.contract_ccy)
                                                               ELSE bg.amount END AS lcy_curr_balance

             FROM crmstaging.vw_guarantee@"Dbstaging580.Localdomain" bg
                , crm_partner b
             WHERE bg.customer_no = b.cif_no) daily_lcbg
        ON (lcbg.contract_no = daily_lcbg.contract_no)
        WHEN MATCHED THEN
            UPDATE
            SET partner_id       = daily_lcbg.partner_id
              , balance          = daily_lcbg.amount
              , currency         = daily_lcbg.currency
              , maturity_date    = daily_lcbg.maturity_date
              , open_date        = daily_lcbg.open_date
              , remark           = daily_lcbg.remark
              , type             = daily_lcbg.type
              , created_date     = daily_lcbg.created_date
              , branch_code      = daily_lcbg.branch_code
              --, created_date     = daily_lcbg.created_date
              --, branch_code      = daily_lcbg.branch_code
              , account_type     = daily_lcbg.account_type
              , contract_status  = daily_lcbg.contract_status
              , lcy_curr_balance = daily_lcbg.lcy_curr_balance
        WHEN NOT MATCHED THEN
            INSERT ( account_lcbg_id, partner_id, balance, contract_no, currency
                   , maturity_date, open_date, remark
                   , type, created_date, branch_code, account_type, contract_status
                   , lcy_curr_balance)
            VALUES ( crm_partner_bank_account_lcbg_seq.nextval
                   , daily_lcbg.partner_id
                   , daily_lcbg.amount
                   , daily_lcbg.contract_no
                   , daily_lcbg.currency
                   , daily_lcbg.maturity_date
                   , daily_lcbg.open_date
                   , daily_lcbg.remark
                   , daily_lcbg.type
                   , daily_lcbg.created_date
                   , daily_lcbg.branch_code
                   , daily_lcbg.account_type
                   , daily_lcbg.contract_status
                   , daily_lcbg.lcy_curr_balance);
        row_count := row_count + SQL%ROWCOUNT;

        INSERT
        INTO etl_log (log_date, table_name, row_affected)
        VALUES (SYSDATE, 'CRM_PARTNER_BANK_ACCOUNT_LCBG', row_count);
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_etl_bank_lcbg_daily', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;
    END proc_etl_bank_lcbg_daily;

    PROCEDURE proc_etl_bank_credit_card_daily
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        BEGIN

            MERGE
            INTO crm_partner_bank_account_credit_card crd
            USING
                (SELECT p.partner_id                                                                        AS partner_id
                      , card.cardlimit                                                                      AS card_limit
                      , NULL                                                                                AS user_id
                      , card.remain                                                                         AS balance
                      , card.ccy                                                                            AS currency
                      , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))                                             AS created_date
                      , NULL                                                                                AS updated_date
                      , NULL                                                                                AS term
                      , card.pan                                                                            AS card_no
                      , card.crd_stat                                                                       AS card_status
                      , card.crd_typ                                                                        AS card_type
                      , card.canceldate                                                                     AS expiry_date
                      , card.createdate                                                                     AS issuing_date
                      , card.minpayment                                                                     AS minimum_payment
                      , card.accountno                                                                      AS account_no
                      , CASE
                            WHEN SUBSTR(card.accountno, 0, 3) = '900' THEN TO_CHAR(card.branchpart)
                                                                      ELSE SUBSTR(card.accountno, 0, 3) END AS branch_code
                 FROM crmstaging.card@"Dbstaging580.Localdomain" card
                    , crm_partner p
                 WHERE card.cust_no = p.cif_no
                   AND (TRUNC(card.createdate) = TRUNC(SYSDATE - 1) OR
                        TRUNC(card.updatedate) = TRUNC(SYSDATE - 1))) daily_crd
            ON (crd.card_no = daily_crd.card_no)
            WHEN MATCHED THEN
                UPDATE
                SET partner_id      = daily_crd.partner_id
                  , user_id         = daily_crd.user_id
                  , balance         = daily_crd.balance
                  , card_limit      = daily_crd.card_limit
                  , card_status     = daily_crd.card_status
                  , card_type       = daily_crd.card_type
                  , currency        = daily_crd.currency
                  , expiry_date     = daily_crd.expiry_date
                  , issuing_date    = daily_crd.issuing_date
                  , minimum_payment = daily_crd.minimum_payment
                  , created_date    = daily_crd.created_date
                  , updated_date    = daily_crd.updated_date
                  , term            = daily_crd.term
                  , account_no      = daily_crd.account_no
                  , branch_code     = daily_crd.branch_code
            WHEN NOT MATCHED THEN
                INSERT ( account_credit_card_id, partner_id, user_id, balance, card_limit, card_no, card_status
                       , card_type
                       , currency, expiry_date, issuing_date, minimum_payment, created_date, updated_date, term
                       , account_no
                       , branch_code)
                VALUES ( crm_partner_bank_account_credit_card_seq.nextval
                       , daily_crd.partner_id
                       , daily_crd.user_id
                       , daily_crd.balance
                       , daily_crd.card_limit
                       , daily_crd.card_no
                       , daily_crd.card_status
                       , daily_crd.card_type
                       , daily_crd.currency
                       , daily_crd.expiry_date
                       , daily_crd.issuing_date
                       , daily_crd.minimum_payment
                       , daily_crd.created_date
                       , daily_crd.updated_date
                       , daily_crd.term
                       , daily_crd.account_no
                       , daily_crd.branch_code);
            row_count := SQL%ROWCOUNT;

            UPDATE crm_partner_bank_account_credit_card
            SET card_type_code = 'VISA_DEBIT'
            WHERE card_no LIKE '4424%';

            UPDATE crm_partner_bank_account_credit_card
            SET card_type_code = 'VISA_CREDIT'
            WHERE card_no LIKE '4705%';

            UPDATE crm_partner_bank_account_credit_card
            SET card_type_code = 'ATM'
            WHERE card_no LIKE '9704%';

            INSERT
            INTO etl_log (log_date, table_name, row_affected)
            VALUES (SYSDATE, 'PARTNER_BANK_ACCOUNT_CREDIT_CARD', row_count);
            COMMIT;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                error_code := SQLCODE;
                error_message := SQLERRM;
                INSERT
                INTO error_log (error_proc, error_code, error_message, error_line, error_date)
                VALUES ( 'proc_etl_bank_credit_card_daily', error_code, error_message
                       , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
                COMMIT;
        END;
    END proc_etl_bank_credit_card_daily;

    PROCEDURE proc_etl_bank_ibmb_daily
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        MERGE
        INTO crm_partner_bank_account_banking banking
        USING
            (SELECT DISTINCT p.partner_id
                           , mic.default_account                     AS account_no
                           , mic.created_date                        AS open_date
                           , 0                                       AS is_digital_wallet
                           , 0                                       AS is_ecommerce
                           , mic.status                              AS status
                           , 'IB'                                    AS account_type
                           , NULL                                    AS transaction_limit
                           , s.limit_ccy                             AS currency
                           , mic.branch_code
                           -- , mic.username                            AS contract_no
                           , CASE WHEN mic.username LIKE '%admin' THEN 'U1' || SUBSTR(mic.username,0,9)
                               ELSE mic.username END                 AS contract_no
                           , mic.cif
                           , mic.cust_type
                           , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                           , mic.created_by
             FROM crmstaging.mt_ib_customer@"Dbstaging580.Localdomain" mic
                , crm_partner p
                , crmstaging.sttm_customer@"Dbstaging580.Localdomain" s
             WHERE mic.cif = p.cif_no
               AND mic.cif = s.customer_no) daily_banking
        ON (banking.contract_no = daily_banking.contract_no)
        WHEN MATCHED THEN
            UPDATE
            SET partner_id        = daily_banking.partner_id
              , open_date         = daily_banking.open_date
              , is_digital_wallet = daily_banking.is_digital_wallet
              , is_ecommerce      = daily_banking.is_ecommerce
              , status            = daily_banking.status
              , account_type      = daily_banking.account_type
              , transaction_limit = daily_banking.transaction_limit
              , currency          = daily_banking.currency
              , branch_code       = daily_banking.branch_code
              --, contract_no       = daily_banking.contract_no
              , cif               = daily_banking.cif
              , cust_type         = daily_banking.cust_type
              , created_date      = daily_banking.created_date
              , created_by        = daily_banking.created_by
        WHEN NOT MATCHED THEN
            INSERT ( account_banking_id, partner_id, account_no, open_date, is_digital_wallet, is_ecommerce, status
                   , account_type, transaction_limit, currency, branch_code, contract_no, cif, cust_type, created_date
                   , created_by)
            VALUES ( crm_partner_bank_account_banking_seq.nextval
                   , daily_banking.partner_id
                   , daily_banking.account_no
                   , daily_banking.open_date
                   , daily_banking.is_digital_wallet
                   , daily_banking.is_ecommerce
                   , daily_banking.status
                   , daily_banking.account_type
                   , daily_banking.transaction_limit
                   , daily_banking.currency
                   , daily_banking.branch_code
                   , daily_banking.contract_no
                   , daily_banking.cif
                   , daily_banking.cust_type
                   , daily_banking.created_date
                   , daily_banking.created_by);
        row_count := SQL%ROWCOUNT;

        UPDATE crm_partner_bank_account_banking
        SET is_ecommerce = 1
        WHERE partner_id IN (SELECT partner_id
                             FROM crm_partner
                             WHERE cif_no IN
                                   (SELECT cif FROM crmstaging.mt_ib_ecom_register@"Dbstaging580.Localdomain"));

        UPDATE crm_partner_bank_account_banking
        SET is_digital_wallet = 1
        WHERE partner_id IN (SELECT partner_id
                             FROM crm_partner
                             WHERE cif_no IN
                                   (SELECT cif FROM crmstaging.mt_ib_momo_register@"Dbstaging580.Localdomain"));

        UPDATE crm_partner_bank_account_banking a
        SET transaction_limit = (SELECT limit
         FROM (SELECT b.partner_id, SUM(limit_day) limit
               FROM crmstaging.vw_mt_ib_limit_customer@"Dbstaging580.Localdomain" a
                  , crm_partner b
               WHERE a.cif = b.cif_no
               GROUP BY b.partner_id)
         WHERE partner_id = a.partner_id);

        UPDATE crm_partner_bank_account_banking
        SET transaction_limit = (SELECT limit_day
                                 FROM crmstaging.vw_mt_ib_parameter@"Dbstaging580.Localdomain")
        WHERE transaction_limit IS NULL;

        INSERT
        INTO etl_log (log_date, table_name, row_affected)
        VALUES (SYSDATE, 'PARTNER_BANK_ACCOUNT_BANKING', row_count);
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_etl_bank_ibmb_daily', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;
    END proc_etl_bank_ibmb_daily;

    PROCEDURE proc_etl_bank_transaction_daily
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        BEGIN
            DELETE FROM crm_partner_transaction WHERE TRUNC(updated_date) = TRUNC(SYSDATE);
            INSERT
            INTO crm_partner_transaction( transaction_id, partner_id, account_no, related_account, trn_ref_no, amount
                                        , currency, description
                                        , created_date, transaction_type, updated_date)
            WITH data AS
                     (SELECT b.partner_id       AS partner_id
                           , tn.ac_no           AS account_no
                           , tn.related_account AS related_account
                           , tn.trn_ref_no      AS trn_ref_no
                           , tn.amount          AS amount
                           , tn.currency        AS currency
                           , tn.trn_desc        AS description
                           , tn.value_dt        AS created_date
                           , tn.drcr_ind        AS transaction_type
                      FROM crmstaging.vw_transaction@"Dbstaging580.Localdomain" tn
                         , crm_partner b
                      WHERE tn.customer_no = b.cif_no
                        AND TRUNC(tn.trn_dt) = TRUNC(SYSDATE - 1))
            SELECT crm_partner_transaction_seq.nextval
                 , partner_id
                 , account_no
                 , related_account
                 , trn_ref_no
                 , amount
                 , currency
                 , description
                 , created_date
                 , transaction_type
                 , SYSDATE
            FROM data;
            row_count := SQL%ROWCOUNT;

            INSERT
            INTO etl_log (log_date, table_name, row_affected)
            VALUES (SYSDATE, 'CRM_PARTNER_TRANSACTION', row_count);
            COMMIT;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                error_code := SQLCODE;
                error_message := SQLERRM;
                INSERT
                INTO error_log (error_proc, error_code, error_message, error_line, error_date)
                VALUES ( 'proc_etl_bank_transaction_daily', error_code, error_message
                       , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
                COMMIT;
        END;
    END proc_etl_bank_transaction_daily;

    PROCEDURE
        proc_etl_bank_collateral_daily AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        BEGIN

            MERGE
            INTO crm_partner_collateral collat
            USING
                (SELECT NULL                                           AS user_id
                      , cp.partner_id                                  AS partner_id
                      , CASE
                            WHEN lmtm.collateral_type IN ('01_Q_SU_DUNG_DAT_TAI_SAN_GLTD')
                                THEN (SELECT select_id
                                      FROM crm_base_meta_select
                                      WHERE fn_remove_accents(name) = 'Bat dong san'
                                        AND type_id = 'PARTNER_COLLATERAL_TYPE')
                            WHEN lmtm.collateral_type IN ('03C_GIAY_TO_CO_GIA_TCTD_CHUA_N',
                                                          '07B_KY_PHIEU_TCTD_CHUA_NIEMYET',
                                                          '10B_GIAY_TO_KHAC_DN_DA_NIEMYET',
                                                          '08B_CHUNG_CHI_TIEN_GUI_FCY',
                                                          '03B_GIAY_TO_CO_GIA_DN_DA_N_YET',
                                                          '10C_GIAY_TO_KHAC_TCTD_CHUA_N_Y',
                                                          '08A_CHUNG_CHI_TIEN_GUI_VND',
                                                          '03A_GIAY_TO_CO_GIA_TCTD_DA_N_Y',
                                                          '05B_CO_PHIEU_DN_DA_NIEM_YET',
                                                          '10A_GIAY_TO_KHAC_TCTD_DA_N_YET',
                                                          '04B_TRAI_PHIEU_CP_1_DEN_5_NAM',
                                                          '05A_CO_PHIEU_TCTD_DA_NIEM_YET',
                                                          'GTCG_KH_DUA_CK_TCK_DA_CQSH')
                                THEN (SELECT select_id
                                      FROM crm_base_meta_select
                                      WHERE fn_remove_accents(name) = 'Giay to co gia'
                                        AND type_id = 'PARTNER_COLLATERAL_TYPE')
                            WHEN lmtm.collateral_type IN ('02_PHUONG_TIEN_GIAO_THONG_VTAI')
                                THEN (SELECT select_id
                                      FROM crm_base_meta_select
                                      WHERE fn_remove_accents(name) = 'O to'
                                        AND type_id = 'PARTNER_COLLATERAL_TYPE')
                            WHEN lmtm.collateral_type IN ('13_MAY_MOC_VA_THIET_BI',
                                                          'MAY_MOC_KHONG_MO_MOI',
                                                          '14_DAY_CHUYEN_SAN_XUAT')
                                THEN (SELECT select_id
                                      FROM crm_base_meta_select
                                      WHERE fn_remove_accents(name) = 'May moc thiet bi'
                                        AND type_id = 'PARTNER_COLLATERAL_TYPE')
                            WHEN lmtm.collateral_type IN ('17_HANG_HOA_KHAC',
                                                          '12_NGUYEN_VAT_LIEU_VA_HANG_HOA',
                                                          '15_NGUYEN_NHIEN_VAT_LIEU',
                                                          '26_QUYEN_TAI_SAN_KHAC',
                                                          '23_QUYEN_GOP_VON_DOANH_NGHIEP',
                                                          '27_TAI_SAN_KHAC',
                                                          '21_QUYEN_DOI_NO',
                                                          '16_HANG_TIEU_DUNG',
                                                          '18_TAI_SAN_DANG_KI_QSD_SH_KHAC',
                                                          'TSDB_TIEN_KY_QUY',
                                                          'BAO_LANH_KHONG_HUY_NGANG')
                                THEN (SELECT select_id
                                      FROM crm_base_meta_select
                                      WHERE fn_remove_accents(name) = 'Khac'
                                        AND type_id = 'PARTNER_COLLATERAL_TYPE')
                        END                                            AS collateral_type_id
                      , lmtm.collateral_value                          AS amount
                      , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))        AS created_date
                      , NULL                                           AS updated_date
                      , (SELECT currency_id
                         FROM crm_base_currency
                         WHERE code = lmtm.collateral_currency)
                                                                       AS currency_id
                      , NVL(b.field_val_8, 'CHINH CHU')                AS owner_name
                      , b.field_val_2                                  AS guarantor_name
                      , lmtm.start_date                                AS valuation_date
                      , todate(b.field_val_16)
                                                                       AS latest_valuation_date
                      , b.field_val_4                                  AS identity_paper_number
                      , (SELECT address_id
                         FROM crm_boundary_address
                         WHERE cif_no = lmtm.liab_id
                           AND collateral_code = lmtm.collateral_code) AS address_id
                      , lmtm.review_date                               AS termination_date
                      , lmtm.record_stat                               AS record_stat
                      , lmtm.collateral_code                           AS collateral_code

                 FROM crmstaging.lmtm_collat@"Dbstaging580.Localdomain" lmtm
                    , crmstaging.cstm_function_userdef_fields@"Dbstaging580.Localdomain" b
                    , crm_partner cp
                 WHERE lmtm.collateral_code || '~' || lmtm.liab_id || '~' = b.rec_key
                   AND function_id = 'LMDCOLLT'
                   AND cp.cif_no = lmtm.liab_id
                   AND TRUNC(lmtm.checker_dt_stamp) = TRUNC(SYSDATE - 1)) daily_collat
            ON (collat.collateral_code = daily_collat.collateral_code)
            WHEN MATCHED THEN
                UPDATE
                SET user_id                 = daily_collat.user_id
                  , partner_id              = daily_collat.partner_id
                  , collateral_type_id      = daily_collat.collateral_type_id
                  , amount                  = daily_collat.amount
                  , created_date            = daily_collat.created_date
                  , updated_date            = daily_collat.updated_date
                  , currency_id             = daily_collat.currency_id
                  , owner_name              = daily_collat.owner_name
                  , guarantor_name          = daily_collat.guarantor_name
                  , valuation_date          = daily_collat.valuation_date
                  , valuation_maturity_date = daily_collat.termination_date - daily_collat.latest_valuation_date
                  , identity_paper_number   = daily_collat.identity_paper_number
                  , address_id              = daily_collat.address_id
                  , termination_date        = daily_collat.termination_date
                  , record_stat             = daily_collat.record_stat
                  , latest_valuation_date   = daily_collat.latest_valuation_date
            WHEN NOT MATCHED THEN
                INSERT ( collateral_id, user_id, partner_id, collateral_type_id, amount, created_date, updated_date
                       , currency_id
                       , owner_name, guarantor_name, valuation_date, valuation_maturity_date
                       , identity_paper_number, address_id, termination_date, record_stat, collateral_code
                       , latest_valuation_date)
                VALUES ( crm_partner_bank_account_banking_seq.nextval
                       , daily_collat.user_id
                       , daily_collat.partner_id
                       , daily_collat.collateral_type_id
                       , daily_collat.amount
                       , daily_collat.created_date
                       , daily_collat.updated_date
                       , daily_collat.currency_id
                       , daily_collat.owner_name
                       , daily_collat.guarantor_name
                       , daily_collat.valuation_date
                       , daily_collat.termination_date - daily_collat.latest_valuation_date
                       , daily_collat.identity_paper_number
                       , daily_collat.address_id
                       , daily_collat.termination_date
                       , daily_collat.record_stat
                       , daily_collat.collateral_code
                       , daily_collat.latest_valuation_date);
            row_count := SQL%ROWCOUNT;

            INSERT
            INTO etl_log (log_date, table_name, row_affected)
            VALUES (SYSDATE, 'CRM_PARTNER_COLLATERAL', row_count);
            COMMIT;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                error_code := SQLCODE;
                error_message := SQLERRM;
                INSERT
                INTO error_log (error_proc, error_code, error_message, error_line, error_date)
                VALUES ( 'proc_etl_bank_collateral_daily', error_code, error_message
                       , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
                COMMIT;
        END;
    END proc_etl_bank_collateral_daily;

    PROCEDURE
        proc_crm_partner_holding_daily AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        --TODO: Insert CRM PARTNER HOLDING
        MERGE
        INTO crm_partner_holding holding
        USING
            (SELECT DISTINCT p.partner_id
                           , bph.casa                                                        AS casa
                           , bph.tgtk                                                        AS fd
                           , bph.od                                                          AS overdraft
                           , bph.tvtc                                                        AS unsecured_loan
                           , bph.tv_the_chap                                                 AS secured_loan
                           , bph.visa_debit                                                  AS debit_card
                           , bph.visa_credit                                                 AS credit_card
                           , bph.ib                                                          AS i_banking
                           , bph.mb                                                          AS m_banking
                           , 0                                                               AS life_insurance
                           , 0                                                               AS non_life_insurance
                           , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))                         AS created_date
                           , CASE
                                 WHEN EXISTS(SELECT 1
                                             FROM crm_partner_bank_account_lcbg
                                             WHERE p.partner_id = crm_partner_bank_account_lcbg.partner_id
                                               AND account_type = 'LC')
                                     THEN 1
                                     ELSE 0 END                                              AS lc
                           , CASE
                                 WHEN EXISTS(SELECT 1
                                             FROM crm_partner_bank_account_lcbg
                                             WHERE p.partner_id = crm_partner_bank_account_lcbg.partner_id
                                               AND account_type = 'BG')
                                     THEN 1
                                     ELSE 0 END                                              AS bg
                           , CASE WHEN bph.tvtc = 1 OR bph.tv_the_chap = 1 THEN 1 ELSE 0 END AS loan
             FROM crmstaging.base_partner_holding@"Dbstaging580.Localdomain" bph
                , crm_partner p
             WHERE bph.cif = p.cif_no) daily_holding
        ON (holding.partner_id = daily_holding.partner_id)
        WHEN MATCHED THEN
            UPDATE
            SET casa               = NVL(daily_holding.casa, 0)
              , credit_card        = NVL(daily_holding.credit_card, 0)
              , debit_card         = NVL(daily_holding.debit_card, 0)
              , fd                 = NVL(daily_holding.fd, 0)
              , i_banking          = NVL(daily_holding.i_banking, 0)
              , m_banking          = NVL(daily_holding.m_banking, 0)
              , life_insurance     = NVL(daily_holding.life_insurance, 0)
              , non_life_insurance = NVL(daily_holding.non_life_insurance, 0)
              , overdraft          = NVL(daily_holding.overdraft, 0)
              , secured_loan       = NVL(daily_holding.secured_loan, 0)
              , unsecured_loan     = NVL(daily_holding.unsecured_loan, 0)
              , created_date       = daily_holding.created_date
              , lc                 = NVL(daily_holding.lc, 0)
              , bg                 = NVL(daily_holding.bg, 0)
              , loan               = NVL(daily_holding.loan, 0)
        WHEN NOT MATCHED THEN
            INSERT ( partner_id, casa, credit_card, debit_card, fd, i_banking, m_banking, life_insurance
                   , non_life_insurance
                   , overdraft, secured_loan, unsecured_loan, created_date, lc, bg, loan)
            VALUES ( daily_holding.partner_id
                   , NVL(daily_holding.casa, 0)
                   , NVL(daily_holding.credit_card, 0)
                   , NVL(daily_holding.debit_card, 0)
                   , NVL(daily_holding.fd, 0)
                   , NVL(daily_holding.i_banking, 0)
                   , NVL(daily_holding.m_banking, 0)
                   , NVL(daily_holding.life_insurance, 0)
                   , NVL(daily_holding.non_life_insurance, 0)
                   , NVL(daily_holding.overdraft, 0)
                   , NVL(daily_holding.secured_loan, 0)
                   , NVL(daily_holding.unsecured_loan, 0)
                   , daily_holding.created_date
                   , NVL(daily_holding.lc, 0)
                   , NVL(daily_holding.bg, 0)
                   , NVL(daily_holding.loan, 0));
        row_count := SQL%ROWCOUNT;

        INSERT
        INTO etl_log (log_date, table_name, row_affected)
        VALUES (SYSDATE, 'CRM_PARTNER_HOLDING', row_count);
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_crm_partner_holding_daily', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;
    END proc_crm_partner_holding_daily;

    PROCEDURE
        proc_crm_partner_contract_daily AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN


        --LOAN
        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT c.partner_id                            AS partner_id
                  , ln.contract_no                          AS contract_no
                  , 'LOAN'                                  AS contract_type_id
                  , ln.outstanding                          AS balance
                  , ln.currency                             AS currency_code
                  , ln.term                                 AS term
                  , ln.rate                                 AS interest_rate
                  , ln.open_date                            AS open_date
                  , ln.maturity_date                        AS maturity_date
                  , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                  , SUBSTR(ln.contract_no, 0, 3)            AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_loan ln
             WHERE c.partner_id = ln.partner_id
               AND TRUNC(ln.created_date) = TRUNC(SYSDATE)) daily_contract
        ON (contract.contract_no = daily_contract.contract_no)
        WHEN MATCHED THEN
            UPDATE
            SET partner_id    = daily_contract.partner_id
              --, contract_type_id = daily_contract.contract_type_id
              , balance       = daily_contract.balance
              , currency_code = daily_contract.currency_code
              , term          = daily_contract.term
              , interest_rate = daily_contract.interest_rate
              , open_date     = daily_contract.open_date
              , maturity_date = daily_contract.maturity_date
              , created_date  = daily_contract.created_date
              , branch_code   = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);
        row_count := SQL%ROWCOUNT;

        --CASA
        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT c.partner_id                            AS partner_id
                  , cs.account_no                           AS contract_no
                  , 'CASA'                                  AS contract_type_id
                  , cs.available_balance                    AS balance
                  , cs.currency                             AS currency_code
                  , NULL                                    AS term
                  , cs.rate                                 AS interest_rate
                  , cs.open_date                            AS open_date
                  , NULL                                    AS maturity_date
                  , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                  , cs.branch_code                          AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_casa cs
             WHERE c.partner_id = cs.partner_id
               AND TRUNC(cs.created_date) = TRUNC(SYSDATE)) daily_contract
        ON (contract.contract_no || contract.contract_type_id = daily_contract.contract_no || 'CASA')
        WHEN MATCHED THEN
            UPDATE
            SET partner_id    = daily_contract.partner_id
              --, contract_type_id = daily_contract.contract_type_id
              , balance       = daily_contract.balance
              , currency_code = daily_contract.currency_code
              , term          = daily_contract.term
              , interest_rate = daily_contract.interest_rate
              , open_date     = daily_contract.open_date
              , maturity_date = daily_contract.maturity_date
              , created_date  = daily_contract.created_date
              , branch_code   = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);
        row_count := row_count + SQL%ROWCOUNT;

        --VISA_DEBIT
        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT c.partner_id                            AS partner_id
                  , card.card_no                            AS contract_no
                  , 'DEBIT_CARD'                            AS contract_type_id
                  , card.balance                            AS balance
                  , card.currency                           AS currency_code
                  , NULL                                    AS term
                  , NULL                                    AS interest_rate
                  , card.issuing_date                       AS open_date
                  , card.expiry_date                        AS maturity_date
                  , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                  , card.branch_code                        AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_credit_card card
             WHERE c.partner_id = card.partner_id
               AND card.card_type_code = 'VISA_DEBIT'
               AND TRUNC(card.created_date) = TRUNC(SYSDATE)) daily_contract
        ON (contract.contract_no || contract.contract_type_id = daily_contract.contract_no || 'DEBIT_CARD')
        WHEN MATCHED THEN
            UPDATE
            SET partner_id    = daily_contract.partner_id
              --, contract_type_id = daily_contract.contract_type_id
              , balance       = daily_contract.balance
              , currency_code = daily_contract.currency_code
              , term          = daily_contract.term
              , interest_rate = daily_contract.interest_rate
              , open_date     = daily_contract.open_date
              , maturity_date = daily_contract.maturity_date
              , created_date  = daily_contract.created_date
              , branch_code   = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);
        row_count := row_count + SQL%ROWCOUNT;

        --VISA_CREDIT
        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT c.partner_id                            AS partner_id
                  , card.card_no                            AS contract_no
                  , 'CREDIT_CARD'                           AS contract_type_id
                  , card.balance                            AS balance
                  , card.currency                           AS currency_code
                  , NULL                                    AS term
                  , NULL                                    AS interest_rate
                  , card.issuing_date                       AS open_date
                  , card.expiry_date                        AS maturity_date
                  , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                  , card.branch_code                        AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_credit_card card
             WHERE c.partner_id = card.partner_id
               AND card.card_type_code = 'VISA_CREDIT'
               AND TRUNC(card.created_date) = TRUNC(SYSDATE)) daily_contract
        ON (contract.contract_no || contract.contract_type_id = daily_contract.contract_no || 'CREDIT_CARD')
        WHEN MATCHED THEN
            UPDATE
            SET partner_id    = daily_contract.partner_id
              --, contract_type_id = daily_contract.contract_type_id
              , balance       = daily_contract.balance
              , currency_code = daily_contract.currency_code
              , term          = daily_contract.term
              , interest_rate = daily_contract.interest_rate
              , open_date     = daily_contract.open_date
              , maturity_date = daily_contract.maturity_date
              , created_date  = daily_contract.created_date
              , branch_code   = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);
        row_count := row_count + SQL%ROWCOUNT;

        --ATM
        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT c.partner_id                            AS partner_id
                  , card.card_no                            AS contract_no
                  , 'ATM_CARD'                              AS contract_type_id
                  , card.balance                            AS balance
                  , card.currency                           AS currency_code
                  , NULL                                    AS term
                  , NULL                                    AS interest_rate
                  , card.issuing_date                       AS open_date
                  , card.expiry_date                        AS maturity_date
                  , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                  , card.branch_code                        AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_credit_card card
             WHERE c.partner_id = card.partner_id
               AND card.card_type_code = 'ATM'
               AND TRUNC(card.created_date) = TRUNC(SYSDATE)) daily_contract
        ON (contract.contract_no || contract.contract_type_id = daily_contract.contract_no || 'ATM_CARD')
        WHEN MATCHED THEN
            UPDATE
            SET partner_id    = daily_contract.partner_id
              --, contract_type_id = daily_contract.contract_type_id
              , balance       = daily_contract.balance
              , currency_code = daily_contract.currency_code
              , term          = daily_contract.term
              , interest_rate = daily_contract.interest_rate
              , open_date     = daily_contract.open_date
              , maturity_date = daily_contract.maturity_date
              , created_date  = daily_contract.created_date
              , branch_code   = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);
        row_count := row_count + SQL%ROWCOUNT;

        --FD
        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT c.partner_id                            AS partner_id
                  , fd.account_no                           AS contract_no
                  , 'FD'                                    AS contract_type_id
                  , fd.balance                              AS balance
                  , fd.currency                             AS currency_code
                  , fd.term                                 AS term
                  , fd.rate                                 AS interest_rate
                  , fd.open_date                            AS open_date
                  , fd.close_date                           AS maturity_date
                  , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                  , fd.branch_code                          AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_fd fd
             WHERE c.partner_id = fd.partner_id
               AND TRUNC(fd.created_date) = TRUNC(SYSDATE)) daily_contract
        ON (contract.contract_no || contract.contract_type_id = daily_contract.contract_no || 'FD')
        WHEN MATCHED THEN
            UPDATE
            SET partner_id    = daily_contract.partner_id
              --, contract_type_id = daily_contract.contract_type_id
              , balance       = daily_contract.balance
              , currency_code = daily_contract.currency_code
              , term          = daily_contract.term
              , interest_rate = daily_contract.interest_rate
              , open_date     = daily_contract.open_date
              , maturity_date = daily_contract.maturity_date
              , created_date  = daily_contract.created_date
              , branch_code   = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);
        row_count := row_count + SQL%ROWCOUNT;

        -- LCBG
        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT DISTINCT c.partner_id
                           , lcbg.contract_no                        AS contract_no
                           , lcbg.account_type                       AS contract_type_id
                           , lcbg.balance                            AS balance
                           , lcbg.currency                           AS currency_code
                           , lcbg.tenor                              AS term
                           , NULL                                    AS interest_rate
                           , lcbg.open_date                          AS open_date
                           , lcbg.maturity_date                      AS maturity_date
                           , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                           , lcbg.branch_code                        AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_lcbg lcbg
             WHERE c.partner_id = lcbg.partner_id
               AND lcbg.account_type = 'LC') daily_contract
        ON (contract.contract_no || contract.contract_type_id = daily_contract.contract_no || 'LC')
        WHEN MATCHED THEN
            UPDATE
            SET partner_id    = daily_contract.partner_id
              , balance       = daily_contract.balance
              , currency_code = daily_contract.currency_code
              , term          = daily_contract.term
              , interest_rate = daily_contract.interest_rate
              , open_date     = daily_contract.open_date
              , maturity_date = daily_contract.maturity_date
              , created_date  = daily_contract.created_date
              , branch_code   = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);
        row_count := row_count + SQL%ROWCOUNT;

        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT DISTINCT c.partner_id
                           , lcbg.contract_no                        AS contract_no
                           , lcbg.account_type                       AS contract_type_id
                           , lcbg.balance                            AS balance
                           , lcbg.currency                           AS currency_code
                           , lcbg.tenor                              AS term
                           , NULL                                    AS interest_rate
                           , lcbg.open_date                          AS open_date
                           , lcbg.maturity_date                      AS maturity_date
                           , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                           , lcbg.branch_code                        AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_lcbg lcbg
             WHERE c.partner_id = lcbg.partner_id
               AND lcbg.account_type = 'BG') daily_contract
        ON (contract.contract_no || contract.contract_type_id = daily_contract.contract_no || 'BG')
        WHEN MATCHED THEN
            UPDATE
            SET partner_id    = daily_contract.partner_id
              , balance       = daily_contract.balance
              , currency_code = daily_contract.currency_code
              , term          = daily_contract.term
              , interest_rate = daily_contract.interest_rate
              , open_date     = daily_contract.open_date
              , maturity_date = daily_contract.maturity_date
              , created_date  = daily_contract.created_date
              , branch_code   = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);
        row_count := row_count + SQL%ROWCOUNT;

        --OVERDRAFT
--        MERGE
--        INTO crm_partner_contract contract
--        USING
--            (SELECT c.partner_id                            AS partner_id
--                  , ovd.account_no                          AS contract_no
--                  , 'OVERDRAFT'                             AS contract_type_id
--                  , ovd.balance                             AS balance
--                  , ovd.currency                            AS currency_code
--                  , ovd.term                                AS term
--                  , ovd.rate                                AS interest_rate
--                  , ovd.open_date                           AS open_date
--                  , ovd.close_date                          AS maturity_date
--                  , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
--                  , SUBSTR(ovd.account_no, 0, 3)            AS branch_code
--             FROM crm_partner c
--                , crm_partner_bank_account_overdraft ovd
--             WHERE c.partner_id = ovd.partner_id
--               AND TRUNC(ovd.created_date) = TRUNC(SYSDATE)) daily_contract
--        ON (contract.contract_no = daily_contract.contract_no)
--        WHEN NOT MATCHED THEN
--            INSERT ( contract_id
--                   , partner_id
--                   , contract_no
--                   , contract_type_id
--                   , balance, currency_code
--                   , term, interest_rate
--                   , open_date, maturity_date
--                   , created_date
--                   , branch_code)
--            VALUES ( crm_partner_contract_seq.nextval
--                   , daily_contract.partner_id
--                   , daily_contract.contract_no
--                   , daily_contract.contract_type_id
--                   , daily_contract.balance
--                   , daily_contract.currency_code
--                   , daily_contract.term
--                   , daily_contract.interest_rate
--                   , daily_contract.open_date
--                   , daily_contract.maturity_date
--                   , daily_contract.created_date
--                   , daily_contract.branch_code);
--        row_count := row_count + SQL%ROWCOUNT;

        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT c.partner_id                            AS partner_id
                  , ovd.account_no                          AS contract_no
                  , 'OVERDRAFT'                             AS contract_type_id
                  , ovd.balance                             AS balance
                  , ovd.currency                            AS currency_code
                  , ovd.term                                AS term
                  , ovd.rate                                AS interest_rate
                  , ovd.open_date                           AS open_date
                  , ovd.close_date                          AS maturity_date
                  , ovd.created_date                        AS created_date
                  , SUBSTR(ovd.account_no, 0, 3)            AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_overdraft ovd
             WHERE c.partner_id = ovd.partner_id
             AND account_no not in (SELECT contract_no FROM crm_partner_bank_account_loan)) daily_contract
        ON (contract.contract_no = daily_contract.contract_no)
        WHEN MATCHED THEN
             UPDATE
            SET partner_id              = daily_contract.partner_id
              , contract_type_id        = daily_contract.contract_type_id
              , balance                 = daily_contract.balance
              , currency_code           = daily_contract.currency_code
              , term                    = daily_contract.term
              , interest_rate           = daily_contract.interest_rate
              , open_date               = daily_contract.open_date
              , maturity_date           = daily_contract.maturity_date
              , created_date            = daily_contract.created_date
              , branch_code             = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);

        --IBMB
        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT DISTINCT --crm_partner_contract_seq.nextval
                 c.partner_id                                        AS partner_id
                           , ibmb.contract_no                        AS contract_no
                           , 'I_BANKING'                             AS contract_type_id
                           , NULL                                    AS balance
                           , ibmb.currency                           AS currency_code
                           , NULL                                    AS term
                           , NULL                                    AS interest_rate
                           , ibmb.open_date                          AS open_date
                           , NULL                                    AS maturity_date
                           , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                           , ibmb.branch_code                        AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_banking ibmb
             WHERE c.partner_id = ibmb.partner_id
               AND ibmb.cust_type = 'I') daily_contract
        ON (contract.contract_no || contract.contract_type_id = daily_contract.contract_no || 'I_BANKING')
        WHEN MATCHED THEN
            UPDATE
            SET partner_id    = daily_contract.partner_id
              , balance       = daily_contract.balance
              , currency_code = daily_contract.currency_code
              , term          = daily_contract.term
              , interest_rate = daily_contract.interest_rate
              , open_date     = daily_contract.open_date
              , maturity_date = daily_contract.maturity_date
              , created_date  = daily_contract.created_date
              , branch_code   = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);
        row_count := row_count + SQL%ROWCOUNT;

        MERGE
        INTO crm_partner_contract contract
        USING
            (SELECT DISTINCT --crm_partner_contract_seq.nextval
                 c.partner_id                                        AS partner_id
                           , ibmb.contract_no                        AS contract_no
                           , 'I_BANKING'                             AS contract_type_id
                           , NULL                                    AS balance
                           , ibmb.currency                           AS currency_code
                           , NULL                                    AS term
                           , NULL                                    AS interest_rate
                           , ibmb.open_date                          AS open_date
                           , NULL                                    AS maturity_date
                           , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                           , ibmb.branch_code                        AS branch_code
             FROM crm_partner c
                , crm_partner_bank_account_banking ibmb
             WHERE c.partner_id = ibmb.partner_id
               AND ibmb.cust_type = 'C'
               AND contract_no LIKE 'U1%') daily_contract
        ON (contract.contract_no || contract.contract_type_id = daily_contract.contract_no || 'I_BANKING')
        WHEN MATCHED THEN
            UPDATE
            SET partner_id    = daily_contract.partner_id
              , balance       = daily_contract.balance
              , currency_code = daily_contract.currency_code
              , term          = daily_contract.term
              , interest_rate = daily_contract.interest_rate
              , open_date     = daily_contract.open_date
              , maturity_date = daily_contract.maturity_date
              , created_date  = daily_contract.created_date
              , branch_code   = daily_contract.branch_code
        WHEN NOT MATCHED THEN
            INSERT ( contract_id
                   , partner_id
                   , contract_no
                   , contract_type_id
                   , balance, currency_code
                   , term, interest_rate
                   , open_date, maturity_date
                   , created_date
                   , branch_code)
            VALUES ( crm_partner_contract_seq.nextval
                   , daily_contract.partner_id
                   , daily_contract.contract_no
                   , daily_contract.contract_type_id
                   , daily_contract.balance
                   , daily_contract.currency_code
                   , daily_contract.term
                   , daily_contract.interest_rate
                   , daily_contract.open_date
                   , daily_contract.maturity_date
                   , daily_contract.created_date
                   , daily_contract.branch_code);
        row_count := row_count + SQL%ROWCOUNT;

        --TODO: Update contract
        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND c.loai_hinh_vay IN ('I.10.BDS-MUA NHA DE O',
                                                        'I.8. BDS - QSDD',
                                                        'II.1.TD-MUA,SC NHA VA DAT')
                                AND c.product_code NOT IN ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3',
                                                           'VPT3', 'VPD3')
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND c.loai_hinh_vay IN ('I.11.BDS-XAY SUA NHA',
                                                        'I.6. BDS-XD,SC NHA DE O')
                                AND c.product_code NOT IN ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3',
                                                           'VPT3', 'VPD3')
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND c.loai_hinh_vay IN ('II.2. TD-MUA PT DI LAI',
                                                        'II.8.TD-MUA OTO', 'IX.DT PT VAN TAI')
                                AND c.product_code NOT IN ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3',
                                                           'VPT3', 'VPD3')
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND c.loai_hinh_vay IN ('II.4.TD-H.TAP' || '&' || 'CH.BENH DOM',
                                                        'II.5. TD-MUA NOI THAT GD', 'II.7. TD- CV NHU CAU KHAC')
                                AND c.product_code NOT IN ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3',
                                                           'VPT3', 'VPD3',
                                                           'EDNK', 'EDTK', 'EDDK', 'EDNH', 'EDTH', 'EDDH')
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND (c.loai_hinh_vay IN ('II.TD.12. DU_HOC')
                                  OR c.product_code IN ('EDNK', 'EDTK', 'EDDK', 'EDNH', 'EDTH', 'EDDH'))
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND (c.loai_hinh_vay IN
                                     ('II.3.TD-H.TAP' || '&' || 'CH.BENH OVS', 'II.9.TD-THAM.NGTHAN OVS',
                                      'II.10.TD-DU LICH OVS', 'II.11.CMTC-HTAP' || '&' || 'CBENH OVS')
                                  OR c.product_code IN
                                     ('N11A', 'N11M', 'T11A', 'T11M', 'S00A', 'S00M', 'M00A', 'M00M'))
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'MORTAGED_VALUEABLE_PAPER_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND (c.loai_hinh_vay IN ('X.GTCG.1. CAM_CO_NHOM _I', 'X.GTCG.2. CAM_CO_NHOM _II',
                                                         'X.GTCG.3. CAM_CO_NHOM _III')
                                  OR c.product_code IN
                                     ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3', 'VPT3', 'VPD3'))
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'UNSERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND c.loai_hinh_vay NOT IN ('XV.79.THAUCHI_KD_CO_TSDB', 'XV.80.THAUCHI_TD_CO_TSDB',
                                                            'XV.78.THAU_CHI_TIN_CHAP')
                                AND c.product_code NOT IN ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3',
                                                           'VPT3', 'VPD3')
                                AND c.muc_dich_vay = ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'UNSERCURED_LOAN'
        WHERE contract_no IN (SELECT c.account_number
                              FROM crmstaging.cltb_account_master@"Dbstaging580.Localdomain" c
                              WHERE c.product_code = 'VISA')
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND c.loai_hinh_vay IN ('VI.CV-KINH DOANH TM,DV', 'VII.1.CV SX CONG NGHIEP',
                                                        'VII.2.CVSX NONG,LAMNGHIEP', 'VII.3.NT,CB THUY HAI SAN',
                                                        'VII.4.CV SX KHAC', 'VIII.CV XD CAU, DUONG',
                                                        'III.1.XNK - CV XUAT KHAU', 'III.2.XNK - CV NHAP KHAU')
                                AND c.product_code NOT IN ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3',
                                                           'VPT3', 'VPD3')
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND c.loai_hinh_vay IN
                                    ('I.1. BDS - XD CSHT KD', 'I.4. BDS - XD VPCHOTHUE', 'I.2. BDS - XD KCN-KCX',
                                     'I.3. BDS- XD KHU DO THI',
                                     'I.5. BDS-XD TT TMAI', 'I.7.BDS-XD,SC NHA DEBAN', 'I.9. BDS-DT,KD BDS KHAC',
                                     'I.12.BDS-XD,SC NH,KS,KDL')
                                AND c.product_code NOT IN ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3',
                                                           'VPT3', 'VPD3')
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND c.loai_hinh_vay IN ('IV.2.VAY MUA CK=CCCK/TS #', 'IV.3.CV UNG TRUOC MUA CK',
                                                        'IV.4.CV BSUNG KLENH CK', 'IV.7.CKHAU GTCG-KH MUA CK',
                                                        'IV.8.CV' || '&' || 'CKHAU GTCG#MUA CK',
                                                        'IV.5.CV NLD-MCP L1 CTNN', 'IV.6.CV GOPVON/MUA CC QDT')
                                AND c.product_code NOT IN ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3',
                                                           'VPT3', 'VPD3')
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND c.loai_hinh_vay IN ('III.3.XKLD')
                                AND c.product_code NOT IN ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3',
                                                           'VPT3', 'VPD3')
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'SERCURED_LOAN'
        WHERE contract_no IN (SELECT c.contract_no
                              FROM crm_partner_bank_account_loan c
                                 , crm_partner p
                              WHERE c.partner_id = p.partner_id
                                AND c.loai_hinh_vay IN ('V. MUC DICH KHAC')
                                AND c.product_code NOT IN ('VPN1', 'VPT1', 'VPD1', 'VPN2', 'VPT2', 'VPD2', 'VPN3',
                                                           'VPT3', 'VPD3',
                                                           'EDNK', 'EDTK', ' EDDK', ' EDNH', 'EDTH', 'EDDH')
                                AND c.muc_dich_vay <> ('1. TIN CHAP-H.TOAN=LUONG')
                                AND p.type_id = 0)
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract
        SET contract_type_code = 'MORTAGED_VALUEABLE_PAPER_LOAN'
        WHERE contract_no IN (SELECT c.account_number
                              FROM crmstaging.cltb_account_master@"Dbstaging580.Localdomain" c
                              WHERE c.product_code IN ('S50A', 'M50A', 'S51A', 'M51A')
                                AND c.field_char_2 IN ('XII.CK.1.CO KY HAN GTCG, NHOM I',
                                                       'XII.CK.2.CO KY HAN GTCG, NHOM II',
                                                       'XII.CK.3.CBL QUYEN TD GTCG, NHOM I',
                                                       'XII.CK.4.CBL QUYEN TD GTCG, NHOM II',
                                                       'IV.1.CHIET KHAU GTCG-CTCK')
                                AND c.product_code IN ('S50A', 'M50A', 'S51A', 'M51A', 'S00A'))
          AND contract_type_id = 'LOAN';

        UPDATE crm_partner_contract c
        SET c.contract_type_code = (SELECT type
                                    FROM crm_partner_bank_account_overdraft ovd
                                    WHERE c.contract_no = ovd.account_no
                                      AND rownum = 1)
        WHERE c.contract_type_id = 'OVERDRAFT';

        INSERT
        INTO etl_log (log_date, table_name, row_affected)
        VALUES (SYSDATE, 'CRM_PARTNER_CONTRACT', row_count);
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_crm_partner_contract_daily', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;

    END proc_crm_partner_contract_daily;

    PROCEDURE
        proc_crm_partner_contract_assign_daily AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        --TODO: Insert CRM_PARTNER_CONTRACT_ASSIGN
        BEGIN

            MERGE
            INTO crm_partner_contract_assign assign
            USING
                (WITH loan_contract AS (SELECT contract_id, contract_no, field_char_1, maker_dt_stamp
                                        FROM crm_partner_contract a
                                                 JOIN crmstaging.cltb_account_master@"Dbstaging580.Localdomain" b
                                                      ON a.contract_no = b.account_number
                                        WHERE contract_type_id = 'LOAN'
                                          AND TRUNC(created_date) = TRUNC(SYSDATE))
                    , casa_contract AS (SELECT contract_id, contract_no, maker_id, ac_open_date
                                        FROM crm_partner_contract a
                                                 JOIN crmstaging.sttm_cust_account@"Dbstaging580.Localdomain" b
                                                      ON a.contract_no = b.cust_ac_no
                                        WHERE account_type = 'U'
                                          AND contract_type_id = 'CASA'
                                          AND TRUNC(created_date) = TRUNC(SYSDATE))
                    , overdraft_cust_contract AS (SELECT contract_id, contract_no, maker_id, ac_open_date
                                                  FROM crm_partner_contract a
                                                           JOIN crmstaging.sttm_cust_account@"Dbstaging580.Localdomain" b
                                                                ON a.contract_no = b.cust_ac_no
                                                  WHERE contract_type_id = 'OVERDRAFT'
                                                    AND TRUNC(created_date) = TRUNC(SYSDATE))
                    , overdraft_account_contract AS (SELECT contract_id, contract_no, field_char_1, maker_dt_stamp
                                                     FROM crm_partner_contract a
                                                              JOIN crmstaging.cltb_account_master@"Dbstaging580.Localdomain" b
                                                                   ON a.contract_no = b.account_number
                                                     WHERE contract_type_id = 'OVERDRAFT'
                                                       AND TRUNC(created_date) = TRUNC(SYSDATE))
                    , banking_contract AS (SELECT a.contract_id, a.contract_no, b.created_by, a.open_date
                                           FROM crm_partner_contract a
                                                    JOIN crm_partner_bank_account_banking b
                                                         ON a.contract_no = b.contract_no
                                           WHERE contract_type_id = 'I_BANKING')
                    , contract_assign AS (SELECT contract_id
                                               , CASE
                                                     WHEN field_char_1 IS NOT NULL THEN (SELECT user_id
                                                                                         FROM crm_auth_user
                                                                                         WHERE code = loan_contract.field_char_1
                                                                                           AND rownum = 1) END user_id
                                               , maker_dt_stamp AS                                             start_date
                                               , contract_no
                                               , 'CREDIT'       AS                                             kpi_type_id
                                          FROM loan_contract
                                          UNION
                                          SELECT contract_id
                                               , CASE
                                                     WHEN maker_id IS NOT NULL THEN (SELECT user_id
                                                                                     FROM crm_auth_user
                                                                                     WHERE code = casa_contract.maker_id) END user_id
                                               , ac_open_date AS                                                              start_date
                                               , contract_no
                                               , 'DEPOSIT'    AS                                                              kpi_type_id
                                          FROM casa_contract
                                          UNION
                                          SELECT contract_id
                                               , CASE
                                                     WHEN maker_id IS NOT NULL THEN (SELECT user_id
                                                                                     FROM crm_auth_user
                                                                                     WHERE code = overdraft_cust_contract.maker_id) END user_id
                                               , ac_open_date AS                                                                        start_date
                                               , contract_no
                                               , 'CREDIT'     AS                                                                        kpi_type_id
                                          FROM overdraft_cust_contract
                                          UNION
                                          SELECT contract_id
                                               , CASE
                                                     WHEN field_char_1 IS NOT NULL THEN (SELECT user_id
                                                                                         FROM crm_auth_user
                                                                                         WHERE code = overdraft_account_contract.field_char_1
                                                                                           AND rownum = 1) END user_id
                                               , maker_dt_stamp AS                                             start_date
                                               , contract_no
                                               , 'CREDIT'       AS                                             kpi_type_id
                                          FROM overdraft_account_contract
                                          UNION
                                          SELECT contract_id
                                               , CASE
                                                     WHEN created_by IS NOT NULL THEN (SELECT user_id
                                                                                       FROM crm_auth_user
                                                                                       WHERE code = banking_contract.created_by
                                                                                         AND rownum = 1) END user_id
                                               , open_date AS                                                start_date
                                               , contract_no
                                               , 'SLKH'    AS                                                kpi_type_id
                                          FROM banking_contract)
                 SELECT contract_id AS contract_id
                      , user_id     AS user_id
                      , kpi_type_id AS kpi_type_id
                      , 100         AS density
                      , start_date  AS start_date
                      , NULL        AS end_date
                      , contract_no AS contract_no
                 FROM contract_assign
                 WHERE user_id IS NOT NULL) daily_contract_assign
            ON (assign.contract_no || assign.kpi_type_id =
                daily_contract_assign.contract_no || daily_contract_assign.kpi_type_id)
            WHEN NOT MATCHED THEN
                INSERT ( assign_id, contract_id, user_id, kpi_type_id, density, start_date
                       , end_date, contract_no, source)
                VALUES ( crm_partner_contract_assign_seq.nextval
                       , daily_contract_assign.contract_id
                       , daily_contract_assign.user_id
                       , daily_contract_assign.kpi_type_id
                       , daily_contract_assign.density
                       , daily_contract_assign.start_date
                       , daily_contract_assign.end_date
                       , daily_contract_assign.contract_no
                       , 'CORE');
            row_count := SQL%ROWCOUNT;

            INSERT
            INTO etl_log (log_date, table_name, row_affected)
            VALUES (SYSDATE, 'CRM_PARTNER_CONTRACT_ASSIGN', row_count);
            COMMIT;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                error_code := SQLCODE;
                error_message := SQLERRM;
                INSERT
                INTO error_log (error_proc, error_code, error_message, error_line, error_date)
                VALUES ( 'proc_crm_partner_contract_assign_daily', error_code, error_message
                       , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
                COMMIT;
        END;
    END proc_crm_partner_contract_assign_daily;

    PROCEDURE
        proc_crm_boundary_address_daily AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN

        --Sync CRM_PARTNER_ADDRESS
        MERGE
        INTO crm_partner_address cpa
        USING
            (SELECT p.partner_id                                                  AS partner_id
                  , c.address_line1 || ' ' || c.address_line2 || ' ' ||
                    DECODE(c.address_line3, '', c.address_line3, c.address_line4) AS address
                  , 'Ðịa Chỉ Khách Hàng'                                          AS type
                  , 'CORE'                                                        AS source
                  , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))                       AS created_date
                  , p.cif_no                                                      AS cif_no
             FROM vw_customer c
                , crm_partner p
             WHERE c.cif_no = p.cif_no
               AND TRUNC(c.checker_dt_stamp) = TRUNC(SYSDATE - 1)) daily_cpa
        ON (cpa.partner_id = daily_cpa.partner_id)
        WHEN MATCHED THEN
            UPDATE
            SET type         = daily_cpa.type
              , address      = daily_cpa.address
              , source       = daily_cpa.source
              , created_date = daily_cpa.created_date
        WHEN NOT MATCHED THEN
            INSERT (partner_address_id, partner_id, type, address, source, created_date, cif_no)
            VALUES ( crm_partner_address_seq.nextval
                   , daily_cpa.partner_id
                   , daily_cpa.type
                   , daily_cpa.address
                   , daily_cpa.source
                   , daily_cpa.created_date
                   , daily_cpa.cif_no);
        row_count := SQL%ROWCOUNT;

        INSERT
        INTO etl_log (log_date, table_name, row_affected)
        VALUES (SYSDATE, 'CRM_PARTNER_ADDRESS', row_count);


        --Sync CRM_BOUNDARY_ADDRESS
        MERGE
        INTO crm_boundary_address cba
        USING
            (SELECT b.field_val_3                           AS name
                  , 'CORE'                                  AS source
                  , CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS created_date
                  , lmtm.collateral_code                    AS collateral_code
                  , p.cif_no                                AS cif_no
             FROM crmstaging.lmtm_collat@"Dbstaging580.Localdomain" lmtm
                , crmstaging.cstm_function_userdef_fields@"Dbstaging580.Localdomain" b
                , crm_partner p
             WHERE lmtm.collateral_code || '~' || lmtm.liab_id || '~' = b.rec_key
               AND function_id = 'LMDCOLLT'
               AND p.cif_no = lmtm.liab_id
               AND b.field_val_3 IS NOT NULL
               AND TRUNC(lmtm.checker_dt_stamp) = TRUNC(SYSDATE - 1)) daily_cba
        ON (cba.collateral_code = daily_cba.collateral_code)
        WHEN MATCHED THEN
            UPDATE
            SET name         = daily_cba.name
              , source       = daily_cba.source
              , created_date = daily_cba.created_date
              , cif_no       = daily_cba.cif_no
        WHEN NOT MATCHED THEN
            INSERT ( address_id, name, source, created_date, collateral_code
                   , cif_no)
            VALUES ( crm_boundary_address_seq.nextval
                   , daily_cba.name
                   , daily_cba.source
                   , daily_cba.created_date
                   , daily_cba.collateral_code
                   , daily_cba.cif_no);
        row_count := SQL%ROWCOUNT;

        INSERT
        INTO etl_log (log_date, table_name, row_affected)
        VALUES (SYSDATE, 'CRM_BOUNDARY_ADDRESS', row_count);
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_crm_boundary_address_daily', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;

    END proc_crm_boundary_address_daily;

    PROCEDURE
        proc_crm_partner_daily AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN

        --Insert PROC_CRM_PARTNER
        --TODO: Update CIF when CIF is NULL (mapping with Identity Number)
        BEGIN
            MERGE
            INTO crm_partner a
            USING (SELECT c.local_branch
                        , '1'
                        , c.full_name
                        , c.cif_no
                        , c.id_type_crm
                        , c.id_number
                        , c.id_type_issue_date
                        , c.id_type_issue_place
                        , c.is_active
                        , c.gender
                        , c.date_of_birth
                        , c.nationality
                        , c.marital_status
                        , c.credit_rate
                        , c.mobile_phone
                        , c.fixed_phone
                        , c.email_address
                        , c.fax
                        , c.capital
                        , c.registration_code
                        , c.tax_number
                        , c.established_date
                   FROM vw_customer c) c
            ON (REPLACE(a.id_number, ' ', '') = REPLACE(c.id_number, ' ', ''))
            WHEN MATCHED THEN
                UPDATE
                SET branch_code         = c.local_branch
                  , customer_type       = '1'
                  , full_name           = c.full_name
                  , cif_no              = c.cif_no
                  , id_type             = c.id_type_crm
                  , id_type_issue_date  = c.id_type_issue_date
                  , id_type_issue_place = c.id_type_issue_place
                  , is_active           = c.is_active
                  , gender              = c.gender
                  , date_of_birth       = c.date_of_birth
                  , nationality         = c.nationality
                  , marital_status      = c.marital_status
                  , credit_rate         = c.credit_rate
                  , mobile_phone        = c.mobile_phone
                  , fixed_phone         = c.fixed_phone
                  , email_address       = c.email_address
                  , fax                 = c.fax
                  , capital             = c.capital
                  , registration_code   = c.registration_code
                  , tax_number          = c.tax_number
                  , established_date    = c.established_date
                WHERE a.cif_no IS NULL;
            row_count := SQL%ROWCOUNT;
        END;

        --TODO: Update CIF when CIF is NULL (mapping with Registration Code)
        BEGIN

            MERGE
            INTO crm_partner a
            USING (SELECT c.local_branch
                        , '1'
                        , c.full_name
                        , c.cif_no
                        , c.id_type_crm
                        , c.id_number
                        , c.id_type_issue_date
                        , c.id_type_issue_place
                        , c.is_active
                        , c.gender
                        , c.date_of_birth
                        , c.nationality
                        , c.marital_status
                        , c.credit_rate
                        , c.mobile_phone
                        , c.fixed_phone
                        , c.email_address
                        , c.fax
                        , c.capital
                        , c.registration_code
                        , c.tax_number
                        , c.established_date
                   FROM vw_customer c) c
            ON (REPLACE(a.registration_code, ' ', '') = REPLACE(c.registration_code, ' ', ''))
            WHEN MATCHED THEN
                UPDATE
                SET branch_code         = c.local_branch
                  , customer_type       = '1'
                  , full_name           = c.full_name
                  , cif_no              = c.cif_no
                  , id_type             = c.id_type_crm
                  , id_number           =c.id_number
                  , id_type_issue_date  = c.id_type_issue_date
                  , id_type_issue_place = c.id_type_issue_place
                  , is_active           = c.is_active
                  , gender              = c.gender
                  , date_of_birth       = c.date_of_birth
                  , nationality         = c.nationality
                  , marital_status      = c.marital_status
                  , credit_rate         = c.credit_rate
                  , mobile_phone        = c.mobile_phone
                  , fixed_phone         = c.fixed_phone
                  , email_address       = c.email_address
                  , fax                 = c.fax
                  , capital             = c.capital
                  , tax_number          = c.tax_number
                  , established_date    = c.established_date
                WHERE a.cif_no IS NULL;
            row_count := row_count + SQL%ROWCOUNT;
        END;

        --TODO: Update CIF when CIF is NULL (mapping with Mobile Phone)
        BEGIN
            MERGE
            INTO crm_partner a
            USING (SELECT c.local_branch
                        , '1'
                        , c.full_name
                        , c.cif_no
                        , c.id_type_crm
                        , c.id_number
                        , c.id_type_issue_date
                        , c.id_type_issue_place
                        , c.is_active
                        , c.gender
                        , c.date_of_birth
                        , c.nationality
                        , c.marital_status
                        , c.credit_rate
                        , c.mobile_phone
                        , c.fixed_phone
                        , c.email_address
                        , c.fax
                        , c.capital
                        , c.registration_code
                        , c.tax_number
                        , c.established_date
                   FROM vw_customer c) c
            ON (REPLACE(a.mobile_phone, ' ', '') = REPLACE(c.mobile_phone, ' ', ''))
            WHEN MATCHED THEN
                UPDATE
                SET branch_code         = c.local_branch
                  , customer_type       = '1'
                  , full_name           = c.full_name
                  , cif_no              = c.cif_no
                  , id_type             = c.id_type_crm
                  , id_number           =c.id_number
                  , id_type_issue_date  = c.id_type_issue_date
                  , id_type_issue_place = c.id_type_issue_place
                  , is_active           = c.is_active
                  , gender              = c.gender
                  , date_of_birth       = c.date_of_birth
                  , nationality         = c.nationality
                  , marital_status      = c.marital_status
                  , credit_rate         = c.credit_rate
                  , fixed_phone         = c.fixed_phone
                  , email_address       = c.email_address
                  , fax                 = c.fax
                  , capital             = c.capital
                  , registration_code   = c.registration_code
                  , tax_number          = c.tax_number
                  , established_date    = c.established_date
                WHERE a.cif_no IS NULL;
            row_count := row_count + SQL%ROWCOUNT;
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        --TODO: Update Identity Number (CMND/CCCD/Passport/DKKD...) when CIF is NOT NULL
        BEGIN
            MERGE
            INTO crm_partner a
            USING (SELECT id_number, cif_no, registration_code, local_branch, full_name, mobile_phone, e_mail, tax_number
                   FROM vw_customer c
                   WHERE TRUNC(checker_dt_stamp) >= TRUNC(SYSDATE - 3)) b
            ON (REPLACE(a.cif_no, ' ', '') = REPLACE(b.cif_no, ' ', ''))
            WHEN MATCHED THEN
                UPDATE
                SET a.id_number         = b.id_number
                  , a.registration_code = b.registration_code
                  , a.full_name         = b.full_name
                  , a.mobile_phone      = b.mobile_phone
                  , a.tax_number        = b.tax_number
                  , a.email_address     = b.e_mail
                  , a.branch_code       = b.local_branch
                WHERE a.cif_no IS NOT NULL;
            row_count := row_count + SQL%ROWCOUNT;
        END;

        --TODO: Insert new Corporate Customers from Core
        BEGIN
            INSERT
            INTO crm_partner ( partner_id, type_id, type_desc, customer_type, assigned_user_id, avatar_id
                             , contract_id
                             , source_id, ceo_name, full_name, cif_no, gender, date_of_birth, id_type, id_number
                             , id_type_issue_date, id_type_issue_place, mobile_phone, fixed_phone, email_address
                             , web_site, fax, capital
                --, total_asset
                             , established_date, registration_code, tax_number
                             , nationality, marital_status, education, occupation, number_of_dependent
                             , sale_turnover, number_of_employee, credit_rate, is_active, created_date,
                --POSITION_ID,
                               holding_company_id,
                --currency_id,
                               currency, business_type_id, branch_code)
            WITH data AS (SELECT DISTINCT c.type_id_crm         AS type_id
                                        , c.type_id             AS type_desc
                                        --c.CUSTOMER_TYPE_CRM   As CUSTOMER_TYPE,
                                        , 1                     AS customer_type
                                        , NULL                  AS assigned_user_id
                                        , NULL                  AS avatar_id
                                        , NULL                  AS contract
                                        --c.SOURCE_ID           As SOURCE_ID,
                                        , NULL                  AS source_id
                                        , NULL                  AS ceo_name
                                        , c.full_name           AS full_name
                                        , c.cif_no              AS cif_no
                                        , c.gender              AS gender
                                        , c.date_of_birth       AS date_of_birth
                                        , c.id_type_crm         AS id_type_crm
                                        , c.id_number           AS id_number
                                        , c.id_type_issue_date  AS id_type_issue_date
                                        , c.id_type_issue_place AS id_type_issue_place
                                        , c.mobile_phone        AS mobile_phone
--                                         , '0999999999'           AS mobile_phone
                                        , c.fixed_phone         AS fixed_phone
                                        , c.e_mail              AS email_address
--                                         , 'nguyenvana@gmail.com' AS email_address
                                        , NULL                  AS web_site
                                        , c.e_fax               AS fax
                                        , c.capital             AS capital
                                        --, c.total_asset         AS total_asset
                                        , c.established_date    AS established_date
                                        , c.registration_code   AS registration_code
                                        , c.tax_number          AS tax_number
                                        , c.nationality         AS nationality
                                        , c.marital_status      AS marital_status
                                        , NULL                  AS education
                                        , NULL                  AS occupation
                                        , c.number_of_dependent AS number_of_dependent
                                        , c.sale_turnover       AS sale_turnover
                                        , c.number_of_employee  AS number_of_employee
                                        , c.credit_rate         AS credit_rate
                                        , c.is_active           AS is_active
                                        , c.cif_creation_date   AS created_date
                                        --c.POSITION_ID         As POSITION_ID,
                                        , c.holding_company_id  AS holding_company_id
--                                             , c.currency_id         AS currency_id
                                        , c.currency            AS currency
                                        , c.business_type_id    AS business_type_id
                                        , c.local_branch        AS branch_code
                          FROM vw_customer c
                          WHERE c.type_id = 'C'
                            AND c.cif_creation_date IS NOT NULL
--                                 AND c.currency_id IS NOT NULL
                            AND NOT EXISTS(SELECT NULL
                                           FROM crm_partner b
                                           WHERE REPLACE(c.cif_no, ' ', '') = REPLACE(b.cif_no, ' ', ''))
                            AND NOT EXISTS(SELECT NULL
                                           FROM crm_partner b
                                           WHERE REPLACE(c.id_number, ' ', '') = REPLACE(b.id_number, ' ', ''))
                --and ROWNUM = 1
            )
            SELECT crm_partner_seq.nextval
                 , type_id
                 , type_desc
                 , customer_type
                 , assigned_user_id
                 , avatar_id
                 , contract
                 , source_id
                 , ceo_name
                 , full_name
                 , cif_no
                 , gender
                 , date_of_birth
                 , id_type_crm
                 , id_number
                 , id_type_issue_date
                 , id_type_issue_place
                 , mobile_phone
                 , fixed_phone
                 , email_address
                 , web_site
                 , fax
                 , capital
                 --, total_asset
                 , established_date
                 , registration_code
                 , tax_number
                 , nationality
                 , marital_status
                 , education
                 , occupation
                 , number_of_dependent
                 , sale_turnover
                 , number_of_employee
                 , credit_rate
                 , is_active
                 , created_date
                 --POSITION_ID,
                 , holding_company_id
--                      , currency_id
                 , currency
                 , business_type_id
                 , branch_code
            FROM data;
            row_count := row_count + SQL%ROWCOUNT;
        END;

        --TODO: Insert new Individual Customers from Core
        BEGIN
            INSERT
            INTO crm_partner ( partner_id, type_id, type_desc, customer_type, assigned_user_id, avatar_id
                             , contract_id
                             , source_id, ceo_name, full_name, cif_no, gender, date_of_birth, id_type, id_number
                             , id_type_issue_date, id_type_issue_place, mobile_phone, fixed_phone, email_address
                             , web_site, fax, capital
                --, total_asset
                             , established_date, registration_code, tax_number
                             , nationality, marital_status, education, occupation, number_of_dependent
                             , sale_turnover, number_of_employee, credit_rate, is_active, created_date,
                --POSITION_ID,
                               holding_company_id,
--                                   currency_id,
                               currency, business_type_id
                             , branch_code)
            WITH data AS (SELECT DISTINCT c.type_id_crm         AS type_id
                                        , c.type_id             AS type_desc
                                        --c.CUSTOMER_TYPE_CRM   AS CUSTOMER_TYPE,
                                        , 1                     AS customer_type
                                        , NULL                  AS assigned_user_id
                                        , NULL                  AS avatar_id
                                        , NULL                  AS contract
                                        --c.SOURCE_ID           As SOURCE_ID,
                                        , NULL                  AS source_id
                                        , NULL                  AS ceo_name
                                        , c.full_name           AS full_name
                                        , c.cif_no              AS cif_no
                                        , c.gender              AS gender
                                        , c.date_of_birth       AS date_of_birth
                                        , c.id_type_crm         AS id_type_crm
                                        , c.id_number           AS id_number
                                        , c.id_type_issue_date  AS id_type_issue_date
                                        , c.id_type_issue_place AS id_type_issue_place
                                        , c.mobile_phone        AS mobile_phone
--                                         , '0999999999'           AS mobile_phone
                                        , c.fixed_phone         AS fixed_phone
                                        , c.e_mail              AS email_address
--                                         , 'nguyenvana@gmail.com' AS email_address
                                        , NULL                  AS web_site
                                        , c.e_fax               AS fax
                                        , c.capital             AS capital
                                        --, c.total_asset         AS total_asset
                                        , c.established_date    AS established_date
                                        , c.registration_code   AS registration_code
                                        , c.tax_number          AS tax_number
                                        , c.nationality         AS nationality
                                        , c.marital_status      AS marital_status
                                        , NULL                  AS education
                                        , NULL                  AS occupation
                                        , c.number_of_dependent AS number_of_dependent
                                        , c.sale_turnover       AS sale_turnover
                                        , c.number_of_employee  AS number_of_employee
                                        , c.credit_rate         AS credit_rate
                                        , c.is_active           AS is_active
                                        , c.cif_creation_date   AS created_date
                                        --c.POSITION_ID         As POSITION_ID,
                                        , c.holding_company_id  AS holding_company_id
--                                             , c.currency_id         AS currency_id
                                        , c.currency            AS currency
                                        , c.business_type_id    AS business_type_id
                                        , c.local_branch        AS branch_code
                          FROM vw_customer c
                          WHERE c.type_id = 'I'
                            AND c.cif_creation_date IS NOT NULL
--                                 AND c.currency_id IS NOT NULL
                            AND NOT EXISTS(SELECT NULL
                                           FROM crm_partner b
                                           WHERE REPLACE(c.cif_no, ' ', '') = REPLACE(b.cif_no, ' ', ''))
                            AND NOT EXISTS(SELECT NULL
                                           FROM crm_partner b
                                           WHERE REPLACE(c.id_number, ' ', '') = REPLACE(b.id_number, ' ', ''))
                --and ROWNUM = 1
            )
            SELECT crm_partner_seq.nextval
                 , type_id
                 , type_desc
                 , customer_type
                 , assigned_user_id
                 , avatar_id
                 , contract
                 , source_id
                 , ceo_name
                 , full_name
                 , cif_no
                 , gender
                 , date_of_birth
                 , id_type_crm
                 , id_number
                 , id_type_issue_date
                 , id_type_issue_place
                 , mobile_phone
                 , fixed_phone
                 , email_address
                 , web_site
                 , fax
                 , capital
                 --, total_asset
                 , established_date
                 , registration_code
                 , tax_number
                 , nationality
                 , marital_status
                 , education
                 , occupation
                 , number_of_dependent
                 , sale_turnover
                 , number_of_employee
                 , credit_rate
                 , is_active
                 , created_date
                 --POSITION_ID,
                 , holding_company_id
--                      , currency_id
                 , currency
                 , business_type_id
                 , branch_code
            FROM data;
            row_count := row_count + SQL%ROWCOUNT;

            INSERT
            INTO etl_log (log_date, table_name, row_affected)
            VALUES (SYSDATE, 'CRM_PARTNER', row_count);
            COMMIT;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_crm_partner_daily', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace)
                   , SYSDATE);
            COMMIT;
    END proc_crm_partner_daily;
END;