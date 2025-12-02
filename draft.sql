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