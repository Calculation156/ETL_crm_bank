CREATE OR REPLACE PACKAGE BODY package_etl_bo_sync_job IS
    error_code VARCHAR2(10);
    error_message VARCHAR2(512);
    row_count NUMBER;
    start_time TIMESTAMP;
    end_time TIMESTAMP;

    PROCEDURE sync_bo
    AS
    BEGIN
        package_etl_bo_sync_job.sync_cl_table;
        package_etl_bo_sync_job.sync_deposit_table;
        package_etl_bo_sync_job.sync_tf_lc_detail_table;
        package_etl_bo_sync_job.sync_tf_lc_table;
        package_etl_bo_sync_job.sync_tf_bg_table;
        package_etl_bo_sync_job.sync_thau_chi_table;
        package_etl_bo_sync_job.insert_thau_chi_co_tsdb;
    END sync_bo;

    PROCEDURE sync_cl_table AS
    BEGIN
        start_time := SYSDATE;
        INSERT
        INTO bo_rptw_cl_new ( id, backup_date, product_code, customer_id, account_number, branch_code, value_date
                        , maturity_date
                        , user_defined_status, account_status, dsbr_amount_lcy, outstanding, outstanding_lcy
                        , principal_paid_lcy, rate
                        , tenor, currency
                        , latest_update)
        SELECT seq_bo_rptw_cl_new.nextval
             , a.backup_date
             , a.product_code
             , a.customer_id
             , a.account_number
             , a.branch_code
             , a.value_date
             , a.maturity_date
             , a.user_defined_status
             , a.account_status
             , a.dsbr_amount_lcy
             , a.outstanding
             , a.outstanding_lcy
             , a.principal_paid_lcy
             , a.rate
             , a.tenor
             , a.currency
             , SYSDATE
        FROM (SELECT backup_date
                   , product_code
                   , customer_id
                   , account_number
                   , branch_code
                   , value_date
                   , maturity_date
                   , user_defined_status
                   , account_status
                   , dsbr_amount_lcy
                   , outstanding
                   , outstanding_lcy
                   , principal_paid_lcy
                   , rate
                   , tenor
                   , currency
              FROM flexbo.rpvw_cl_detail_daily@"Report.Localdomain"
              WHERE TRUNC(backup_date) = TRUNC(SYSDATE - 1)
              UNION ALL
              SELECT backup_date
                   , product_code
                   , customer_id
                   , account_number
                   , branch_code
                   , value_date
                   , maturity_date
                   , user_defined_status
                   , account_status
                   , dsbr_amount_lcy
                   , outstanding
                   , outstanding_lcy
                   , principal_paid_lcy
                   , rate
                   , tenor
                   , currency
              FROM flexbo.rpvw_cl_detail_today@"Report.Localdomain"
              WHERE TRUNC(backup_date) = TRUNC(SYSDATE - 1)) a;
        row_count := SQL%ROWCOUNT;

        MERGE
        INTO bo_rptw_cl_new t1
        USING (SELECT customer_no, customer_type FROM sttm_customer) t2
        ON (t1.customer_id = t2.customer_no)
        WHEN MATCHED THEN
            UPDATE
            SET t1.customer_type = t2.customer_type
            WHERE TRUNC(latest_update) = TRUNC(SYSDATE);

        MERGE
        INTO bo_rptw_cl_new t1
        USING (SELECT account_number, field_char_15, field_char_4, field_char_2, original_st_date
               FROM cltb_account_apps_master) t2
        ON (t1.account_number = t2.account_number)
        WHEN MATCHED THEN
            UPDATE
            SET t1.field_char_15       = t2.field_char_15
              , t1.field_char_4        = t2.field_char_4
              , t1.field_char_2        = t2.field_char_2
              , t1.original_start_date = t2.original_st_date
            WHERE TRUNC(latest_update) >= TRUNC(SYSDATE - 5);

        IF (row_count > 0)
        THEN
            BEGIN
                INSERT
                INTO etl_log (log_date, table_name, source, row_affected, status, action)
                VALUES (SYSDATE, 'bo_rptw_cl_new', 'BO', row_count, 'SUCCESS', 'SYNC');
            END;
        ELSE
            BEGIN
                BEGIN
                    INSERT
                    INTO etl_log (log_date, table_name, source, row_affected, status, action)
                    VALUES (SYSDATE, 'bo_rptw_cl_new', 'BO', row_count, 'FAILED', 'SYNC');
                END;
            END;
        END IF;
        end_time := SYSDATE;

        INSERT
        INTO bo_log (proc_name, time_running, row_count_staging, log_date)
        VALUES ('proc_sync_cl_table', end_time - start_time, row_count, SYSDATE);


    EXCEPTION
        WHEN
            OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_sync_cl_table', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;

    END;

    PROCEDURE sync_deposit_table AS
    BEGIN
        start_time := SYSDATE;
        INSERT
        INTO bo_rptw_deposit_new
        ( id, branch_code, cust_ac_no, cust_no, ccy, account_class, ac_open_date, lcy_curr_balance, account_type, tenor
        , rate
        , backup_date, acc_status, record_stat, latest_update)
        SELECT seq_bo_rptw_deposit_new.nextval
             , branch_code
             , cust_ac_no
             , cust_no
             , ccy
             , account_class
             , ac_open_date
             , lcy_curr_balance
             , account_type
             , tenor
             , rate
             , backup_date
             , acc_status
             , record_stat
             , SYSDATE
        FROM flexbo.rptb_deposit_today@"Report.Localdomain"
        WHERE TRUNC(backup_date) = TRUNC(SYSDATE - 1);
        row_count := SQL%ROWCOUNT;

        MERGE
        INTO bo_rptw_deposit_new t1
        USING (SELECT customer_no, customer_type FROM sttm_customer) t2
        ON (t1.cust_no = t2.customer_no)
        WHEN MATCHED THEN
            UPDATE
            SET t1.customer_type = t2.customer_type
            WHERE TRUNC(latest_update) = TRUNC(SYSDATE);

        IF (row_count > 0)
        THEN
            BEGIN
                INSERT
                INTO etl_log (log_date, table_name, source, row_affected, status, action)
                VALUES (SYSDATE, 'bo_rptw_deposit_new', 'BO', row_count, 'SUCCESS', 'SYNC');
            END;
        ELSE
            BEGIN
                BEGIN
                    INSERT
                    INTO etl_log (log_date, table_name, source, row_affected, status, action)
                    VALUES (SYSDATE, 'bo_rptw_deposit_new', 'BO', row_count, 'FAILED', 'SYNC');
                END;
            END;
        END IF;
        end_time := SYSDATE;

        INSERT
        INTO bo_log (proc_name, time_running, row_count_staging, log_date)
        VALUES ('proc_sync_deposit_table', end_time - start_time, row_count, SYSDATE);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_sync_deposit_table', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;
    END;

    PROCEDURE sync_tf_lc_detail_table AS
    BEGIN
        start_time := SYSDATE;
        INSERT
        INTO bo_rptw_tf_lc_detail
        ( backup_date, branch, contract_ref_no, user_ref_no, version_no, event_seq_no, event_code, ext_ref_no
        , related_lc_ref_no, product_code, settlement_type, settlement_method, contract_ccy, contract_amt
        , max_contract_amt, max_liability_amt, expiry_date, issue_date, closure_date, cif_id, cust_type, cust_name
        , cust_ref_no, credit_line, remarks, effective_date, tenor, contract_status, product_group, sale_rate_vnd
        , sale_rate_usd, collat_amount_max, collat_amount_remain, outstanding_lc, customer_infor, dr_cont_liab
        , cr_cont_liab, remain_cont_liab, product_type, cust_ac_no, customer_type, date_send, customer_ben, auth_status
        , user_defined_status, outstanding_liability, latest_update)
        SELECT backup_date
             , branch
             , contract_ref_no
             , user_ref_no
             , version_no
             , event_seq_no
             , event_code
             , ext_ref_no
             , related_lc_ref_no
             , product_code
             , settlement_type
             , settlement_method
             , contract_ccy
             , contract_amt
             , max_contract_amt
             , max_liability_amt
             , expiry_date
             , issue_date
             , closure_date
             , cif_id
             , cust_type
             , cust_name
             , cust_ref_no
             , credit_line
             , remarks
             , effective_date
             , tenor
             , contract_status
             , product_group
             , sale_rate_vnd
             , sale_rate_usd
             , collat_amount_max
             , collat_amount_remain
             , outstanding_lc
             , customer_infor
             , dr_cont_liab
             , cr_cont_liab
             , remain_cont_liab
             , product_type
             , cust_ac_no
             , customer_type
             , date_send
             , customer_ben
             , auth_status
             , user_defined_status
             , outstanding_liability
             , backup_date + 1
        FROM rptw_tf_lc_detail@"Report.Localdomain"
        WHERE TRUNC(backup_date) = TRUNC(SYSDATE - 1);
        row_count := SQL%ROWCOUNT;

        IF (row_count > 0)
        THEN
            BEGIN
                INSERT
                INTO etl_log (log_date, table_name, source, row_affected, status, action)
                VALUES (SYSDATE, 'bo_rptw_tf_lc_detail', 'BO', row_count, 'SUCCESS', 'SYNC');
            END;
        ELSE
            BEGIN
                BEGIN
                    INSERT
                    INTO etl_log (log_date, table_name, source, row_affected, status, action)
                    VALUES (SYSDATE, 'bo_rptw_tf_lc_detail', 'BO', row_count, 'FAILED', 'SYNC');
                END;
            END;
        END IF;
        end_time := SYSDATE;

        INSERT
        INTO bo_log (proc_name, time_running, row_count_staging, log_date)
        VALUES ('proc_sync_tf_lc_detail_table', end_time - start_time, row_count, SYSDATE);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_sync_tf_lc_detail_table', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;
    END;

    PROCEDURE sync_tf_lc_table AS
    BEGIN
        BEGIN
            start_time := SYSDATE;
            INSERT
            INTO bo_rptw_tf_lc_new
            ( id, backup_date, branch, contract_ref_no, product_code, issue_date, closure_date, cif_id, cust_type
            , product_type, contract_ccy, contract_status
            , customer_type, user_defined_status, os_liability, latest_update)
            SELECT seq_bo_rptw_tf_lc_new.nextval
                 , a.backup_date
                 , a.branch
                 , a.contract_ref_no
                 , a.product_code
                 , a.issue_date
                 , a.closure_date
                 , a.cif_id
                 , a.cust_type
                 , a.product_type
                 , a.contract_ccy
                 , a.contract_status
                 , a.customer_type
                 , a.user_defined_status
                 , b.outstanding_amt
                 , SYSDATE
            FROM rptb_tf_lc_today@"Report.Localdomain" a
               , (SELECT contract_ref_no, outstanding_amt
                  FROM rptb_rawdata_lc@"Report.Localdomain"
                  WHERE TRUNC(backup_date) = TRUNC(SYSDATE - 1)) b
            WHERE TRUNC(a.backup_date) = TRUNC(SYSDATE - 1)
              AND a.contract_ref_no = b.contract_ref_no;
            row_count := SQL%ROWCOUNT;
        END;

        IF (row_count > 0)
        THEN
            BEGIN
                INSERT
                INTO etl_log (log_date, table_name, source, row_affected, status, action)
                VALUES (SYSDATE, 'bo_rptw_tf_lc_new', 'BO', row_count, 'SUCCESS', 'SYNC');
            END;
        ELSE
            BEGIN
                BEGIN
                    INSERT
                    INTO etl_log (log_date, table_name, source, row_affected, status, action)
                    VALUES (SYSDATE, 'bo_rptw_tf_lc_new', 'BO', row_count, 'FAILED', 'SYNC');
                END;
            END;
        END IF;
        end_time := SYSDATE;

        INSERT
        INTO bo_log (proc_name, time_running, row_count_staging, log_date)
        VALUES ('proc_sync_tf_lc_table', end_time - start_time, row_count, SYSDATE);

    EXCEPTION
        WHEN
            OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_sync_tf_lc_table', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;
    END;

    PROCEDURE sync_tf_bg_table AS
    BEGIN
        BEGIN
            start_time := SYSDATE;
            INSERT
            INTO bo_rptw_tf_bg_new
            ( id, backup_date, branch, contract_ref_no, product_code, issue_date, closure_date, cif_id, cust_type
            , product_type, contract_ccy, contract_status
            , customer_type, user_defined_status, os_liability, latest_update)
            SELECT seq_bo_rptw_tf_bg_new.nextval
                 , a.backup_date
                 , a.branch
                 , a.contract_ref_no
                 , a.product_code
                 , a.issue_date
                 , a.closure_date
                 , a.cif_id
                 , a.cust_type
                 , a.product_type
                 , a.contract_ccy
                 , a.contract_status
                 , a.customer_type
                 , a.user_defined_status
                 , b.outstanding_amt
                 , SYSDATE
            FROM rptb_tf_lc_today@"Report.Localdomain" a
               , (SELECT contract_ref_no, outstanding_amt
                  FROM flexbo.rptb_rawdata_bg@"Report.Localdomain"
                  WHERE TRUNC(backup_date) = TRUNC(SYSDATE - 1)) b
            WHERE TRUNC(a.backup_date) = TRUNC(SYSDATE - 1)
              AND a.contract_ref_no = b.contract_ref_no;
            row_count := SQL%ROWCOUNT;
        END;

        IF (row_count > 0)
        THEN
            BEGIN
                INSERT
                INTO etl_log (log_date, table_name, source, row_affected, status, action)
                VALUES (SYSDATE, 'bo_rptw_tf_bg_new', 'BO', row_count, 'SUCCESS', 'SYNC');
            END;
        ELSE
            BEGIN
                BEGIN
                    INSERT
                    INTO etl_log (log_date, table_name, source, row_affected, status, action)
                    VALUES (SYSDATE, 'bo_rptw_tf_bg_new', 'BO', row_count, 'FAILED', 'SYNC');
                END;
            END;
        END IF;
        end_time := SYSDATE;

        INSERT
        INTO bo_log (proc_name, time_running, row_count_staging, log_date)
        VALUES ('proc_sync_tf_bg_table', end_time - start_time, row_count, SYSDATE);

    EXCEPTION
        WHEN
            OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_sync_tf_bg_table', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;
    END;

    PROCEDURE sync_thau_chi_table IS
    BEGIN
        start_time := SYSDATE;
        INSERT
        INTO bo_rpvw_thau_chi_new
        ( id, branch_code, cust_ac_no, cust_no, lcy_bal, trn_dt, create_date, currency, tsdb, account_class, du_am
        , latest_update, rate)
        SELECT seq_bo_rpvw_thau_chi_new.nextval
             , a.branch_code
             , a.cust_ac_no
             , a.cust_no
             , a.lcy_bal
             , a.trn_dt
             , a.create_date
             , a.currency
             , a.tsdb
             , b.account_class
             , a.du_am
             , SYSDATE
             , b.rate
        FROM thau_chi@"Dbstaging.Localdomain" a
           , (SELECT cust_ac_no, account_class, rate
              FROM rptw_dd_detail@"Report.Localdomain" b
              WHERE backup_date = TRUNC(SYSDATE - 1)) b
        WHERE a.trn_dt = TRUNC(SYSDATE - 1)
          AND a.cust_ac_no = b.cust_ac_no;
        row_count := SQL%ROWCOUNT;

        IF (row_count > 0)
        THEN
            BEGIN
                INSERT
                INTO etl_log (log_date, table_name, source, row_affected, status, action)
                VALUES (SYSDATE, 'bo_rpvw_thau_chi_new', 'BO', row_count, 'SUCCESS', 'SYNC');
            END;
        ELSE
            BEGIN
                BEGIN
                    INSERT
                    INTO etl_log (log_date, table_name, source, row_affected, status, action)
                    VALUES (SYSDATE, 'bo_rpvw_thau_chi_new', 'BO', row_count, 'FAILED', 'SYNC');
                END;
            END;
        END IF;
        end_time := SYSDATE;

        INSERT
        INTO bo_log (proc_name, time_running, row_count_staging, log_date)
        VALUES ('proc_sync_thau_chi_table', end_time - start_time, row_count, SYSDATE);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_sync_thau_chi_table', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;

    END;

    PROCEDURE insert_thau_chi_co_tsdb
    AS
    BEGIN
        start_time := SYSDATE;
        INSERT
        INTO base_thau_chi_co_tsdb
            (cust_ac_no, cust_no, backup_date, lcy_curr_balance, branch_code, rate)
        SELECT aa.cust_ac_no
             , aa.cust_no
             , aa.backup_date
             , cc.lcy_bal AS lcy_curr_balance
             , aa.branch_code
             , (CASE
                    WHEN (SELECT rate
                          FROM icvw_qry t
                          WHERE t.acc = aa.cust_ac_no
                            AND t.ude_id = 'OD_RATE'
                            AND ude_eff_dt =
                                (SELECT MAX(ude_eff_dt)
                                 FROM icvw_qry t
                                 WHERE t.acc = aa.cust_ac_no
                                   AND t.ude_id = 'OD_RATE'
                                   AND TRUNC(t.ude_eff_dt) <= TRUNC(aa.backup_date))
                            AND rownum = 1) = 0 THEN
                        (SELECT CASE
                                    WHEN rate_code IS NULL THEN
                                        ude_val
                                                           ELSE
                                        (ude_val +
                                         (SELECT rate
                                          FROM ictm_rates
                                          WHERE ccy_code = 'VND'
                                            AND rate_code = 'OD_RATE'
                                            AND eff_dt =
                                                (SELECT MAX(eff_dt)
                                                 FROM ictm_rates
                                                 WHERE ccy_code = 'VND'
                                                   AND rate_code = 'OD_RATE'
                                                   AND TRUNC(eff_dt) <= TRUNC(aa.backup_date))))
                                    END AS interest_rate
                         FROM icvw_qry t
                         WHERE t.acc = aa.cust_ac_no
                           AND t.ude_id = 'OD_RATE'
                           AND ude_eff_dt =
                               (SELECT MAX(ude_eff_dt)
                                FROM icvw_qry t
                                WHERE t.acc = aa.cust_ac_no
                                  AND t.ude_id = 'OD_RATE'
                                  AND TRUNC(t.ude_eff_dt) <= TRUNC(aa.backup_date)))
                                                ELSE
                        (SELECT rate + amt
                         FROM icvw_qry t
                         WHERE t.acc = aa.cust_ac_no
                           AND t.ude_id = 'OD_RATE'
                           AND ude_eff_dt =
                               (SELECT MAX(ude_eff_dt)
                                FROM icvw_qry t
                                WHERE t.acc = aa.cust_ac_no
                                  AND t.ude_id = 'OD_RATE'
                                  AND TRUNC(t.ude_eff_dt) <= TRUNC(aa.backup_date))
                           AND rownum = 1)
            END)             rate
        FROM bo_rptw_deposit_new aa
           , bo_rpvw_thau_chi_new cc
           , sttm_cust_account bb
        WHERE aa.account_class = 'OI01'
          AND aa.cust_ac_no = bb.cust_ac_no
          AND aa.cust_ac_no = cc.cust_ac_no
          AND cc.tsdb = 'Y'
          AND aa.backup_date = cc.trn_dt
          AND cc.du_am = 1
          AND TRUNC(aa.backup_date) = TRUNC(SYSDATE - 1);
        row_count := SQL%ROWCOUNT;

        IF (row_count > 0)
        THEN
            BEGIN
                INSERT
                INTO etl_log (log_date, table_name, source, row_affected, status, action)
                VALUES (SYSDATE, 'base_thau_chi_co_tsdb', 'BO', row_count, 'SUCCESS', 'SYNC');
            END;
        ELSE
            BEGIN
                BEGIN
                    INSERT
                    INTO etl_log (log_date, table_name, source, row_affected, status, action)
                    VALUES (SYSDATE, 'base_thau_chi_co_tsdb', 'BO', row_count, 'FAILED', 'SYNC');
                END;
            END;
        END IF;
        end_time := SYSDATE;

        INSERT
        INTO bo_log (proc_name, time_running, row_count_staging, log_date)
        VALUES ('proc_base_thau_chi_co_tsdb', end_time - start_time, row_count, SYSDATE);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_base_thau_chi_co_tsdb', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;
    END;
END package_etl_bo_sync_job;
/

