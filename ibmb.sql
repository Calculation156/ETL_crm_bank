CREATE OR REPLACE VIEW vw_mt_ib_limit_customer AS
select *
from ebanking.bb_account_security@EBANKING.LOCALDOMAIN a, ebanking.bb_corp_info@EBANKING.LOCALDOMAIN b
where a.corp_id = b.corp_id;

CREATE OR REPLACE VIEW vw_mt_ib_limit_customer AS
Select "CIF","TRANS_TYPE","USERNAME","USER_CURRENCY","USER_LIMIT_AMT","CREATE_DATE","MODIFY_DATE","USER_EXECUTE" from EBANKING.MT_IB_LIMIT_CUSTOMER@"IBPRO.LOCALDOMAIN"
/


CREATE OR REPLACE VIEW vw_mt_ib_parameter AS
select limit_day
from ebanking.bc_account_security@EBANKING.LOCALDOMAIN where security_id = '12533';
/


UPDATE crm_partner_bank_account_banking a
        SET transaction_limit = (SELECT limit
         FROM (SELECT b.partner_id, SUM(limit_day) limit
               FROM crmstaging.vw_mt_ib_limit_customer@"Dbstaging580.Localdomain" a
                  , crm_partner b
               WHERE a.cif = b.cif_no
               GROUP BY b.partner_id)
         WHERE partner_id = a.partner_id);
/

UPDATE crm_partner_bank_account_banking
        SET transaction_limit = (SELECT limit_day
                                 FROM crmstaging.vw_mt_ib_parameter@"Dbstaging580.Localdomain")
        WHERE transaction_limit IS NULL;
/

with cte as (
select cif_no, acct_no, row_number() over(partition by cif_no order by create_time desc ) ro
from ebanking.bk_account_info@EBANKING.LOCALDOMAIN where IS_DEFAULT='Y')
select cif_no, acct_no from cte where ro = 1;
/

create or replace view vw_bb_user_info_with_cif as
select a.user_id, a.user_name,a.org_id,a.nick,a.status, a.create_time, a.role_id, a.create_by, c.cif_no, a.update_by, c.corp_id
FROM ebanking.bb_user_info@EBANKING.LOCALDOMAIN a
join (select cif_no, corp_id from ebanking.bb_corp_info@EBANKING.LOCALDOMAIN) c
    on a.corp_id = c.corp_id
where ((role_id = 1 and a.user_name like 'U1%') or a.user_name like '%admin');
/

create or replace NONEDITIONABLE PACKAGE BODY package_etl_ib_mb_sync_job_new
AS
    PROCEDURE excute_etl_ib_mb_sync_job(
        t VARCHAR2
    )
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN

        -- SYNC mt_ib_customer TABLE FROM IB_MB
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE mt_ib_customer';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;
        -- trùng cif bb
        INSERT
        INTO mt_ib_customer( cif
                           , username
                           , cust_type
                           , identity_no
                           , email
                           , mobilephone
                           , branch_code
                           , created_date
                           , created_by
                           , modified_date
                           , modified_by
                           , approved_date
                           , approved_by
                           , status
                           , fullname
                           , default_account
                           , save_acc
                           , loan_acc
                           , payment_acc
                           , ib_service
                           , confirm_type
                           , secretkey
                           , counter
                           , id_limit_level
                           , send_otp_time
                           , sms_service
                           , login_failed
                           , last_login
                           , last_ip
                           , role_id
                           , current_login
                           , current_ip
                           , appr_level
                           , lang
                           , date_change_pass
                           , cif_group
                           , etl_status
                           , etl_log_date)
        SELECT a.cif_no
             , a.user_name
             , 'I'
             , NULL
             , NULL
             , NULL
             , a.sign_org
             , a.create_time
             , a.create_by
             , NULL
             , NULL
             , NULL
             , NULL
             , CASE WHEN a.status = 'ACTV' THEN 18
                WHEN a.status = 'DLTD' THEN 16
                WHEN a.status = 'UPNM' THEN 2
                WHEN a.status = 'FRZU' THEN 4
                ELSE 0 END
             , a.nick
             , (select acct_no from vw_bk_account_info b where b.cif_no = a.cif_no)
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , 'INSERT NEW'
             , SYSDATE
        FROM ebanking.bc_user_info@EBANKING.LOCALDOMAIN a
        UNION
        SELECT a.cif_no
             , a.user_name
             , 'C'
             , NULL
             , NULL
             , NULL
             , TO_CHAR(a.org_id)
             , a.create_time
             , a.create_by
             , NULL
             , NULL
             , NULL
             , NULL
             , CASE WHEN a.status = 'ACTV' THEN 18
                WHEN a.status = 'DLTD' THEN 16
                WHEN a.status = 'UPNM' THEN 2
                WHEN a.status = 'FRZU' THEN 4
                ELSE 0 END
             , a.nick
             , (select acct_no from vw_bk_account_info b where b.cif_no = a.cif_no)
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , 'INSERT NEW'
             , SYSDATE
        FROM vw_bb_user_info_with_cif a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM mt_ib_customer;
        IF row_count > 0 THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'IB/MB', 'mt_ib_customer', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'IB/MB', 'mt_ib_customer', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC mt_ib_ecom_register TABLE FROM IB_MB
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE mt_ib_ecom_register';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;
        INSERT
        INTO mt_ib_ecom_register( id, username, cif, pan, limit, currency, cus_service_id, status, user_create
                                , user_update, date_create, date_update, branch_code, etl_status
                                , etl_log_date)
        SELECT seq_mt_ib_ecom_register.nextval
             , b.USER_NAME
             , b.CIF_NO
             , a.card_no
             , a.new_limit
             , 'VND'
             , NULL
             , a.status
             , a.user_id
             , a.update_by
             , a.create_time
             , a.update_time
             , b.sign_org
             , 'INSERT NEW'
             , SYSDATE
        FROM (SELECT *
              FROM ebanking.bc_card_action_history@EBANKING.LOCALDOMAIN
              WHERE ecom = 'Y'
                AND type IN ('UNLOCKPAYMENT', 'LOCKPAYMENT', 'CHANGE_LIMIT')) a
            join (select user_id, USER_NAME,CIF_NO, sign_org  from ebanking.bc_user_info@EBANKING.LOCALDOMAIN) b
            on a.user_id = b.user_id;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM mt_ib_ecom_register;
        IF row_count > 0 THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'IB/MB', 'mt_ib_ecom_register', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'IB/MB', 'mt_ib_ecom_register', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC mt_ib_momo_register TABLE FROM IB_MB
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE mt_ib_momo_register';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;
        INSERT
        INTO mt_ib_momo_register
        ( id, username, cif, pan, limit, currency, cus_service_id, status, user_create, user_update, date_create
        , date_update, branch_code, account, linked, token, etl_status, etl_log_date)
                    SELECT seq_mt_ib_momo_register.nextval
             , b.USER_NAME
             , b.CIF_NO
             , a.pan
             , NULL
             , a.currency
             , NULL
             , a.status
             , a.user_id
             , a.update_by
             , a.create_time
             , a.update_time
             , b.sign_org
             , a.acct_no
             , a.linked
             , a.token
             , 'INSERT NEW'
             , SYSDATE
        FROM (SELECT * FROM ebanking.bc_user_wallet@EBANKING.LOCALDOMAIN WHERE wallet_code = 'MM') a
        join (select user_id, USER_NAME,CIF_NO, sign_org  from ebanking.bc_user_info@EBANKING.LOCALDOMAIN) b
            on a.user_id = b.user_id;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM mt_ib_momo_register;
        IF row_count > 0 THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'IB/MB', 'mt_ib_momo_register', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'IB/MB', 'mt_ib_momo_register', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC mt_ibcn_term_deposit TABLE FROM IB_MB
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE mt_ibcn_term_deposit';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO mt_ibcn_term_deposit
        ( id, tknguon, branch_code, tktietkiemonline, accountclass, kyhan, sotien, ccy, chithidaohan, sodienthoai, tdays
        , tmonths, ngayhieuluc, ngaytattoan, custname, fcc_ref, trn_dt, bank_comment, create_by, create_date
        , appr_open_l1_by, appr_open_l1_date, appr_open_l2_by, appr_open_l2_date, status_open, close_by, close_date
        , appr_close_l1_by, appr_close_l1_date, appr_close_l2_by, appr_close_l2_date, status_close, note, checksum, cif
        , rollover, fcc_red_ref, etl_status, etl_log_date)
        SELECT seq_mt_ibcn_term_deposit.nextval
             , a.account_no
             , SUBSTR(a.account_no, 1, 3)
             , a.receipt_no
             , a.product_code
             , term || 'M'
             , a.principal
             , a.currency_code
             , NULL
             , b.mobile
             , NULL
             , term
             , NULL
             , NULL
             , b.nick
             , NULL
             , NULL
             , NULL
             , a.user_id
             , a.opening_date
             , NULL
             , NULL
             , NULL
             , NULL
             , a.status
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , NULL
             , CASE WHEN status = 'CLOS' THEN 'C' ELSE NULL END
             , a.interest_rate
             , NULL
             , a.cif_no
             , a.is_circular
             , NULL
             , 'INSERT NEW'
             , SYSDATE
        FROM (SELECT * FROM ebanking.bk_receipt_info@EBANKING.LOCALDOMAIN) a
        join (select user_id, mobile, NICK  from ebanking.bc_user_info@EBANKING.LOCALDOMAIN) b
            on a.user_id = b.user_id;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM mt_ibcn_term_deposit;
        IF row_count > 0 THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'IB/MB', 'mt_ibcn_term_deposit', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'IB/MB', 'mt_ibcn_term_deposit', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC mt_ibcn_trans TABLE FROM IB_MB
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE mt_ibcn_trans';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;
        INSERT
        INTO mt_ibcn_trans
        ( id, trn_ref_no, trans_date, description, amount, currency, trans_type, trans_status, beneficiary_ac_no
        , beneficiary_name, applicant_ac_no, applicant_name, applicant_cif, value_date, applicant_branch
        , beneficiary_branch, create_by, create_date, approve_by, approve_date, fee_type, fee_amount, checksum
        , beneficiary_bank_name, fee_vat, fee_currency, product_cd, amount2, exchange_rate, fcc_revert, fcc_revert_batch
        , etl_status, etl_log_date)
        SELECT seq_mt_ibcn_trans.nextval
             , a.core_sn
             , a.create_time
             , NULL
             , a.amount
             , a.currency_code
             , CASE WHEN a.service_type = 'TR' THEN 'CTTHT'
                WHEN a.service_type = 'TF' AND a.is_fast_payment = 'Y' THEN 'CT247'
                WHEN a.service_type = 'TF' THEN 'CTNHT'
                ELSE a.service_type END
             , CASE WHEN a.status = 'SUCC' THEN 'S' ELSE a.status END
             , a.beneficiary_account_no
             , a.beneficiary_name
             , a.rollout_account_no
             , a.rollout_account_name
             , b.CIF_NO
             , NULL
             , a.rollout_branch_id
             , a.beneficiary_bank_id
             , a.create_by
             , a.create_time
             , a.update_by
             , a.update_time
             , a.paid_fee_src
             , a.fee
             , NULL
             , a.beneficiary_bank_name
             , a.vat
             , a.currency_code
             , NULL
             , a.amount_convert
             , a.exchange_rate
             , NULL
             , NULL
             , 'INSERT NEW'
             , SYSDATE
        FROM ebanking.bc_transfer_history@EBANKING.LOCALDOMAIN a
        join (select user_id, CIF_NO  from ebanking.bc_user_info@EBANKING.LOCALDOMAIN) b
            on a.user_id = b.user_id;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM mt_ibcn_trans;
        IF row_count > 0 THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'IB/MB', 'mt_ibcn_trans', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'IB/MB', 'mt_ibcn_trans', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC mt_ib_corp_trans TABLE FROM IB_MB
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE mt_ib_corp_trans';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;
        INSERT
        INTO mt_ib_corp_trans
        ( id, trn_ref_no, trans_date, description, amount, currency, trans_type, trans_status, beneficiary_ac_no
        , beneficiary_name, applicant_ac_no, applicant_name, applicant_cif, value_date, applicant_branch
        , beneficiary_branch, create_by, create_date, approve_by, approve_date, fee_type, fee_amount, checksum
        , beneficiary_bank_name, fee_vat, fee_currency, product_cd, amount2, exchange_rate, fcc_revert, fcc_revert_batch
        , check_xml, etl_status, etl_log_date)
        SELECT seq_mt_ib_corp_trans.nextval
             , a.core_sn
             , a.update_time
             , NULL
             , a.amount
             , a.currency_code
             , CASE WHEN a.service_type = 'TR' THEN 'CTTHT'
                WHEN a.service_type = 'TF' AND a.is_fast_payment = 'Y' THEN 'CT247'
                WHEN a.service_type = 'TF' THEN 'CTNHT'
                ELSE a.service_type END
             , CASE WHEN a.status = 'SUCC' THEN 'A' ELSE a.status END
             , a.bnfc_acct_no
             , a.bnfc_name
             , a.rollout_acct_no
             , a.rollout_acct_name
             , (select cif_no from ebanking.bb_corp_info@EBANKING.LOCALDOMAIN d where d.corp_id = a.corp_id)
             , NULL
             , a.rollout_branch_id
             , a.bnfc_branch_id
             , (select user_name from ebanking.bb_user_info@EBANKING.LOCALDOMAIN d where d.user_id = a.create_by)
             , a.create_time
             , (select user_name from ebanking.bb_user_info@EBANKING.LOCALDOMAIN d where d.user_id = a.update_by)
             , a.update_time
             , a.paid_fee_src
             , a.fee
             , NULL
             , a.bnfc_bank_name
             , a.vat
             , a.currency_code
             , NULL
             , a.amount_convert
             , a.exchange_rate_date
             , NULL
             , NULL
             , NULL
             , 'INSERT NEW'
             , SYSDATE
        FROM ebanking.bb_transfer_history@EBANKING.LOCALDOMAIN a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM mt_ib_corp_trans;
        IF row_count > 0 THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'IB/MB', 'mt_ib_corp_trans', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'IB/MB', 'mt_ib_corp_trans', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC sms_userregister TABLE FROM IB_MB
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE sms_userregister';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;
        INSERT
        INTO sms_userregister
        ( id, custid, active, custtype, servicesms, mobilephone, rolesystem, accountnodefault, createdate, updatedate
        , usercreate, userupdate, userauth, authdate, lang, auth_stat, branch, etl_status, etl_log_date)
        SELECT seq_tbl_transaction.nextval
             , custid
             , active
             , custtype
             , servicesms
             , mobilephone
             , rolesystem
             , accountnodefault
             , createdate
             , updatedate
             , usercreate
             , userupdate
             , userauth
             , authdate
             , lang
             , auth_stat
             , branch
             , 'INSERT NEW'
             , SYSDATE
        FROM ebanking.sms_userregister@"Ibpro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM sms_userregister;
        IF row_count > 0 THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'IB/MB', 'sms_userregister', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'IB/MB', 'sms_userregister', 'SYNC', row_count);
            COMMIT;
        END IF;

     -- SYNC ib_mb_vw_transfer TABLE FROM IB_MB
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE ib_mb_vw_transfer';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;
        INSERT
        INTO ib_mb_vw_transfer
        ( id, msg_id, source, source_ref, xref, trn_ref_no, trans_date, description, amount, currency, trans_type
        , applicant_ac_no, applicant_branch, applicant_name, applicant_cif, applicant_pan, beneficiary_ac_no
        , beneficiary_name, beneficiary_branch, beneficiary_bank_name, value_date, create_by, create_date, approve_by
        , approve_date, fee_type, fee_amount, fee_vat, fee_currency, product_code, currency2, amount2, exchange_rate
        , trans_status, err_code, err_desc, checksum, fccrefrevert, etl_status, etl_log_date)
        SELECT seq_tbl_transaction.nextval
             , null
             , null
             , null
             , null
             , trn_ref_no
             , trans_date
             , description
             , amount
             , SOURCE_CURRENCY_CODE
             , trans_type
             , applicant_ac_no
             , null
             , applicant_name
             , applicant_cif
             , null
             , beneficiary_ac_no
             , beneficiary_name
             , beneficiary_branch
             , beneficiary_bank_name
             , value_date
             , create_by
             , create_date
             , approve_by
             , approve_date
             , fee_type
             , fee_amount
             , fee_vat
             , fee_currency
             , product
             , null
             , null
             , null
             , trans_status
             , null
             , null
             , checksum
             , fccrefrevert
             , 'INSERT NEW'
             , SYSDATE
        FROM EBANKING.vw_transfer@EBANKING a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM ib_mb_vw_transfer;
        IF row_count > 0 THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'IB/MB', 'ib_mb_vw_transfer', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'IB/MB', 'ib_mb_vw_transfer', 'SYNC', row_count);
            COMMIT;
        END IF;

    EXCEPTION
        WHEN
            OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_etl_ib_mb_sync', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;


    END excute_etl_ib_mb_sync_job;
END package_etl_ib_mb_sync_job_new;
