CREATE PACKAGE package_etl_core_run_specific_day_job AS

    PROCEDURE excute_etl_core_run_specific_day_job(
        run_date DATE
    );

END package_etl_core_run_specific_day_job;
/

CREATE OR REPLACE PACKAGE BODY package_etl_core_run_specific_day_job
AS
    PROCEDURE excute_etl_core_run_specific_day_job(
        run_date DATE
    )
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
    BEGIN
        MERGE
        INTO
            sttm_customer a
        USING (SELECT *
               FROM sttm_customer@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.customer_no = b.customer_no
                )
        WHEN MATCHED THEN
            UPDATE
            SET customer_type             = b.customer_type
              , customer_name1            = b.customer_name1
              , address_line1             = b.address_line1
              , address_line3             = b.address_line3
              , address_line2             = b.address_line2
              , address_line4             = b.address_line4
              , country                   = b.country
              , short_name                = b.short_name
              , nationality               = b.nationality
              , language                  = b.language
              , exposure_country          = b.exposure_country
              , local_branch              = b.local_branch
              , liability_no              = b.liability_no
              , unique_id_name            = b.unique_id_name
              , unique_id_value           = b.unique_id_value
              , frozen                    = b.frozen
              , deceased                  = b.deceased
              , whereabouts_unknown       = b.whereabouts_unknown
              , customer_category         = b.customer_category
              , ho_ac_no                  = b.ho_ac_no
              , record_stat               = b.record_stat
              , auth_stat                 = b.auth_stat
              , mod_no                    = b.mod_no
              , maker_id                  = b.maker_id
              , maker_dt_stamp            = b.maker_dt_stamp
              , checker_id                = b.checker_id
              , checker_dt_stamp          = b.checker_dt_stamp
              , once_auth                 = b.once_auth
              , fx_cust_clean_risk_limit  = b.fx_cust_clean_risk_limit
              , overall_limit             = b.overall_limit
              , fx_clean_risk_limit       = b.fx_clean_risk_limit
              , credit_rating             = b.credit_rating
              , revision_date             = b.revision_date
              , limit_ccy                 = b.limit_ccy
              , cas_cust                  = b.cas_cust
              , liab_node                 = b.liab_node
              , sec_cust_clean_risk_limit = b.sec_cust_clean_risk_limit
              , sec_clean_risk_limit      = b.sec_clean_risk_limit
              , sec_cust_pstl_risk_limit  = b.sec_cust_pstl_risk_limit
              , sec_pstl_risk_limit       = b.sec_pstl_risk_limit
              , liab_br                   = b.liab_br
              , past_due_flag             = b.past_due_flag
              , default_media             = b.default_media
              , ssn                       = b.ssn
              , swift_code                = b.swift_code
              , loc_code                  = b.loc_code
              , short_name2               = b.short_name2
              , utility_provider          = b.utility_provider
              , utility_provider_id       = b.utility_provider_id
              , risk_profile              = b.risk_profile
              , debtor_category           = b.debtor_category
              , full_name                 = b.full_name
              , udf_1                     = b.udf_1
              , udf_2                     = b.udf_2
              , udf_3                     = b.udf_3
              , udf_4                     = b.udf_4
              , udf_5                     = b.udf_5
              , aml_required              = b.aml_required
              , aml_customer_grp          = b.aml_customer_grp
              , mailers_required          = b.mailers_required
              , group_code                = b.group_code
              , exposure_category         = b.exposure_category
              , cust_classification       = b.cust_classification
              , cif_status                = b.cif_status
              , cif_status_since          = b.cif_status_since
              , charge_group              = b.charge_group
              , introducer                = b.introducer
              , cust_clg_group            = b.cust_clg_group
              , chk_digit_valid_reqd      = b.chk_digit_valid_reqd
              , alg_id                    = b.alg_id
              , ft_accting_as_of          = b.ft_accting_as_of
              , unadvised                 = b.unadvised
              , tax_group                 = b.tax_group
              , consol_tax_cert_reqd      = b.consol_tax_cert_reqd
              , individual_tax_cert_reqd  = b.individual_tax_cert_reqd
              , cls_ccy_allowed           = b.cls_ccy_allowed
              , cls_participant           = b.cls_participant
              , fx_netting_customer       = b.fx_netting_customer
              , risk_category             = b.risk_category
              , fax_number                = b.fax_number
              , ext_ref_no                = b.ext_ref_no
              , crm_customer              = b.crm_customer
              , issuer_customer           = b.issuer_customer
              , treasury_customer         = b.treasury_customer
              , cif_creation_date         = b.cif_creation_date
              , wht_pct                   = b.wht_pct
              , rp_customer               = b.rp_customer
              , generate_mt920            = b.generate_mt920
              , etl_status                = 'UPDATE'
              , etl_log_date              = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( customer_no
            , customer_type
            , customer_name1
            , address_line1
            , address_line3
            , address_line2
            , address_line4
            , country
            , short_name
            , nationality
            , language
            , exposure_country
            , local_branch
            , liability_no
            , unique_id_name
            , unique_id_value
            , frozen
            , deceased
            , whereabouts_unknown
            , customer_category
            , ho_ac_no
            , record_stat
            , auth_stat
            , mod_no
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , once_auth
            , fx_cust_clean_risk_limit
            , overall_limit
            , fx_clean_risk_limit
            , credit_rating
            , revision_date
            , limit_ccy
            , cas_cust
            , liab_node
            , sec_cust_clean_risk_limit
            , sec_clean_risk_limit
            , sec_cust_pstl_risk_limit
            , sec_pstl_risk_limit
            , liab_br
            , past_due_flag
            , default_media
            , ssn
            , swift_code
            , loc_code
            , short_name2
            , utility_provider
            , utility_provider_id
            , risk_profile
            , debtor_category
            , full_name
            , udf_1
            , udf_2
            , udf_3
            , udf_4
            , udf_5
            , aml_required
            , aml_customer_grp
            , mailers_required
            , group_code
            , exposure_category
            , cust_classification
            , cif_status
            , cif_status_since
            , charge_group
            , introducer
            , cust_clg_group
            , chk_digit_valid_reqd
            , alg_id
            , ft_accting_as_of
            , unadvised
            , tax_group
            , consol_tax_cert_reqd
            , individual_tax_cert_reqd
            , cls_ccy_allowed
            , cls_participant
            , fx_netting_customer
            , risk_category
            , fax_number
            , ext_ref_no
            , crm_customer
            , issuer_customer
            , treasury_customer
            , cif_creation_date
            , wht_pct
            , rp_customer
            , generate_mt920
            , etl_status
            , etl_log_date)
            VALUES ( b.customer_no
                   , b.customer_type
                   , b.customer_name1
                   , b.address_line1
                   , b.address_line3
                   , b.address_line2
                   , b.address_line4
                   , b.country
                   , b.short_name
                   , b.nationality
                   , b.language
                   , b.exposure_country
                   , b.local_branch
                   , b.liability_no
                   , b.unique_id_name
                   , b.unique_id_value
                   , b.frozen
                   , b.deceased
                   , b.whereabouts_unknown
                   , b.customer_category
                   , b.ho_ac_no
                   , b.record_stat
                   , b.auth_stat
                   , b.mod_no
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.once_auth
                   , b.fx_cust_clean_risk_limit
                   , b.overall_limit
                   , b.fx_clean_risk_limit
                   , b.credit_rating
                   , b.revision_date
                   , b.limit_ccy
                   , b.cas_cust
                   , b.liab_node
                   , b.sec_cust_clean_risk_limit
                   , b.sec_clean_risk_limit
                   , b.sec_cust_pstl_risk_limit
                   , b.sec_pstl_risk_limit
                   , b.liab_br
                   , b.past_due_flag
                   , b.default_media
                   , b.ssn
                   , b.swift_code
                   , b.loc_code
                   , b.short_name2
                   , b.utility_provider
                   , b.utility_provider_id
                   , b.risk_profile
                   , b.debtor_category
                   , b.full_name
                   , b.udf_1
                   , b.udf_2
                   , b.udf_3
                   , b.udf_4
                   , b.udf_5
                   , b.aml_required
                   , b.aml_customer_grp
                   , b.mailers_required
                   , b.group_code
                   , b.exposure_category
                   , b.cust_classification
                   , b.cif_status
                   , b.cif_status_since
                   , b.charge_group
                   , b.introducer
                   , b.cust_clg_group
                   , b.chk_digit_valid_reqd
                   , b.alg_id
                   , b.ft_accting_as_of
                   , b.unadvised
                   , b.tax_group
                   , b.consol_tax_cert_reqd
                   , b.individual_tax_cert_reqd
                   , b.cls_ccy_allowed
                   , b.cls_participant
                   , b.fx_netting_customer
                   , b.risk_category
                   , b.fax_number
                   , b.ext_ref_no
                   , b.crm_customer
                   , b.issuer_customer
                   , b.treasury_customer
                   , b.cif_creation_date
                   , b.wht_pct
                   , b.rp_customer
                   , b.generate_mt920
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;

        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'STTM_CUSTOMER', 'RUN_SPECIFIC_DAY', row_count);
        COMMIT;

        MERGE
        INTO
            sttm_branch a
        USING (SELECT *
               FROM sttm_branch@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.branch_code = b.branch_code
                )
        WHEN MATCHED THEN
            UPDATE
            SET branch_name              = b.branch_name
              , branch_addr1             = b.branch_addr1
              , branch_addr2             = b.branch_addr2
              , branch_addr3             = b.branch_addr3
              , parent_branch            = b.parent_branch
              , regional_office          = b.regional_office
              , bank_code                = b.bank_code
              , host_name                = b.host_name
              , walkin_customer          = b.walkin_customer
              , netting_suspense_gl      = b.netting_suspense_gl
              , contingent_suspense_glsl = b.contingent_suspense_glsl
              , current_cycle            = b.current_cycle
              , current_period           = b.current_period
              , swift_addr               = b.swift_addr
              , telex_addr               = b.telex_addr
              , end_of_input             = b.end_of_input
              , rep_history_period       = b.rep_history_period
              , suspense_glsl            = b.suspense_glsl
              , generate                 = b.generate
              , time_level               = b.time_level
              , record_stat              = b.record_stat
              , auth_stat                = b.auth_stat
              , mod_no                   = b.mod_no
              , maker_id                 = b.maker_id
              , maker_dt_stamp           = b.maker_dt_stamp
              , checker_id               = b.checker_id
              , checker_dt_stamp         = b.checker_dt_stamp
              , once_auth                = b.once_auth
              , suspense_gl_fcy          = b.suspense_gl_fcy
              , cont_suspense_gl_fcy     = b.cont_suspense_gl_fcy
              , country_code             = b.country_code
              , cif_id                   = b.cif_id
              , job_stat                 = b.job_stat
              , fund_branch              = b.fund_branch
              , conversion_gl            = b.conversion_gl
              , conversion_txncode       = b.conversion_txncode
              , week_hol1                = b.week_hol1
              , week_hol2                = b.week_hol2
              , branch_lcy               = b.branch_lcy
              , offset_hr                = b.offset_hr
              , offset_min               = b.offset_min
              , clearing_acc             = b.clearing_acc
              , gen_mt103                = b.gen_mt103
              , def_bank_oper_code       = b.def_bank_oper_code
              , proceed_without_float    = b.proceed_without_float
              , offset_clearing_account  = b.offset_clearing_account
              , clearing_bank_code       = b.clearing_bank_code
              , cod_atm_stop             = b.cod_atm_stop
              , cod_start_tank           = b.cod_start_tank
              , cod_atm_branch           = b.cod_atm_branch
              , cod_inst_id              = b.cod_inst_id
              , cod_ib_trn_code          = b.cod_ib_trn_code
              , cod_cust_transfer        = b.cod_cust_transfer
              , atm_suspense_gl          = b.atm_suspense_gl
              , iban_mask_bank_code      = b.iban_mask_bank_code
              , iban_mask_account_number = b.iban_mask_account_number
              , clg_brn_code             = b.clg_brn_code
              , sector_code              = b.sector_code
              , clearing_brn             = b.clearing_brn
              , routing_no               = b.routing_no
              , mis_ccy_mismatch_group   = b.mis_ccy_mismatch_group
              , drsus_prod               = b.drsus_prod
              , crsus_prod               = b.crsus_prod
              , pc_clearing_brn          = b.pc_clearing_brn
              , interdict_check_reqd     = b.interdict_check_reqd
              , interdict_time_out       = b.interdict_time_out
              , auto_auth                = b.auto_auth
              , msg_gen_days             = b.msg_gen_days
              , referral_hr              = b.referral_hr
              , referral_min             = b.referral_min
              , gen_mt103p               = b.gen_mt103p
              , status_processing_basis  = b.status_processing_basis
              , back_valued_chk_req      = b.back_valued_chk_req
              , back_value_days          = b.back_value_days
              , provisioning_frequency   = b.provisioning_frequency
              , track_py_pnl_adjustment  = b.track_py_pnl_adjustment
              , uncollected_funds_basis  = b.uncollected_funds_basis
              , pl_split_reqd            = b.pl_split_reqd
              , offset_hours             = b.offset_hours
              , offset_mins              = b.offset_mins
              , current_tax_cycle        = b.current_tax_cycle
              , tax_cert_freq            = b.tax_cert_freq
              , tax_cert_day             = b.tax_cert_day
              , consol_tax_cert_reqd     = b.consol_tax_cert_reqd
              , individual_tax_cert_reqd = b.individual_tax_cert_reqd
              , deferred_stmt            = b.deferred_stmt
              , deferred_stmt_status     = b.deferred_stmt_status
              , internal_swap_customer   = b.internal_swap_customer
              , iceod_status             = b.iceod_status
              , enterprise_gl            = b.enterprise_gl
              , ldap_template            = b.ldap_template
              , dsn_name                 = b.dsn_name
              , brn_avail_stat           = b.brn_avail_stat
              , etl_status               = 'UPDATE'
              , etl_log_date             = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( branch_code
            , branch_name
            , branch_addr1
            , branch_addr2
            , branch_addr3
            , parent_branch
            , regional_office
            , bank_code
            , host_name
            , walkin_customer
            , netting_suspense_gl
            , contingent_suspense_glsl
            , current_cycle
            , current_period
            , swift_addr
            , telex_addr
            , end_of_input
            , rep_history_period
            , suspense_glsl
            , generate
            , time_level
            , record_stat
            , auth_stat
            , mod_no
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , once_auth
            , suspense_gl_fcy
            , cont_suspense_gl_fcy
            , country_code
            , cif_id
            , job_stat
            , fund_branch
            , conversion_gl
            , conversion_txncode
            , week_hol1
            , week_hol2
            , branch_lcy
            , offset_hr
            , offset_min
            , clearing_acc
            , gen_mt103
            , def_bank_oper_code
            , proceed_without_float
            , offset_clearing_account
            , clearing_bank_code
            , cod_atm_stop
            , cod_start_tank
            , cod_atm_branch
            , cod_inst_id
            , cod_ib_trn_code
            , cod_cust_transfer
            , atm_suspense_gl
            , iban_mask_bank_code
            , iban_mask_account_number
            , clg_brn_code
            , sector_code
            , clearing_brn
            , routing_no
            , mis_ccy_mismatch_group
            , drsus_prod
            , crsus_prod
            , pc_clearing_brn
            , interdict_check_reqd
            , interdict_time_out
            , auto_auth
            , msg_gen_days
            , referral_hr
            , referral_min
            , gen_mt103p
            , status_processing_basis
            , back_valued_chk_req
            , back_value_days
            , provisioning_frequency
            , track_py_pnl_adjustment
            , uncollected_funds_basis
            , pl_split_reqd
            , offset_hours
            , offset_mins
            , current_tax_cycle
            , tax_cert_freq
            , tax_cert_day
            , consol_tax_cert_reqd
            , individual_tax_cert_reqd
            , deferred_stmt
            , deferred_stmt_status
            , internal_swap_customer
            , iceod_status
            , enterprise_gl
            , ldap_template
            , dsn_name
            , brn_avail_stat
            , etl_status
            , etl_log_date)
            VALUES ( b.branch_code
                   , b.branch_name
                   , b.branch_addr1
                   , b.branch_addr2
                   , b.branch_addr3
                   , b.parent_branch
                   , b.regional_office
                   , b.bank_code
                   , b.host_name
                   , b.walkin_customer
                   , b.netting_suspense_gl
                   , b.contingent_suspense_glsl
                   , b.current_cycle
                   , b.current_period
                   , b.swift_addr
                   , b.telex_addr
                   , b.end_of_input
                   , b.rep_history_period
                   , b.suspense_glsl
                   , b.generate
                   , b.time_level
                   , b.record_stat
                   , b.auth_stat
                   , b.mod_no
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.once_auth
                   , b.suspense_gl_fcy
                   , b.cont_suspense_gl_fcy
                   , b.country_code
                   , b.cif_id
                   , b.job_stat
                   , b.fund_branch
                   , b.conversion_gl
                   , b.conversion_txncode
                   , b.week_hol1
                   , b.week_hol2
                   , b.branch_lcy
                   , b.offset_hr
                   , b.offset_min
                   , b.clearing_acc
                   , b.gen_mt103
                   , b.def_bank_oper_code
                   , b.proceed_without_float
                   , b.offset_clearing_account
                   , b.clearing_bank_code
                   , b.cod_atm_stop
                   , b.cod_start_tank
                   , b.cod_atm_branch
                   , b.cod_inst_id
                   , b.cod_ib_trn_code
                   , b.cod_cust_transfer
                   , b.atm_suspense_gl
                   , b.iban_mask_bank_code
                   , b.iban_mask_account_number
                   , b.clg_brn_code
                   , b.sector_code
                   , b.clearing_brn
                   , b.routing_no
                   , b.mis_ccy_mismatch_group
                   , b.drsus_prod
                   , b.crsus_prod
                   , b.pc_clearing_brn
                   , b.interdict_check_reqd
                   , b.interdict_time_out
                   , b.auto_auth
                   , b.msg_gen_days
                   , b.referral_hr
                   , b.referral_min
                   , b.gen_mt103p
                   , b.status_processing_basis
                   , b.back_valued_chk_req
                   , b.back_value_days
                   , b.provisioning_frequency
                   , b.track_py_pnl_adjustment
                   , b.uncollected_funds_basis
                   , b.pl_split_reqd
                   , b.offset_hours
                   , b.offset_mins
                   , b.current_tax_cycle
                   , b.tax_cert_freq
                   , b.tax_cert_day
                   , b.consol_tax_cert_reqd
                   , b.individual_tax_cert_reqd
                   , b.deferred_stmt
                   , b.deferred_stmt_status
                   , b.internal_swap_customer
                   , b.iceod_status
                   , b.enterprise_gl
                   , b.ldap_template
                   , b.dsn_name
                   , b.brn_avail_stat
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;

        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'STTM_BRANCH', 'RUN_SPECIFIC_DAY', row_count);
        COMMIT;

        MERGE
        INTO
            cltb_account_apps_master a
        USING (SELECT *
               FROM cltb_account_apps_master@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                        a.account_number = b.account_number
                    AND a.branch_code = b.branch_code
                )
        WHEN MATCHED THEN
            UPDATE
            SET application_num           = b.application_num
              , customer_id               = b.customer_id
              , product_code              = b.product_code
              , product_category          = b.product_category
              , book_date                 = b.book_date
              , value_date                = b.value_date
              , maturity_date             = b.maturity_date
              , amount_financed           = b.amount_financed
              , downpayment_amount        = b.downpayment_amount
              , currency                  = b.currency
              , original_st_date          = b.original_st_date
              , primary_applicant_id      = b.primary_applicant_id
              , primary_applicant_name    = b.primary_applicant_name
              , user_defined_status       = b.user_defined_status
              , calc_reqd                 = b.calc_reqd
              , back_val_eff_dt           = b.back_val_eff_dt
              , auto_man_rollover         = b.auto_man_rollover
              , schedule_basis            = b.schedule_basis
              , ude_rollover_basis        = b.ude_rollover_basis
              , rollover_type             = b.rollover_type
              , special_amount            = b.special_amount
              , rate_code_pref            = b.rate_code_pref
              , passbook_facility         = b.passbook_facility
              , atm_facility              = b.atm_facility
              , allow_back_period_entry   = b.allow_back_period_entry
              , int_stmt                  = b.int_stmt
              , track_receivable_aliq     = b.track_receivable_aliq
              , track_receivable_mliq     = b.track_receivable_mliq
              , liquidation_mode          = b.liquidation_mode
              , amend_past_paid_schedule  = b.amend_past_paid_schedule
              , cheque_book_facility      = b.cheque_book_facility
              , liq_back_valued_schedules = b.liq_back_valued_schedules
              , liq_comp_dates_flag       = b.liq_comp_dates_flag
              , retries_auto_liq          = b.retries_auto_liq
              , residual_amount           = b.residual_amount
              , account_status            = b.account_status
              , auth_stat                 = b.auth_stat
              , version_no                = b.version_no
              , latest_esn                = b.latest_esn
              , next_accr_date            = b.next_accr_date
              , has_problems              = b.has_problems
              , process_no                = b.process_no
              , amount_disbursed          = b.amount_disbursed
              , stop_accruals             = b.stop_accruals
              , funded_status             = b.funded_status
              , amortized                 = b.amortized
              , recalc_action_code        = b.recalc_action_code
              , maker_id                  = b.maker_id
              , maker_dt_stamp            = b.maker_dt_stamp
              , checker_id                = b.checker_id
              , checker_dt_stamp          = b.checker_dt_stamp
              , arvn_applied              = b.arvn_applied
              , alt_acc_no                = b.alt_acc_no
              , partial_liquidation       = b.partial_liquidation
              , aliq_reversed_pmt         = b.aliq_reversed_pmt
              , no_of_installments        = b.no_of_installments
              , frequency                 = b.frequency
              , frequency_unit            = b.frequency_unit
              , first_ins_date            = b.first_ins_date
              , linked_reference          = b.linked_reference
              , linkage_type              = b.linkage_type
              , field_char_1              = b.field_char_1
              , field_char_2              = b.field_char_2
              , field_char_3              = b.field_char_3
              , field_char_4              = b.field_char_4
              , field_char_5              = b.field_char_5
              , field_char_6              = b.field_char_6
              , field_char_7              = b.field_char_7
              , field_char_8              = b.field_char_8
              , field_char_9              = b.field_char_9
              , field_char_10             = b.field_char_10
              , field_char_11             = b.field_char_11
              , field_char_12             = b.field_char_12
              , field_char_13             = b.field_char_13
              , field_char_14             = b.field_char_14
              , field_char_15             = b.field_char_15
              , field_char_16             = b.field_char_16
              , field_char_17             = b.field_char_17
              , field_char_18             = b.field_char_18
              , field_char_19             = b.field_char_19
              , field_char_20             = b.field_char_20
              , field_number_1            = b.field_number_1
              , field_number_2            = b.field_number_2
              , field_number_3            = b.field_number_3
              , field_number_4            = b.field_number_4
              , field_number_5            = b.field_number_5
              , field_number_6            = b.field_number_6
              , field_number_7            = b.field_number_7
              , field_number_8            = b.field_number_8
              , field_number_9            = b.field_number_9
              , field_number_10           = b.field_number_10
              , field_number_11           = b.field_number_11
              , field_number_12           = b.field_number_12
              , field_number_13           = b.field_number_13
              , field_number_14           = b.field_number_14
              , field_number_15           = b.field_number_15
              , field_number_16           = b.field_number_16
              , field_number_17           = b.field_number_17
              , field_number_18           = b.field_number_18
              , field_number_19           = b.field_number_19
              , field_number_20           = b.field_number_20
              , field_date_1              = b.field_date_1
              , field_date_2              = b.field_date_2
              , field_date_3              = b.field_date_3
              , field_date_4              = b.field_date_4
              , field_date_5              = b.field_date_5
              , field_date_6              = b.field_date_6
              , field_date_7              = b.field_date_7
              , field_date_8              = b.field_date_8
              , field_date_9              = b.field_date_9
              , field_date_10             = b.field_date_10
              , roll_by                   = b.roll_by
              , maturity_type             = b.maturity_type
              , net_principal             = b.net_principal
              , index_xrate               = b.index_xrate
              , dr_payment_mode           = b.dr_payment_mode
              , cr_payment_mode           = b.cr_payment_mode
              , dr_prod_ac                = b.dr_prod_ac
              , cr_prod_ac                = b.cr_prod_ac
              , dr_acc_brn                = b.dr_acc_brn
              , cr_acc_brn                = b.cr_acc_brn
              , ext_acc_no_cr             = b.ext_acc_no_cr
              , ext_acc_name_cr           = b.ext_acc_name_cr
              , clg_bank_code_cr          = b.clg_bank_code_cr
              , clg_brn_code_cr           = b.clg_brn_code_cr
              , pc_cat_cr                 = b.pc_cat_cr
              , ext_acc_no_dr             = b.ext_acc_no_dr
              , ext_acc_name_dr           = b.ext_acc_name_dr
              , clg_bank_code_dr          = b.clg_bank_code_dr
              , clg_brn_code_dr           = b.clg_brn_code_dr
              , pc_cat_dr                 = b.pc_cat_dr
              , card_no                   = b.card_no
              , instrument_no_cr          = b.instrument_no_cr
              , routing_no_cr             = b.routing_no_cr
              , end_point_cr              = b.end_point_cr
              , clg_prod_code_cr          = b.clg_prod_code_cr
              , sector_code_cr            = b.sector_code_cr
              , instrument_no_dr          = b.instrument_no_dr
              , routing_no_dr             = b.routing_no_dr
              , end_point_dr              = b.end_point_dr
              , clg_prod_code_dr          = b.clg_prod_code_dr
              , sector_code_dr            = b.sector_code_dr
              , upload_source_dr          = b.upload_source_dr
              , upload_source_cr          = b.upload_source_cr
              , emi_amount                = b.emi_amount
              , cutoff_transaction        = b.cutoff_transaction
              , delinquency_status        = b.delinquency_status
              , execution_date            = b.execution_date
              , migration_date            = b.migration_date
              , usgt_status               = b.usgt_status
              , last_intraday_accr_dt     = b.last_intraday_accr_dt
              , giro_mode_dr              = b.giro_mode_dr
              , giro_service_dr           = b.giro_service_dr
              , giro_number_dr            = b.giro_number_dr
              , payer_acc_no_dr           = b.payer_acc_no_dr
              , payer_bank_code_dr        = b.payer_bank_code_dr
              , payer_branch_dr           = b.payer_branch_dr
              , payer_address1_dr         = b.payer_address1_dr
              , payer_address2_dr         = b.payer_address2_dr
              , payer_address3_dr         = b.payer_address3_dr
              , payer_address4_dr         = b.payer_address4_dr
              , giro_mode_cr              = b.giro_mode_cr
              , giro_service_cr           = b.giro_service_cr
              , giro_number_cr            = b.giro_number_cr
              , payer_acc_no_cr           = b.payer_acc_no_cr
              , payer_bank_code_cr        = b.payer_bank_code_cr
              , payer_branch_cr           = b.payer_branch_cr
              , payer_address1_cr         = b.payer_address1_cr
              , payer_address2_cr         = b.payer_address2_cr
              , payer_address3_cr         = b.payer_address3_cr
              , payer_address4_cr         = b.payer_address4_cr
              , due_dates_on              = b.due_dates_on
              , user_ref_no               = b.user_ref_no
              , bill_ref_no               = b.bill_ref_no
              , rollover_allowed          = b.rollover_allowed
              , amt_available             = b.amt_available
              , commitment_type           = b.commitment_type
              , loan_type                 = b.loan_type
              , module_code               = b.module_code
              , decap_int_amt_for_roll    = b.decap_int_amt_for_roll
              , etl_status                = 'UPDATE'
              , etl_log_date              = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( account_number
            , branch_code
            , application_num
            , customer_id
            , product_code
            , product_category
            , book_date
            , value_date
            , maturity_date
            , amount_financed
            , downpayment_amount
            , currency
            , original_st_date
            , primary_applicant_id
            , primary_applicant_name
            , user_defined_status
            , calc_reqd
            , back_val_eff_dt
            , auto_man_rollover
            , schedule_basis
            , ude_rollover_basis
            , rollover_type
            , special_amount
            , rate_code_pref
            , passbook_facility
            , atm_facility
            , allow_back_period_entry
            , int_stmt
            , track_receivable_aliq
            , track_receivable_mliq
            , liquidation_mode
            , amend_past_paid_schedule
            , cheque_book_facility
            , liq_back_valued_schedules
            , liq_comp_dates_flag
            , retries_auto_liq
            , residual_amount
            , account_status
            , auth_stat
            , version_no
            , latest_esn
            , next_accr_date
            , has_problems
            , process_no
            , amount_disbursed
            , stop_accruals
            , funded_status
            , amortized
            , recalc_action_code
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , arvn_applied
            , alt_acc_no
            , partial_liquidation
            , aliq_reversed_pmt
            , no_of_installments
            , frequency
            , frequency_unit
            , first_ins_date
            , linked_reference
            , linkage_type
            , field_char_1
            , field_char_2
            , field_char_3
            , field_char_4
            , field_char_5
            , field_char_6
            , field_char_7
            , field_char_8
            , field_char_9
            , field_char_10
            , field_char_11
            , field_char_12
            , field_char_13
            , field_char_14
            , field_char_15
            , field_char_16
            , field_char_17
            , field_char_18
            , field_char_19
            , field_char_20
            , field_number_1
            , field_number_2
            , field_number_3
            , field_number_4
            , field_number_5
            , field_number_6
            , field_number_7
            , field_number_8
            , field_number_9
            , field_number_10
            , field_number_11
            , field_number_12
            , field_number_13
            , field_number_14
            , field_number_15
            , field_number_16
            , field_number_17
            , field_number_18
            , field_number_19
            , field_number_20
            , field_date_1
            , field_date_2
            , field_date_3
            , field_date_4
            , field_date_5
            , field_date_6
            , field_date_7
            , field_date_8
            , field_date_9
            , field_date_10
            , roll_by
            , maturity_type
            , net_principal
            , index_xrate
            , dr_payment_mode
            , cr_payment_mode
            , dr_prod_ac
            , cr_prod_ac
            , dr_acc_brn
            , cr_acc_brn
            , ext_acc_no_cr
            , ext_acc_name_cr
            , clg_bank_code_cr
            , clg_brn_code_cr
            , pc_cat_cr
            , ext_acc_no_dr
            , ext_acc_name_dr
            , clg_bank_code_dr
            , clg_brn_code_dr
            , pc_cat_dr
            , card_no
            , instrument_no_cr
            , routing_no_cr
            , end_point_cr
            , clg_prod_code_cr
            , sector_code_cr
            , instrument_no_dr
            , routing_no_dr
            , end_point_dr
            , clg_prod_code_dr
            , sector_code_dr
            , upload_source_dr
            , upload_source_cr
            , emi_amount
            , cutoff_transaction
            , delinquency_status
            , execution_date
            , migration_date
            , usgt_status
            , last_intraday_accr_dt
            , giro_mode_dr
            , giro_service_dr
            , giro_number_dr
            , payer_acc_no_dr
            , payer_bank_code_dr
            , payer_branch_dr
            , payer_address1_dr
            , payer_address2_dr
            , payer_address3_dr
            , payer_address4_dr
            , giro_mode_cr
            , giro_service_cr
            , giro_number_cr
            , payer_acc_no_cr
            , payer_bank_code_cr
            , payer_branch_cr
            , payer_address1_cr
            , payer_address2_cr
            , payer_address3_cr
            , payer_address4_cr
            , due_dates_on
            , user_ref_no
            , bill_ref_no
            , rollover_allowed
            , amt_available
            , commitment_type
            , loan_type
            , module_code
            , decap_int_amt_for_roll
            , etl_status
            , etl_log_date)
            VALUES ( b.account_number
                   , b.branch_code
                   , b.application_num
                   , b.customer_id
                   , b.product_code
                   , b.product_category
                   , b.book_date
                   , b.value_date
                   , b.maturity_date
                   , b.amount_financed
                   , b.downpayment_amount
                   , b.currency
                   , b.original_st_date
                   , b.primary_applicant_id
                   , b.primary_applicant_name
                   , b.user_defined_status
                   , b.calc_reqd
                   , b.back_val_eff_dt
                   , b.auto_man_rollover
                   , b.schedule_basis
                   , b.ude_rollover_basis
                   , b.rollover_type
                   , b.special_amount
                   , b.rate_code_pref
                   , b.passbook_facility
                   , b.atm_facility
                   , b.allow_back_period_entry
                   , b.int_stmt
                   , b.track_receivable_aliq
                   , b.track_receivable_mliq
                   , b.liquidation_mode
                   , b.amend_past_paid_schedule
                   , b.cheque_book_facility
                   , b.liq_back_valued_schedules
                   , b.liq_comp_dates_flag
                   , b.retries_auto_liq
                   , b.residual_amount
                   , b.account_status
                   , b.auth_stat
                   , b.version_no
                   , b.latest_esn
                   , b.next_accr_date
                   , b.has_problems
                   , b.process_no
                   , b.amount_disbursed
                   , b.stop_accruals
                   , b.funded_status
                   , b.amortized
                   , b.recalc_action_code
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.arvn_applied
                   , b.alt_acc_no
                   , b.partial_liquidation
                   , b.aliq_reversed_pmt
                   , b.no_of_installments
                   , b.frequency
                   , b.frequency_unit
                   , b.first_ins_date
                   , b.linked_reference
                   , b.linkage_type
                   , b.field_char_1
                   , b.field_char_2
                   , b.field_char_3
                   , b.field_char_4
                   , b.field_char_5
                   , b.field_char_6
                   , b.field_char_7
                   , b.field_char_8
                   , b.field_char_9
                   , b.field_char_10
                   , b.field_char_11
                   , b.field_char_12
                   , b.field_char_13
                   , b.field_char_14
                   , b.field_char_15
                   , b.field_char_16
                   , b.field_char_17
                   , b.field_char_18
                   , b.field_char_19
                   , b.field_char_20
                   , b.field_number_1
                   , b.field_number_2
                   , b.field_number_3
                   , b.field_number_4
                   , b.field_number_5
                   , b.field_number_6
                   , b.field_number_7
                   , b.field_number_8
                   , b.field_number_9
                   , b.field_number_10
                   , b.field_number_11
                   , b.field_number_12
                   , b.field_number_13
                   , b.field_number_14
                   , b.field_number_15
                   , b.field_number_16
                   , b.field_number_17
                   , b.field_number_18
                   , b.field_number_19
                   , b.field_number_20
                   , b.field_date_1
                   , b.field_date_2
                   , b.field_date_3
                   , b.field_date_4
                   , b.field_date_5
                   , b.field_date_6
                   , b.field_date_7
                   , b.field_date_8
                   , b.field_date_9
                   , b.field_date_10
                   , b.roll_by
                   , b.maturity_type
                   , b.net_principal
                   , b.index_xrate
                   , b.dr_payment_mode
                   , b.cr_payment_mode
                   , b.dr_prod_ac
                   , b.cr_prod_ac
                   , b.dr_acc_brn
                   , b.cr_acc_brn
                   , b.ext_acc_no_cr
                   , b.ext_acc_name_cr
                   , b.clg_bank_code_cr
                   , b.clg_brn_code_cr
                   , b.pc_cat_cr
                   , b.ext_acc_no_dr
                   , b.ext_acc_name_dr
                   , b.clg_bank_code_dr
                   , b.clg_brn_code_dr
                   , b.pc_cat_dr
                   , b.card_no
                   , b.instrument_no_cr
                   , b.routing_no_cr
                   , b.end_point_cr
                   , b.clg_prod_code_cr
                   , b.sector_code_cr
                   , b.instrument_no_dr
                   , b.routing_no_dr
                   , b.end_point_dr
                   , b.clg_prod_code_dr
                   , b.sector_code_dr
                   , b.upload_source_dr
                   , b.upload_source_cr
                   , b.emi_amount
                   , b.cutoff_transaction
                   , b.delinquency_status
                   , b.execution_date
                   , b.migration_date
                   , b.usgt_status
                   , b.last_intraday_accr_dt
                   , b.giro_mode_dr
                   , b.giro_service_dr
                   , b.giro_number_dr
                   , b.payer_acc_no_dr
                   , b.payer_bank_code_dr
                   , b.payer_branch_dr
                   , b.payer_address1_dr
                   , b.payer_address2_dr
                   , b.payer_address3_dr
                   , b.payer_address4_dr
                   , b.giro_mode_cr
                   , b.giro_service_cr
                   , b.giro_number_cr
                   , b.payer_acc_no_cr
                   , b.payer_bank_code_cr
                   , b.payer_branch_cr
                   , b.payer_address1_cr
                   , b.payer_address2_cr
                   , b.payer_address3_cr
                   , b.payer_address4_cr
                   , b.due_dates_on
                   , b.user_ref_no
                   , b.bill_ref_no
                   , b.rollover_allowed
                   , b.amt_available
                   , b.commitment_type
                   , b.loan_type
                   , b.module_code
                   , b.decap_int_amt_for_roll
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'CLTB_ACCOUNT_APPS_MASTER', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            sttm_cust_account a
        USING (SELECT *
               FROM sttm_cust_account@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date)
                  OR TRUNC(date_last_cr_activity) = TRUNC(run_date)
                  OR TRUNC(date_last_dr_activity) = TRUNC(run_date)
                  OR TRUNC(date_last_cr) = TRUNC(run_date)
                  OR TRUNC(date_last_dr) = TRUNC(run_date))
            b
        ON
            (
                a.cust_ac_no = b.cust_ac_no
                )
        WHEN MATCHED THEN
            UPDATE
            SET branch_code                  = b.branch_code
              , cust_no                      = b.cust_no
              , ac_desc                      = b.ac_desc
              , ccy                          = b.ccy
              , account_class                = b.account_class
              , ac_stat_no_dr                = b.ac_stat_no_dr
              , ac_stat_no_cr                = b.ac_stat_no_cr
              , ac_stat_block                = b.ac_stat_block
              , ac_stat_stop_pay             = b.ac_stat_stop_pay
              , ac_stat_dormant              = b.ac_stat_dormant
              , joint_ac_indicator           = b.joint_ac_indicator
              , ac_open_date                 = b.ac_open_date
              , ac_stmt_day                  = b.ac_stmt_day
              , ac_stmt_cycle                = b.ac_stmt_cycle
              , alt_ac_no                    = b.alt_ac_no
              , cheque_book_facility         = b.cheque_book_facility
              , atm_facility                 = b.atm_facility
              , passbook_facility            = b.passbook_facility
              , ac_stmt_type                 = b.ac_stmt_type
              , dr_ho_line                   = b.dr_ho_line
              , cr_ho_line                   = b.cr_ho_line
              , cr_cb_line                   = b.cr_cb_line
              , dr_cb_line                   = b.dr_cb_line
              , sublimit                     = b.sublimit
              , uncoll_funds_limit           = b.uncoll_funds_limit
              , ac_stat_frozen               = b.ac_stat_frozen
              , previous_statement_date      = b.previous_statement_date
              , previous_statement_balance   = b.previous_statement_balance
              , previous_statement_no        = b.previous_statement_no
              , tod_limit_start_date         = b.tod_limit_start_date
              , tod_limit_end_date           = b.tod_limit_end_date
              , tod_limit                    = b.tod_limit
              , nominee1                     = b.nominee1
              , nominee2                     = b.nominee2
              , dr_gl                        = b.dr_gl
              , cr_gl                        = b.cr_gl
              , record_stat                  = b.record_stat
              , auth_stat                    = b.auth_stat
              , mod_no                       = b.mod_no
              , maker_id                     = b.maker_id
              , maker_dt_stamp               = b.maker_dt_stamp
              , checker_id                   = b.checker_id
              , checker_dt_stamp             = b.checker_dt_stamp
              , once_auth                    = b.once_auth
              , limit_ccy                    = b.limit_ccy
              , line_id                      = b.line_id
              , offline_limit                = b.offline_limit
              , cas_account                  = b.cas_account
              , acy_opening_bal              = b.acy_opening_bal
              , lcy_opening_bal              = b.lcy_opening_bal
              , acy_today_tover_dr           = b.acy_today_tover_dr
              , lcy_today_tover_dr           = b.lcy_today_tover_dr
              , acy_today_tover_cr           = b.acy_today_tover_cr
              , lcy_today_tover_cr           = b.lcy_today_tover_cr
              , acy_tank_cr                  = b.acy_tank_cr
              , acy_tank_dr                  = b.acy_tank_dr
              , lcy_tank_cr                  = b.lcy_tank_cr
              , lcy_tank_dr                  = b.lcy_tank_dr
              , acy_tover_cr                 = b.acy_tover_cr
              , lcy_tover_cr                 = b.lcy_tover_cr
              , acy_tank_uncollected         = b.acy_tank_uncollected
              , acy_curr_balance             = b.acy_curr_balance
              , lcy_curr_balance             = b.lcy_curr_balance
              , acy_blocked_amount           = b.acy_blocked_amount
              , acy_avl_bal                  = b.acy_avl_bal
              , acy_unauth_dr                = b.acy_unauth_dr
              , acy_unauth_tank_dr           = b.acy_unauth_tank_dr
              , acy_unauth_cr                = b.acy_unauth_cr
              , acy_unauth_tank_cr           = b.acy_unauth_tank_cr
              , acy_unauth_uncollected       = b.acy_unauth_uncollected
              , acy_unauth_tank_uncollected  = b.acy_unauth_tank_uncollected
              , acy_mtd_tover_dr             = b.acy_mtd_tover_dr
              , lcy_mtd_tover_dr             = b.lcy_mtd_tover_dr
              , acy_mtd_tover_cr             = b.acy_mtd_tover_cr
              , lcy_mtd_tover_cr             = b.lcy_mtd_tover_cr
              , acy_accrued_dr_ic            = b.acy_accrued_dr_ic
              , acy_accrued_cr_ic            = b.acy_accrued_cr_ic
              , date_last_cr_activity        = b.date_last_cr_activity
              , date_last_dr_activity        = b.date_last_dr_activity
              , date_last_dr                 = b.date_last_dr
              , date_last_cr                 = b.date_last_cr
              , acy_uncollected              = b.acy_uncollected
              , tod_start_date               = b.tod_start_date
              , tod_end_date                 = b.tod_end_date
              , dormancy_date                = b.dormancy_date
              , dormancy_days                = b.dormancy_days
              , has_tov                      = b.has_tov
              , last_ccy_conv_date           = b.last_ccy_conv_date
              , address1                     = b.address1
              , address2                     = b.address2
              , address3                     = b.address3
              , address4                     = b.address4
              , type_of_chq                  = b.type_of_chq
              , atm_cust_ac_no               = b.atm_cust_ac_no
              , atm_dly_amt_limit            = b.atm_dly_amt_limit
              , atm_dly_count_limit          = b.atm_dly_count_limit
              , gen_stmt_only_on_mvmt        = b.gen_stmt_only_on_mvmt
              , ac_stat_de_post              = b.ac_stat_de_post
              , display_iban_in_advices      = b.display_iban_in_advices
              , clearing_bank_code           = b.clearing_bank_code
              , clearing_ac_no               = b.clearing_ac_no
              , iban_ac_no                   = b.iban_ac_no
              , reg_cc_availability          = b.reg_cc_availability
              , reg_cc_available_funds       = b.reg_cc_available_funds
              , prev_ac_srno_printed_in_pbk  = b.prev_ac_srno_printed_in_pbk
              , latest_srno_submitted        = b.latest_srno_submitted
              , prev_runbal_printed_in_pbk   = b.prev_runbal_printed_in_pbk
              , latest_runbal_submmited      = b.latest_runbal_submmited
              , prev_page_no                 = b.prev_page_no
              , prev_line_no                 = b.prev_line_no
              , mt210_reqd                   = b.mt210_reqd
              , acc_stmt_type2               = b.acc_stmt_type2
              , acc_stmt_day2                = b.acc_stmt_day2
              , ac_stmt_cycle2               = b.ac_stmt_cycle2
              , previous_statement_date2     = b.previous_statement_date2
              , previous_statement_balance2  = b.previous_statement_balance2
              , previous_statement_no2       = b.previous_statement_no2
              , gen_stmt_only_on_mvmt2       = b.gen_stmt_only_on_mvmt2
              , acc_stmt_type3               = b.acc_stmt_type3
              , acc_stmt_day3                = b.acc_stmt_day3
              , ac_stmt_cycle3               = b.ac_stmt_cycle3
              , previous_statement_date3     = b.previous_statement_date3
              , previous_statement_balance3  = b.previous_statement_balance3
              , previous_statement_no3       = b.previous_statement_no3
              , gen_stmt_only_on_mvmt3       = b.gen_stmt_only_on_mvmt3
              , sweep_type                   = b.sweep_type
              , master_account_no            = b.master_account_no
              , auto_deposits_bal            = b.auto_deposits_bal
              , cas_customer                 = b.cas_customer
              , account_type                 = b.account_type
              , min_reqd_bal                 = b.min_reqd_bal
              , positive_pay_ac              = b.positive_pay_ac
              , stale_days                   = b.stale_days
              , cr_auto_ex_rate_lmt          = b.cr_auto_ex_rate_lmt
              , dr_auto_ex_rate_lmt          = b.dr_auto_ex_rate_lmt
              , track_receivable             = b.track_receivable
              , receivable_amount            = b.receivable_amount
              , product_list                 = b.product_list
              , txn_code_list                = b.txn_code_list
              , special_condition_product    = b.special_condition_product
              , special_condition_txncode    = b.special_condition_txncode
              , reg_d_applicable             = b.reg_d_applicable
              , regd_periodicity             = b.regd_periodicity
              , regd_start_date              = b.regd_start_date
              , regd_end_date                = b.regd_end_date
              , td_cert_printed              = b.td_cert_printed
              , checkbook_name_1             = b.checkbook_name_1
              , checkbook_name_2             = b.checkbook_name_2
              , auto_reorder_check_required  = b.auto_reorder_check_required
              , auto_reorder_check_level     = b.auto_reorder_check_level
              , auto_reorder_check_leaves    = b.auto_reorder_check_leaves
              , netting_required             = b.netting_required
              , referral_required            = b.referral_required
              , lodgement_book_facility      = b.lodgement_book_facility
              , acc_status                   = b.acc_status
              , status_since                 = b.status_since
              , inherit_reporting            = b.inherit_reporting
              , overdraft_since              = b.overdraft_since
              , prev_ovd_date                = b.prev_ovd_date
              , status_change_automatic      = b.status_change_automatic
              , overline_od_since            = b.overline_od_since
              , tod_since                    = b.tod_since
              , prev_tod_since               = b.prev_tod_since
              , dormant_param                = b.dormant_param
              , dr_int_due                   = b.dr_int_due
              , excl_sameday_rvrtrns_fm_stmt = b.excl_sameday_rvrtrns_fm_stmt
              , allow_back_period_entry      = b.allow_back_period_entry
              , auto_prov_reqd               = b.auto_prov_reqd
              , exposure_category            = b.exposure_category
              , risk_free_exp_amount         = b.risk_free_exp_amount
              , provision_amount             = b.provision_amount
              , credit_txn_limit             = b.credit_txn_limit
              , cr_lm_start_date             = b.cr_lm_start_date
              , cr_lm_rev_date               = b.cr_lm_rev_date
              , statement_account            = b.statement_account
              , account_derived_status       = b.account_derived_status
              , prov_ccy_type                = b.prov_ccy_type
              , chg_due                      = b.chg_due
              , withdrawable_uncolled_fund   = b.withdrawable_uncolled_fund
              , defer_recon                  = b.defer_recon
              , consolidation_reqd           = b.consolidation_reqd
              , funding                      = b.funding
              , funding_branch               = b.funding_branch
              , funding_account              = b.funding_account
              , mod9_validation_reqd         = b.mod9_validation_reqd
              , validation_digit             = b.validation_digit
              , location                     = b.location
              , media                        = b.media
              , acc_tanked_stat              = b.acc_tanked_stat
              , gen_interim_stmt             = b.gen_interim_stmt
              , gen_interim_stmt_on_mvmt     = b.gen_interim_stmt_on_mvmt
              , gen_balance_report           = b.gen_balance_report
              , interim_report_since         = b.interim_report_since
              , interim_report_type          = b.interim_report_type
              , balance_report_since         = b.balance_report_since
              , balance_report_type          = b.balance_report_type
              , interim_debit_amt            = b.interim_debit_amt
              , interim_credit_amt           = b.interim_credit_amt
              , interim_stmt_day_count       = b.interim_stmt_day_count
              , interim_stmt_ytd_count       = b.interim_stmt_ytd_count
              , etl_status                   = 'UPDATE'
              , etl_log_date                 = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( branch_code
            , cust_ac_no
            , ac_desc
            , cust_no
            , ccy
            , account_class
            , ac_stat_no_dr
            , ac_stat_no_cr
            , ac_stat_block
            , ac_stat_stop_pay
            , ac_stat_dormant
            , joint_ac_indicator
            , ac_open_date
            , ac_stmt_day
            , ac_stmt_cycle
            , alt_ac_no
            , cheque_book_facility
            , atm_facility
            , passbook_facility
            , ac_stmt_type
            , dr_ho_line
            , cr_ho_line
            , cr_cb_line
            , dr_cb_line
            , sublimit
            , uncoll_funds_limit
            , ac_stat_frozen
            , previous_statement_date
            , previous_statement_balance
            , previous_statement_no
            , tod_limit_start_date
            , tod_limit_end_date
            , tod_limit
            , nominee1
            , nominee2
            , dr_gl
            , cr_gl
            , record_stat
            , auth_stat
            , mod_no
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , once_auth
            , limit_ccy
            , line_id
            , offline_limit
            , cas_account
            , acy_opening_bal
            , lcy_opening_bal
            , acy_today_tover_dr
            , lcy_today_tover_dr
            , acy_today_tover_cr
            , lcy_today_tover_cr
            , acy_tank_cr
            , acy_tank_dr
            , lcy_tank_cr
            , lcy_tank_dr
            , acy_tover_cr
            , lcy_tover_cr
            , acy_tank_uncollected
            , acy_curr_balance
            , lcy_curr_balance
            , acy_blocked_amount
            , acy_avl_bal
            , acy_unauth_dr
            , acy_unauth_tank_dr
            , acy_unauth_cr
            , acy_unauth_tank_cr
            , acy_unauth_uncollected
            , acy_unauth_tank_uncollected
            , acy_mtd_tover_dr
            , lcy_mtd_tover_dr
            , acy_mtd_tover_cr
            , lcy_mtd_tover_cr
            , acy_accrued_dr_ic
            , acy_accrued_cr_ic
            , date_last_cr_activity
            , date_last_dr_activity
            , date_last_dr
            , date_last_cr
            , acy_uncollected
            , tod_start_date
            , tod_end_date
            , dormancy_date
            , dormancy_days
            , has_tov
            , last_ccy_conv_date
            , address1
            , address2
            , address3
            , address4
            , type_of_chq
            , atm_cust_ac_no
            , atm_dly_amt_limit
            , atm_dly_count_limit
            , gen_stmt_only_on_mvmt
            , ac_stat_de_post
            , display_iban_in_advices
            , clearing_bank_code
            , clearing_ac_no
            , iban_ac_no
            , reg_cc_availability
            , reg_cc_available_funds
            , prev_ac_srno_printed_in_pbk
            , latest_srno_submitted
            , prev_runbal_printed_in_pbk
            , latest_runbal_submmited
            , prev_page_no
            , prev_line_no
            , mt210_reqd
            , acc_stmt_type2
            , acc_stmt_day2
            , ac_stmt_cycle2
            , previous_statement_date2
            , previous_statement_balance2
            , previous_statement_no2
            , gen_stmt_only_on_mvmt2
            , acc_stmt_type3
            , acc_stmt_day3
            , ac_stmt_cycle3
            , previous_statement_date3
            , previous_statement_balance3
            , previous_statement_no3
            , gen_stmt_only_on_mvmt3
            , sweep_type
            , master_account_no
            , auto_deposits_bal
            , cas_customer
            , account_type
            , min_reqd_bal
            , positive_pay_ac
            , stale_days
            , cr_auto_ex_rate_lmt
            , dr_auto_ex_rate_lmt
            , track_receivable
            , receivable_amount
            , product_list
            , txn_code_list
            , special_condition_product
            , special_condition_txncode
            , reg_d_applicable
            , regd_periodicity
            , regd_start_date
            , regd_end_date
            , td_cert_printed
            , checkbook_name_1
            , checkbook_name_2
            , auto_reorder_check_required
            , auto_reorder_check_level
            , auto_reorder_check_leaves
            , netting_required
            , referral_required
            , lodgement_book_facility
            , acc_status
            , status_since
            , inherit_reporting
            , overdraft_since
            , prev_ovd_date
            , status_change_automatic
            , overline_od_since
            , tod_since
            , prev_tod_since
            , dormant_param
            , dr_int_due
            , excl_sameday_rvrtrns_fm_stmt
            , allow_back_period_entry
            , auto_prov_reqd
            , exposure_category
            , risk_free_exp_amount
            , provision_amount
            , credit_txn_limit
            , cr_lm_start_date
            , cr_lm_rev_date
            , statement_account
            , account_derived_status
            , prov_ccy_type
            , chg_due
            , withdrawable_uncolled_fund
            , defer_recon
            , consolidation_reqd
            , funding
            , funding_branch
            , funding_account
            , mod9_validation_reqd
            , validation_digit
            , location
            , media
            , acc_tanked_stat
            , gen_interim_stmt
            , gen_interim_stmt_on_mvmt
            , gen_balance_report
            , interim_report_since
            , interim_report_type
            , balance_report_since
            , balance_report_type
            , interim_debit_amt
            , interim_credit_amt
            , interim_stmt_day_count
            , interim_stmt_ytd_count
            , etl_status
            , etl_log_date)
            VALUES ( b.branch_code
                   , b.cust_ac_no
                   , b.ac_desc
                   , b.cust_no
                   , b.ccy
                   , b.account_class
                   , b.ac_stat_no_dr
                   , b.ac_stat_no_cr
                   , b.ac_stat_block
                   , b.ac_stat_stop_pay
                   , b.ac_stat_dormant
                   , b.joint_ac_indicator
                   , b.ac_open_date
                   , b.ac_stmt_day
                   , b.ac_stmt_cycle
                   , b.alt_ac_no
                   , b.cheque_book_facility
                   , b.atm_facility
                   , b.passbook_facility
                   , b.ac_stmt_type
                   , b.dr_ho_line
                   , b.cr_ho_line
                   , b.cr_cb_line
                   , b.dr_cb_line
                   , b.sublimit
                   , b.uncoll_funds_limit
                   , b.ac_stat_frozen
                   , b.previous_statement_date
                   , b.previous_statement_balance
                   , b.previous_statement_no
                   , b.tod_limit_start_date
                   , b.tod_limit_end_date
                   , b.tod_limit
                   , b.nominee1
                   , b.nominee2
                   , b.dr_gl
                   , b.cr_gl
                   , b.record_stat
                   , b.auth_stat
                   , b.mod_no
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.once_auth
                   , b.limit_ccy
                   , b.line_id
                   , b.offline_limit
                   , b.cas_account
                   , b.acy_opening_bal
                   , b.lcy_opening_bal
                   , b.acy_today_tover_dr
                   , b.lcy_today_tover_dr
                   , b.acy_today_tover_cr
                   , b.lcy_today_tover_cr
                   , b.acy_tank_cr
                   , b.acy_tank_dr
                   , b.lcy_tank_cr
                   , b.lcy_tank_dr
                   , b.acy_tover_cr
                   , b.lcy_tover_cr
                   , b.acy_tank_uncollected
                   , b.acy_curr_balance
                   , b.lcy_curr_balance
                   , b.acy_blocked_amount
                   , b.acy_avl_bal
                   , b.acy_unauth_dr
                   , b.acy_unauth_tank_dr
                   , b.acy_unauth_cr
                   , b.acy_unauth_tank_cr
                   , b.acy_unauth_uncollected
                   , b.acy_unauth_tank_uncollected
                   , b.acy_mtd_tover_dr
                   , b.lcy_mtd_tover_dr
                   , b.acy_mtd_tover_cr
                   , b.lcy_mtd_tover_cr
                   , b.acy_accrued_dr_ic
                   , b.acy_accrued_cr_ic
                   , b.date_last_cr_activity
                   , b.date_last_dr_activity
                   , b.date_last_dr
                   , b.date_last_cr
                   , b.acy_uncollected
                   , b.tod_start_date
                   , b.tod_end_date
                   , b.dormancy_date
                   , b.dormancy_days
                   , b.has_tov
                   , b.last_ccy_conv_date
                   , b.address1
                   , b.address2
                   , b.address3
                   , b.address4
                   , b.type_of_chq
                   , b.atm_cust_ac_no
                   , b.atm_dly_amt_limit
                   , b.atm_dly_count_limit
                   , b.gen_stmt_only_on_mvmt
                   , b.ac_stat_de_post
                   , b.display_iban_in_advices
                   , b.clearing_bank_code
                   , b.clearing_ac_no
                   , b.iban_ac_no
                   , b.reg_cc_availability
                   , b.reg_cc_available_funds
                   , b.prev_ac_srno_printed_in_pbk
                   , b.latest_srno_submitted
                   , b.prev_runbal_printed_in_pbk
                   , b.latest_runbal_submmited
                   , b.prev_page_no
                   , b.prev_line_no
                   , b.mt210_reqd
                   , b.acc_stmt_type2
                   , b.acc_stmt_day2
                   , b.ac_stmt_cycle2
                   , b.previous_statement_date2
                   , b.previous_statement_balance2
                   , b.previous_statement_no2
                   , b.gen_stmt_only_on_mvmt2
                   , b.acc_stmt_type3
                   , b.acc_stmt_day3
                   , b.ac_stmt_cycle3
                   , b.previous_statement_date3
                   , b.previous_statement_balance3
                   , b.previous_statement_no3
                   , b.gen_stmt_only_on_mvmt3
                   , b.sweep_type
                   , b.master_account_no
                   , b.auto_deposits_bal
                   , b.cas_customer
                   , b.account_type
                   , b.min_reqd_bal
                   , b.positive_pay_ac
                   , b.stale_days
                   , b.cr_auto_ex_rate_lmt
                   , b.dr_auto_ex_rate_lmt
                   , b.track_receivable
                   , b.receivable_amount
                   , b.product_list
                   , b.txn_code_list
                   , b.special_condition_product
                   , b.special_condition_txncode
                   , b.reg_d_applicable
                   , b.regd_periodicity
                   , b.regd_start_date
                   , b.regd_end_date
                   , b.td_cert_printed
                   , b.checkbook_name_1
                   , b.checkbook_name_2
                   , b.auto_reorder_check_required
                   , b.auto_reorder_check_level
                   , b.auto_reorder_check_leaves
                   , b.netting_required
                   , b.referral_required
                   , b.lodgement_book_facility
                   , b.acc_status
                   , b.status_since
                   , b.inherit_reporting
                   , b.overdraft_since
                   , b.prev_ovd_date
                   , b.status_change_automatic
                   , b.overline_od_since
                   , b.tod_since
                   , b.prev_tod_since
                   , b.dormant_param
                   , b.dr_int_due
                   , b.excl_sameday_rvrtrns_fm_stmt
                   , b.allow_back_period_entry
                   , b.auto_prov_reqd
                   , b.exposure_category
                   , b.risk_free_exp_amount
                   , b.provision_amount
                   , b.credit_txn_limit
                   , b.cr_lm_start_date
                   , b.cr_lm_rev_date
                   , b.statement_account
                   , b.account_derived_status
                   , b.prov_ccy_type
                   , b.chg_due
                   , b.withdrawable_uncolled_fund
                   , b.defer_recon
                   , b.consolidation_reqd
                   , b.funding
                   , b.funding_branch
                   , b.funding_account
                   , b.mod9_validation_reqd
                   , b.validation_digit
                   , b.location
                   , b.media
                   , b.acc_tanked_stat
                   , b.gen_interim_stmt
                   , b.gen_interim_stmt_on_mvmt
                   , b.gen_balance_report
                   , b.interim_report_since
                   , b.interim_report_type
                   , b.balance_report_since
                   , b.balance_report_type
                   , b.interim_debit_amt
                   , b.interim_credit_amt
                   , b.interim_stmt_day_count
                   , b.interim_stmt_ytd_count
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'STTM_CUST_ACCOUNT', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            sttm_customer_cat a
        USING (SELECT *
               FROM sttm_customer_cat@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.cust_cat = b.cust_cat
                )
        WHEN MATCHED THEN
            UPDATE
            SET cust_cat_desc        = b.cust_cat_desc
              , record_stat          = b.record_stat
              , auth_stat            = b.auth_stat
              , mod_no               = b.mod_no
              , maker_id             = b.maker_id
              , maker_dt_stamp       = b.maker_dt_stamp
              , checker_id           = b.checker_id
              , checker_dt_stamp     = b.checker_dt_stamp
              , once_auth            = b.once_auth
              , populate_changes_log = b.populate_changes_log
              , etl_status           = 'UPDATE'
              , etl_log_date         = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( cust_cat
            , cust_cat_desc
            , record_stat
            , auth_stat
            , mod_no
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , once_auth
            , populate_changes_log
            , etl_status
            , etl_log_date)
            VALUES ( b.cust_cat
                   , b.cust_cat_desc
                   , b.record_stat
                   , b.auth_stat
                   , b.mod_no
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.once_auth
                   , b.populate_changes_log
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'STTM_CUSTOMER_CAT', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            sttm_cust_classification a
        USING (SELECT *
               FROM sttm_cust_classification@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.cust_classification = b.cust_classification
                )
        WHEN MATCHED THEN
            UPDATE
            SET description      = b.description
              , maker_id         = b.maker_id
              , checker_id       = b.checker_id
              , maker_dt_stamp   = b.maker_dt_stamp
              , checker_dt_stamp = b.checker_dt_stamp
              , once_auth        = b.once_auth
              , auth_stat        = b.auth_stat
              , record_stat      = b.record_stat
              , mod_no           = b.mod_no
              , etl_status       = 'UPDATE'
              , etl_log_date     = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( cust_classification
            , description
            , maker_id
            , checker_id
            , maker_dt_stamp
            , checker_dt_stamp
            , once_auth
            , auth_stat
            , record_stat
            , mod_no
            , etl_status
            , etl_log_date)
            VALUES ( b.cust_classification
                   , b.description
                   , b.maker_id
                   , b.checker_id
                   , b.maker_dt_stamp
                   , b.checker_dt_stamp
                   , b.once_auth
                   , b.auth_stat
                   , b.record_stat
                   , b.mod_no
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'STTM_CUST_CLASSIFICATION', 'RUN_SPECIFIC_DAY', row_count);


        DELETE FROM actb_history WHERE TRUNC(trn_dt) = TRUNC(run_date);
        INSERT
        INTO actb_history
        ( trn_ref_no, event_sr_no, event, ac_branch, ac_no, ac_ccy, drcr_ind, trn_code, amount_tag, fcy_amount
        , exch_rate, lcy_amount, related_customer, related_account, related_reference, mis_flag, mis_head, trn_dt
        , value_dt, txn_init_date, financial_cycle, period_code, instrument_code, bank_code, type, category, cust_gl
        , module, ac_entry_sr_no, ib, flg_position_status, glmis_update_flag, user_id, curr_no, batch_no, print_stat
        , product_accrual, auth_id, product, glmis_val_upd_flag, external_ref_no, dont_showin_stmt, ic_bal_inclusion
        , aml_exception, orig_pnl_gl, stmt_dt, entry_seq_no, etl_status, etl_log_date)
        SELECT trn_ref_no
             , event_sr_no
             , event
             , ac_branch
             , ac_no
             , ac_ccy
             , drcr_ind
             , trn_code
             , amount_tag
             , fcy_amount
             , exch_rate
             , lcy_amount
             , related_customer
             , related_account
             , related_reference
             , mis_flag
             , mis_head
             , trn_dt
             , value_dt
             , txn_init_date
             , financial_cycle
             , period_code
             , instrument_code
             , bank_code
             , type
             , category
             , cust_gl
             , module
             , ac_entry_sr_no
             , ib
             , flg_position_status
             , glmis_update_flag
             , user_id
             , curr_no
             , batch_no
             , print_stat
             , product_accrual
             , auth_id
             , product
             , glmis_val_upd_flag
             , external_ref_no
             , dont_showin_stmt
             , ic_bal_inclusion
             , aml_exception
             , orig_pnl_gl
             , stmt_dt
             , entry_seq_no
             , 'INSERT NEW'
             , SYSDATE
        FROM staging.actb_history_staging@dbstaging.localdomain a
        WHERE TRUNC(trn_dt) = TRUNC(run_date);
        row_count := SQL%ROWCOUNT;

        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'ACTB_HISTORY', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            cytm_ccy_defn a
        USING (SELECT *
               FROM cytm_ccy_defn@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.ccy_code = b.ccy_code
                )
        WHEN MATCHED THEN
            UPDATE
            SET ccy_name            = b.ccy_name
              , country             = b.country
              , ccy_decimals        = b.ccy_decimals
              , ccy_round_rule      = b.ccy_round_rule
              , ccy_round_unit      = b.ccy_round_unit
              , ccy_format_mask     = b.ccy_format_mask
              , ccy_spot_days       = b.ccy_spot_days
              , ccy_int_method      = b.ccy_int_method
              , record_stat         = b.record_stat
              , once_auth           = b.once_auth
              , auth_stat           = b.auth_stat
              , mod_no              = b.mod_no
              , maker_id            = b.maker_id
              , maker_dt_stamp      = b.maker_dt_stamp
              , checker_id          = b.checker_id
              , checker_dt_stamp    = b.checker_dt_stamp
              , position_gl         = b.position_gl
              , position_eqvgl      = b.position_eqvgl
              , ccy_eur_type        = b.ccy_eur_type
              , ccy_tol_limit       = b.ccy_tol_limit
              , settlement_msg_days = b.settlement_msg_days
              , index_flag          = b.index_flag
              , index_base_ccy      = b.index_base_ccy
              , cut_off_hr          = b.cut_off_hr
              , cut_off_min         = b.cut_off_min
              , alt_ccy_code        = b.alt_ccy_code
              , eur_conversion_reqd = b.eur_conversion_reqd
              , cut_off_days        = b.cut_off_days
              , cr_auto_ex_rate_lmt = b.cr_auto_ex_rate_lmt
              , dr_auto_ex_rate_lmt = b.dr_auto_ex_rate_lmt
              , ccy_type            = b.ccy_type
              , gen_103p            = b.gen_103p
              , cls_ccy             = b.cls_ccy
              , fx_netting_days     = b.fx_netting_days
              , iso_num_ccy_code    = b.iso_num_ccy_code
              , etl_status          = 'UPDATE'
              , etl_log_date        = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( ccy_code
            , ccy_name
            , country
            , ccy_decimals
            , ccy_round_rule
            , ccy_round_unit
            , ccy_format_mask
            , ccy_spot_days
            , ccy_int_method
            , record_stat
            , once_auth
            , auth_stat
            , mod_no
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , position_gl
            , position_eqvgl
            , ccy_eur_type
            , ccy_tol_limit
            , settlement_msg_days
            , index_flag
            , index_base_ccy
            , cut_off_hr
            , cut_off_min
            , alt_ccy_code
            , eur_conversion_reqd
            , cut_off_days
            , cr_auto_ex_rate_lmt
            , dr_auto_ex_rate_lmt
            , ccy_type
            , gen_103p
            , cls_ccy
            , fx_netting_days
            , iso_num_ccy_code
            , etl_status
            , etl_log_date)
            VALUES ( b.ccy_code
                   , b.ccy_name
                   , b.country
                   , b.ccy_decimals
                   , b.ccy_round_rule
                   , b.ccy_round_unit
                   , b.ccy_format_mask
                   , b.ccy_spot_days
                   , b.ccy_int_method
                   , b.record_stat
                   , b.once_auth
                   , b.auth_stat
                   , b.mod_no
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.position_gl
                   , b.position_eqvgl
                   , b.ccy_eur_type
                   , b.ccy_tol_limit
                   , b.settlement_msg_days
                   , b.index_flag
                   , b.index_base_ccy
                   , b.cut_off_hr
                   , b.cut_off_min
                   , b.alt_ccy_code
                   , b.eur_conversion_reqd
                   , b.cut_off_days
                   , b.cr_auto_ex_rate_lmt
                   , b.dr_auto_ex_rate_lmt
                   , b.ccy_type
                   , b.gen_103p
                   , b.cls_ccy
                   , b.fx_netting_days
                   , b.iso_num_ccy_code
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'CYTM_CCY_DEFN', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            sttm_bank a
        USING (SELECT *
               FROM sttm_bank@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.bank_code = b.bank_code
                )
        WHEN MATCHED THEN
            UPDATE
            SET auto_gen_cif                  = b.auto_gen_cif
              , interface_id                  = b.interface_id
              , spl_files_prg_days            = b.spl_files_prg_days
              , cif_mask                      = b.cif_mask
              , bank_name                     = b.bank_name
              , ho_branch                     = b.ho_branch
              , discount_ccy                  = b.discount_ccy
              , reporting_currency            = b.reporting_currency
              , ho_currency                   = b.ho_currency
              , ib_account_scheme             = b.ib_account_scheme
              , customer_acc_mask             = b.customer_acc_mask
              , gl_mask                       = b.gl_mask
              , ex_rate_copy                  = b.ex_rate_copy
              , record_stat                   = b.record_stat
              , auth_stat                     = b.auth_stat
              , mod_no                        = b.mod_no
              , maker_id                      = b.maker_id
              , maker_dt_stamp                = b.maker_dt_stamp
              , checker_id                    = b.checker_id
              , checker_dt_stamp              = b.checker_dt_stamp
              , once_auth                     = b.once_auth
              , online_gl_update              = b.online_gl_update
              , year_end_pnl_trncode          = b.year_end_pnl_trncode
              , year_end_pnl_gl               = b.year_end_pnl_gl
              , dw_ac_no                      = b.dw_ac_no
              , dw_ac_no_width                = b.dw_ac_no_width
              , flg_position_ac               = b.flg_position_ac
              , gl_tab_prg_days               = b.gl_tab_prg_days
              , bank_lcy                      = b.bank_lcy
              , checksum_algorithm            = b.checksum_algorithm
              , chq_no_chk_dgt                = b.chq_no_chk_dgt
              , chq_numbering                 = b.chq_numbering
              , unique_cheque_no              = b.unique_cheque_no
              , spread_application            = b.spread_application
              , daily_mis_refinance           = b.daily_mis_refinance
              , auto_generate_batch           = b.auto_generate_batch
              , fl_br_rate_prop               = b.fl_br_rate_prop
              , interbranch_entity            = b.interbranch_entity
              , routing_mask                  = b.routing_mask
              , clg_bank_cd                   = b.clg_bank_cd
              , auto_gen_ccy_mismatch_entries = b.auto_gen_ccy_mismatch_entries
              , auto_gen_vd_mismatch_entries  = b.auto_gen_vd_mismatch_entries
              , ccy_mismatch_acc              = b.ccy_mismatch_acc
              , vdate_mismatch_acc            = b.vdate_mismatch_acc
              , ccy_mismatch_cont_acc         = b.ccy_mismatch_cont_acc
              , vdate_mismatch_cont_acc       = b.vdate_mismatch_cont_acc
              , lead_days                     = b.lead_days
              , unique_lodgment_no            = b.unique_lodgment_no
              , trs_sort_code                 = b.trs_sort_code
              , trs_acc_no                    = b.trs_acc_no
              , trs_suspence_acc              = b.trs_suspence_acc
              , trs_pc_prod_category          = b.trs_pc_prod_category
              , limits_history_required       = b.limits_history_required
              , user_batch_restriction        = b.user_batch_restriction
              , branch_wise_limits            = b.branch_wise_limits
              , propagate_cif                 = b.propagate_cif
              , propagate_cust_addr           = b.propagate_cust_addr
              , propagate_bic                 = b.propagate_bic
              , cif_mask_maint                = b.cif_mask_maint
              , pairwise_pos_handoff          = b.pairwise_pos_handoff
              , sso_installed                 = b.sso_installed
              , chq_mask_basis                = b.chq_mask_basis
              , chq_mask                      = b.chq_mask
              , etl_status                    = 'UPDATE'
              , etl_log_date                  = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( auto_gen_cif
            , interface_id
            , spl_files_prg_days
            , cif_mask
            , bank_code
            , bank_name
            , ho_branch
            , discount_ccy
            , reporting_currency
            , ho_currency
            , ib_account_scheme
            , customer_acc_mask
            , gl_mask
            , ex_rate_copy
            , record_stat
            , auth_stat
            , mod_no
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , once_auth
            , online_gl_update
            , year_end_pnl_trncode
            , year_end_pnl_gl
            , dw_ac_no
            , dw_ac_no_width
            , flg_position_ac
            , gl_tab_prg_days
            , bank_lcy
            , checksum_algorithm
            , chq_no_chk_dgt
            , chq_numbering
            , unique_cheque_no
            , spread_application
            , daily_mis_refinance
            , auto_generate_batch
            , fl_br_rate_prop
            , interbranch_entity
            , routing_mask
            , clg_bank_cd
            , auto_gen_ccy_mismatch_entries
            , auto_gen_vd_mismatch_entries
            , ccy_mismatch_acc
            , vdate_mismatch_acc
            , ccy_mismatch_cont_acc
            , vdate_mismatch_cont_acc
            , lead_days
            , unique_lodgment_no
            , trs_sort_code
            , trs_acc_no
            , trs_suspence_acc
            , trs_pc_prod_category
            , limits_history_required
            , user_batch_restriction
            , branch_wise_limits
            , propagate_cif
            , propagate_cust_addr
            , propagate_bic
            , cif_mask_maint
            , pairwise_pos_handoff
            , sso_installed
            , chq_mask_basis
            , chq_mask
            , etl_status
            , etl_log_date)
            VALUES ( b.auto_gen_cif
                   , b.interface_id
                   , b.spl_files_prg_days
                   , b.cif_mask
                   , b.bank_code
                   , b.bank_name
                   , b.ho_branch
                   , b.discount_ccy
                   , b.reporting_currency
                   , b.ho_currency
                   , b.ib_account_scheme
                   , b.customer_acc_mask
                   , b.gl_mask
                   , b.ex_rate_copy
                   , b.record_stat
                   , b.auth_stat
                   , b.mod_no
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.once_auth
                   , b.online_gl_update
                   , b.year_end_pnl_trncode
                   , b.year_end_pnl_gl
                   , b.dw_ac_no
                   , b.dw_ac_no_width
                   , b.flg_position_ac
                   , b.gl_tab_prg_days
                   , b.bank_lcy
                   , b.checksum_algorithm
                   , b.chq_no_chk_dgt
                   , b.chq_numbering
                   , b.unique_cheque_no
                   , b.spread_application
                   , b.daily_mis_refinance
                   , b.auto_generate_batch
                   , b.fl_br_rate_prop
                   , b.interbranch_entity
                   , b.routing_mask
                   , b.clg_bank_cd
                   , b.auto_gen_ccy_mismatch_entries
                   , b.auto_gen_vd_mismatch_entries
                   , b.ccy_mismatch_acc
                   , b.vdate_mismatch_acc
                   , b.ccy_mismatch_cont_acc
                   , b.vdate_mismatch_cont_acc
                   , b.lead_days
                   , b.unique_lodgment_no
                   , b.trs_sort_code
                   , b.trs_acc_no
                   , b.trs_suspence_acc
                   , b.trs_pc_prod_category
                   , b.limits_history_required
                   , b.user_batch_restriction
                   , b.branch_wise_limits
                   , b.propagate_cif
                   , b.propagate_cust_addr
                   , b.propagate_bic
                   , b.cif_mask_maint
                   , b.pairwise_pos_handoff
                   , b.sso_installed
                   , b.chq_mask_basis
                   , b.chq_mask
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'STTM_BANK', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            ictm_rule a
        USING (SELECT *
               FROM ictm_rule@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.rule_id = b.rule_id
                )
        WHEN MATCHED THEN
            UPDATE
            SET rule_desc        = b.rule_desc
              , rule_catg        = b.rule_catg
              , record_stat      = b.record_stat
              , auth_stat        = b.auth_stat
              , once_auth        = b.once_auth
              , mod_no           = b.mod_no
              , maker_id         = b.maker_id
              , maker_dt_stamp   = b.maker_dt_stamp
              , checker_id       = b.checker_id
              , checker_dt_stamp = b.checker_dt_stamp
              , int_acc_open     = b.int_acc_open
              , int_acc_close    = b.int_acc_close
              , has_accr         = b.has_accr
              , primary_element  = b.primary_element
              , il_rule          = b.il_rule
              , il_type          = b.il_type
              , etl_status       = 'UPDATE'
              , etl_log_date     = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( rule_id
            , rule_desc
            , rule_catg
            , record_stat
            , auth_stat
            , once_auth
            , mod_no
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , int_acc_open
            , int_acc_close
            , has_accr
            , primary_element
            , il_rule
            , il_type
            , etl_status
            , etl_log_date)
            VALUES ( b.rule_id
                   , b.rule_desc
                   , b.rule_catg
                   , b.record_stat
                   , b.auth_stat
                   , b.once_auth
                   , b.mod_no
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.int_acc_open
                   , b.int_acc_close
                   , b.has_accr
                   , b.primary_element
                   , b.il_rule
                   , b.il_type
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'ICTM_RULE', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            cstm_product a
        USING (SELECT *
               FROM cstm_product@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.product_code = b.product_code
                )
        WHEN MATCHED THEN
            UPDATE
            SET product_description      = b.product_description
              , product_slogan           = b.product_slogan
              , product_remarks          = b.product_remarks
              , product_start_date       = b.product_start_date
              , product_end_date         = b.product_end_date
              , product_group            = b.product_group
              , warehouse_code           = b.warehouse_code
              , part_of_product          = b.part_of_product
              , module                   = b.module
              , record_stat              = b.record_stat
              , auth_stat                = b.auth_stat
              , once_auth                = b.once_auth
              , maker_id                 = b.maker_id
              , maker_dt_stamp           = b.maker_dt_stamp
              , checker_id               = b.checker_id
              , checker_dt_stamp         = b.checker_dt_stamp
              , mod_no                   = b.mod_no
              , pool_code                = b.pool_code
              , no_of_legs               = b.no_of_legs
              , branches_list            = b.branches_list
              , currencies_list          = b.currencies_list
              , categories_list          = b.categories_list
              , normal_rate_variance     = b.normal_rate_variance
              , maximum_rate_variance    = b.maximum_rate_variance
              , product_type             = b.product_type
              , rate_code_preferred      = b.rate_code_preferred
              , rate_type_preferred      = b.rate_type_preferred
              , rth_class                = b.rth_class
              , asset_categories_list    = b.asset_categories_list
              , location_list            = b.location_list
              , gen_mt103p               = b.gen_mt103p
              , include_for_tds_calc     = b.include_for_tds_calc
              , instrument_product_allow = b.instrument_product_allow
              , portfolio_product_allow  = b.portfolio_product_allow
              , etl_status               = 'UPDATE'
              , etl_log_date             = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( product_code
            , product_description
            , product_slogan
            , product_remarks
            , product_start_date
            , product_end_date
            , product_group
            , warehouse_code
            , part_of_product
            , module
            , record_stat
            , auth_stat
            , once_auth
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , mod_no
            , pool_code
            , no_of_legs
            , branches_list
            , currencies_list
            , categories_list
            , normal_rate_variance
            , maximum_rate_variance
            , product_type
            , rate_code_preferred
            , rate_type_preferred
            , rth_class
            , asset_categories_list
            , location_list
            , gen_mt103p
            , include_for_tds_calc
            , instrument_product_allow
            , portfolio_product_allow
            , etl_status
            , etl_log_date)
            VALUES ( b.product_code
                   , b.product_description
                   , b.product_slogan
                   , b.product_remarks
                   , b.product_start_date
                   , b.product_end_date
                   , b.product_group
                   , b.warehouse_code
                   , b.part_of_product
                   , b.module
                   , b.record_stat
                   , b.auth_stat
                   , b.once_auth
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.mod_no
                   , b.pool_code
                   , b.no_of_legs
                   , b.branches_list
                   , b.currencies_list
                   , b.categories_list
                   , b.normal_rate_variance
                   , b.maximum_rate_variance
                   , b.product_type
                   , b.rate_code_preferred
                   , b.rate_type_preferred
                   , b.rth_class
                   , b.asset_categories_list
                   , b.location_list
                   , b.gen_mt103p
                   , b.include_for_tds_calc
                   , b.instrument_product_allow
                   , b.portfolio_product_allow
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'CSTM_PRODUCT', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            sttm_account_class a
        USING (SELECT *
               FROM sttm_account_class@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.account_class = b.account_class
                )
        WHEN MATCHED THEN
            UPDATE
            SET description                    = b.description
              , ac_class_type                  = b.ac_class_type
              , dormancy                       = b.dormancy
              , acc_stmt_type                  = b.acc_stmt_type
              , acc_stmt_cycle                 = b.acc_stmt_cycle
              , statement_day                  = b.statement_day
              , limit_check_required           = b.limit_check_required
              , overdraft_facility             = b.overdraft_facility
              , ic_inclusion                   = b.ic_inclusion
              , track_accrued_ic               = b.track_accrued_ic
              , passbook_facility              = b.passbook_facility
              , cheque_book_facility           = b.cheque_book_facility
              , atm_facility                   = b.atm_facility
              , dr_gl_line                     = b.dr_gl_line
              , cr_gl_line                     = b.cr_gl_line
              , natural_gl_sign                = b.natural_gl_sign
              , dr_cb_line                     = b.dr_cb_line
              , cr_cb_line                     = b.cr_cb_line
              , dr_ho_line                     = b.dr_ho_line
              , cr_ho_line                     = b.cr_ho_line
              , branch_list                    = b.branch_list
              , ccy_list                       = b.ccy_list
              , cuscat_list                    = b.cuscat_list
              , mod_no                         = b.mod_no
              , record_stat                    = b.record_stat
              , once_auth                      = b.once_auth
              , auth_stat                      = b.auth_stat
              , maker_id                       = b.maker_id
              , maker_dt_stamp                 = b.maker_dt_stamp
              , checker_id                     = b.checker_id
              , checker_dt_stamp               = b.checker_dt_stamp
              , stacccls_branch_list           = b.stacccls_branch_list
              , stacccls_ccy_list              = b.stacccls_ccy_list
              , stacccls_cuscat_list           = b.stacccls_cuscat_list
              , has_is                         = b.has_is
              , offline_limit                  = b.offline_limit
              , acst_format                    = b.acst_format
              , display_iban_in_advices        = b.display_iban_in_advices
              , auto_rollover                  = b.auto_rollover
              , default_tenor_days             = b.default_tenor_days
              , default_tenor_months           = b.default_tenor_months
              , default_tenor_years            = b.default_tenor_years
              , close_on_maturity              = b.close_on_maturity
              , move_int_to_unclaimed          = b.move_int_to_unclaimed
              , move_pric_to_unclaimed         = b.move_pric_to_unclaimed
              , avail_bal_reqd                 = b.avail_bal_reqd
              , acc_stmt_type2                 = b.acc_stmt_type2
              , acc_stmt_cycle2                = b.acc_stmt_cycle2
              , statement_day2                 = b.statement_day2
              , acc_stmt_type3                 = b.acc_stmt_type3
              , acc_stmt_cycle3                = b.acc_stmt_cycle3
              , statement_day3                 = b.statement_day3
              , auto_dep_ac_class              = b.auto_dep_ac_class
              , auto_dep_trn_code              = b.auto_dep_trn_code
              , sweep_mode                     = b.sweep_mode
              , min_bal_reqd                   = b.min_bal_reqd
              , dep_multiple_of                = b.dep_multiple_of
              , auto_dep_ccy                   = b.auto_dep_ccy
              , auto_dep_break_method          = b.auto_dep_break_method
              , auto_dep_def_rate_code         = b.auto_dep_def_rate_code
              , auto_dep_def_rate_type         = b.auto_dep_def_rate_type
              , break_deposits_first           = b.break_deposits_first
              , provide_interest_on_broken_dep = b.provide_interest_on_broken_dep
              , has_drcr_adv                   = b.has_drcr_adv
              , rd_flag                        = b.rd_flag
              , rd_move_mat_to_unclaimed       = b.rd_move_mat_to_unclaimed
              , rd_move_funds_on_ovd           = b.rd_move_funds_on_ovd
              , rd_schedule_days               = b.rd_schedule_days
              , rd_schedule_months             = b.rd_schedule_months
              , rd_schedule_years              = b.rd_schedule_years
              , rd_min_installment_amt         = b.rd_min_installment_amt
              , rd_min_schedule_days           = b.rd_min_schedule_days
              , rd_max_schedule_days           = b.rd_max_schedule_days
              , interpay                       = b.interpay
              , track_receivable               = b.track_receivable
              , product_list                   = b.product_list
              , txn_code_list                  = b.txn_code_list
              , ac_stat_de_post                = b.ac_stat_de_post
              , account_code                   = b.account_code
              , reg_d_applicable               = b.reg_d_applicable
              , regd_periodicity               = b.regd_periodicity
              , auto_reorder_check_required    = b.auto_reorder_check_required
              , auto_reorder_check_level       = b.auto_reorder_check_level
              , auto_reorder_check_leaves      = b.auto_reorder_check_leaves
              , referral_required              = b.referral_required
              , acc_statistics                 = b.acc_statistics
              , lodgement_book                 = b.lodgement_book
              , status_change_automatic        = b.status_change_automatic
              , dormant_param                  = b.dormant_param
              , end_date                       = b.end_date
              , min_tenor_days                 = b.min_tenor_days
              , min_tenor_months               = b.min_tenor_months
              , min_tenor_years                = b.min_tenor_years
              , max_tenor_days                 = b.max_tenor_days
              , max_tenor_months               = b.max_tenor_months
              , max_tenor_years                = b.max_tenor_years
              , min_amount                     = b.min_amount
              , max_amount                     = b.max_amount
              , grace_period                   = b.grace_period
              , partial_liquidation            = b.partial_liquidation
              , dr_int_liqd_days               = b.dr_int_liqd_days
              , dr_int_liqd_mode               = b.dr_int_liqd_mode
              , dr_int_using_recv              = b.dr_int_using_recv
              , verify_funds_for_drint         = b.verify_funds_for_drint
              , dr_int_notice                  = b.dr_int_notice
              , allow_part_liq_with_amt_blk    = b.allow_part_liq_with_amt_blk
              , excl_sameday_rvrtrns_fm_stmt   = b.excl_sameday_rvrtrns_fm_stmt
              , allow_back_period_entry        = b.allow_back_period_entry
              , event_class_code               = b.event_class_code
              , auto_prov_reqd                 = b.auto_prov_reqd
              , provisioning_frequency         = b.provisioning_frequency
              , exposure_category              = b.exposure_category
              , prov_ccy_type                  = b.prov_ccy_type
              , chg_start_adv                  = b.chg_start_adv
              , free_banking_days              = b.free_banking_days
              , advice_days                    = b.advice_days
              , consolidation_reqd             = b.consolidation_reqd
              , ilm_applicable                 = b.ilm_applicable
              , gen_interim_stmt               = b.gen_interim_stmt
              , gen_interim_stmt_on_mvmt       = b.gen_interim_stmt_on_mvmt
              , gen_balance_report             = b.gen_balance_report
              , interim_report_since           = b.interim_report_since
              , interim_report_type            = b.interim_report_type
              , balance_report_since           = b.balance_report_since
              , balance_report_type            = b.balance_report_type
              , recomp_mdt_at_roll             = b.recomp_mdt_at_roll
              , deferred_bal_update            = b.deferred_bal_update
              , etl_status                     = 'UPDATE'
              , etl_log_date                   = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( account_class
            , description
            , ac_class_type
            , dormancy
            , acc_stmt_type
            , acc_stmt_cycle
            , statement_day
            , limit_check_required
            , overdraft_facility
            , ic_inclusion
            , track_accrued_ic
            , passbook_facility
            , cheque_book_facility
            , atm_facility
            , dr_gl_line
            , cr_gl_line
            , natural_gl_sign
            , dr_cb_line
            , cr_cb_line
            , dr_ho_line
            , cr_ho_line
            , branch_list
            , ccy_list
            , cuscat_list
            , mod_no
            , record_stat
            , once_auth
            , auth_stat
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , stacccls_branch_list
            , stacccls_ccy_list
            , stacccls_cuscat_list
            , has_is
            , offline_limit
            , acst_format
            , display_iban_in_advices
            , auto_rollover
            , default_tenor_days
            , default_tenor_months
            , default_tenor_years
            , close_on_maturity
            , move_int_to_unclaimed
            , move_pric_to_unclaimed
            , avail_bal_reqd
            , acc_stmt_type2
            , acc_stmt_cycle2
            , statement_day2
            , acc_stmt_type3
            , acc_stmt_cycle3
            , statement_day3
            , auto_dep_ac_class
            , auto_dep_trn_code
            , sweep_mode
            , min_bal_reqd
            , dep_multiple_of
            , auto_dep_ccy
            , auto_dep_break_method
            , auto_dep_def_rate_code
            , auto_dep_def_rate_type
            , break_deposits_first
            , provide_interest_on_broken_dep
            , has_drcr_adv
            , rd_flag
            , rd_move_mat_to_unclaimed
            , rd_move_funds_on_ovd
            , rd_schedule_days
            , rd_schedule_months
            , rd_schedule_years
            , rd_min_installment_amt
            , rd_min_schedule_days
            , rd_max_schedule_days
            , interpay
            , track_receivable
            , product_list
            , txn_code_list
            , ac_stat_de_post
            , account_code
            , reg_d_applicable
            , regd_periodicity
            , auto_reorder_check_required
            , auto_reorder_check_level
            , auto_reorder_check_leaves
            , referral_required
            , acc_statistics
            , lodgement_book
            , status_change_automatic
            , dormant_param
            , end_date
            , min_tenor_days
            , min_tenor_months
            , min_tenor_years
            , max_tenor_days
            , max_tenor_months
            , max_tenor_years
            , min_amount
            , max_amount
            , grace_period
            , partial_liquidation
            , dr_int_liqd_days
            , dr_int_liqd_mode
            , dr_int_using_recv
            , verify_funds_for_drint
            , dr_int_notice
            , allow_part_liq_with_amt_blk
            , excl_sameday_rvrtrns_fm_stmt
            , allow_back_period_entry
            , event_class_code
            , auto_prov_reqd
            , provisioning_frequency
            , exposure_category
            , prov_ccy_type
            , chg_start_adv
            , free_banking_days
            , advice_days
            , consolidation_reqd
            , ilm_applicable
            , gen_interim_stmt
            , gen_interim_stmt_on_mvmt
            , gen_balance_report
            , interim_report_since
            , interim_report_type
            , balance_report_since
            , balance_report_type
            , recomp_mdt_at_roll
            , deferred_bal_update
            , etl_status
            , etl_log_date)
            VALUES ( b.account_class
                   , b.description
                   , b.ac_class_type
                   , b.dormancy
                   , b.acc_stmt_type
                   , b.acc_stmt_cycle
                   , b.statement_day
                   , b.limit_check_required
                   , b.overdraft_facility
                   , b.ic_inclusion
                   , b.track_accrued_ic
                   , b.passbook_facility
                   , b.cheque_book_facility
                   , b.atm_facility
                   , b.dr_gl_line
                   , b.cr_gl_line
                   , b.natural_gl_sign
                   , b.dr_cb_line
                   , b.cr_cb_line
                   , b.dr_ho_line
                   , b.cr_ho_line
                   , b.branch_list
                   , b.ccy_list
                   , b.cuscat_list
                   , b.mod_no
                   , b.record_stat
                   , b.once_auth
                   , b.auth_stat
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.stacccls_branch_list
                   , b.stacccls_ccy_list
                   , b.stacccls_cuscat_list
                   , b.has_is
                   , b.offline_limit
                   , b.acst_format
                   , b.display_iban_in_advices
                   , b.auto_rollover
                   , b.default_tenor_days
                   , b.default_tenor_months
                   , b.default_tenor_years
                   , b.close_on_maturity
                   , b.move_int_to_unclaimed
                   , b.move_pric_to_unclaimed
                   , b.avail_bal_reqd
                   , b.acc_stmt_type2
                   , b.acc_stmt_cycle2
                   , b.statement_day2
                   , b.acc_stmt_type3
                   , b.acc_stmt_cycle3
                   , b.statement_day3
                   , b.auto_dep_ac_class
                   , b.auto_dep_trn_code
                   , b.sweep_mode
                   , b.min_bal_reqd
                   , b.dep_multiple_of
                   , b.auto_dep_ccy
                   , b.auto_dep_break_method
                   , b.auto_dep_def_rate_code
                   , b.auto_dep_def_rate_type
                   , b.break_deposits_first
                   , b.provide_interest_on_broken_dep
                   , b.has_drcr_adv
                   , b.rd_flag
                   , b.rd_move_mat_to_unclaimed
                   , b.rd_move_funds_on_ovd
                   , b.rd_schedule_days
                   , b.rd_schedule_months
                   , b.rd_schedule_years
                   , b.rd_min_installment_amt
                   , b.rd_min_schedule_days
                   , b.rd_max_schedule_days
                   , b.interpay
                   , b.track_receivable
                   , b.product_list
                   , b.txn_code_list
                   , b.ac_stat_de_post
                   , b.account_code
                   , b.reg_d_applicable
                   , b.regd_periodicity
                   , b.auto_reorder_check_required
                   , b.auto_reorder_check_level
                   , b.auto_reorder_check_leaves
                   , b.referral_required
                   , b.acc_statistics
                   , b.lodgement_book
                   , b.status_change_automatic
                   , b.dormant_param
                   , b.end_date
                   , b.min_tenor_days
                   , b.min_tenor_months
                   , b.min_tenor_years
                   , b.max_tenor_days
                   , b.max_tenor_months
                   , b.max_tenor_years
                   , b.min_amount
                   , b.max_amount
                   , b.grace_period
                   , b.partial_liquidation
                   , b.dr_int_liqd_days
                   , b.dr_int_liqd_mode
                   , b.dr_int_using_recv
                   , b.verify_funds_for_drint
                   , b.dr_int_notice
                   , b.allow_part_liq_with_amt_blk
                   , b.excl_sameday_rvrtrns_fm_stmt
                   , b.allow_back_period_entry
                   , b.event_class_code
                   , b.auto_prov_reqd
                   , b.provisioning_frequency
                   , b.exposure_category
                   , b.prov_ccy_type
                   , b.chg_start_adv
                   , b.free_banking_days
                   , b.advice_days
                   , b.consolidation_reqd
                   , b.ilm_applicable
                   , b.gen_interim_stmt
                   , b.gen_interim_stmt_on_mvmt
                   , b.gen_balance_report
                   , b.interim_report_since
                   , b.interim_report_type
                   , b.balance_report_since
                   , b.balance_report_type
                   , b.recomp_mdt_at_roll
                   , b.deferred_bal_update
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'STTM_ACCOUNT_CLASS', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            sttm_acclass_ude_types a
        USING (SELECT *
               FROM sttm_acclass_ude_types@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                        a.aclass = b.aclass
                    AND a.product_code = b.product_code
                    AND a.ccy_code = b.ccy_code
                    AND a.branch_code = b.branch_code
                )
        WHEN MATCHED THEN
            UPDATE
            SET ude_id           = b.ude_id
              , debit_credit     = b.debit_credit
              , maker_id         = b.maker_id
              , checker_id       = b.checker_id
              , maker_dt_stamp   = b.maker_dt_stamp
              , checker_dt_stamp = b.checker_dt_stamp
              , once_auth        = b.once_auth
              , auth_stat        = b.auth_stat
              , record_stat      = b.record_stat
              , mod_no           = b.mod_no
              , etl_status       = 'UPDATE'
              , etl_log_date     = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( aclass
            , product_code
            , ccy_code
            , branch_code
            , ude_id
            , debit_credit
            , maker_id
            , checker_id
            , maker_dt_stamp
            , checker_dt_stamp
            , once_auth
            , auth_stat
            , record_stat
            , mod_no
            , etl_status
            , etl_log_date)
            VALUES ( b.aclass
                   , b.product_code
                   , b.ccy_code
                   , b.branch_code
                   , b.ude_id
                   , b.debit_credit
                   , b.maker_id
                   , b.checker_id
                   , b.maker_dt_stamp
                   , b.checker_dt_stamp
                   , b.once_auth
                   , b.auth_stat
                   , b.record_stat
                   , b.mod_no
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'STTM_ACCLASS_UDE_TYPES', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            sttm_trn_code a
        USING (SELECT *
               FROM sttm_trn_code@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.trn_code = b.trn_code
                )
        WHEN MATCHED THEN
            UPDATE
            SET trn_desc                 = b.trn_desc
              , trn_swift_code           = b.trn_swift_code
              , avl_info                 = b.avl_info
              , cheque_mandatory         = b.cheque_mandatory
              , ic_txn_count             = b.ic_txn_count
              , ic_tover_inclusion       = b.ic_tover_inclusion
              , consider_for_activity    = b.consider_for_activity
              , mis_head                 = b.mis_head
              , mod_no                   = b.mod_no
              , maker_dt_stamp           = b.maker_dt_stamp
              , record_stat              = b.record_stat
              , auth_stat                = b.auth_stat
              , once_auth                = b.once_auth
              , maker_id                 = b.maker_id
              , new_val_date             = b.new_val_date
              , avl_days                 = b.avl_days
              , checker_dt_stamp         = b.checker_dt_stamp
              , checker_id               = b.checker_id
              , ic_txn_count_number      = b.ic_txn_count_number
              , avail_bal_reqd           = b.avail_bal_reqd
              , ic_penalty               = b.ic_penalty
              , ic_bal_inclusion         = b.ic_bal_inclusion
              , aml_monitoring           = b.aml_monitoring
              , product_cat              = b.product_cat
              , intraday_release         = b.intraday_release
              , ib_in_lcy                = b.ib_in_lcy
              , stmt_dt_basis            = b.stmt_dt_basis
              , acumen_trn_code          = b.acumen_trn_code
              , ignore_lm_bvt_processing = b.ignore_lm_bvt_processing
              , etl_status               = 'UPDATE'
              , etl_log_date             = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( trn_code
            , trn_desc
            , trn_swift_code
            , avl_info
            , cheque_mandatory
            , ic_txn_count
            , ic_tover_inclusion
            , consider_for_activity
            , mis_head
            , mod_no
            , maker_dt_stamp
            , record_stat
            , auth_stat
            , once_auth
            , maker_id
            , new_val_date
            , avl_days
            , checker_dt_stamp
            , checker_id
            , ic_txn_count_number
            , avail_bal_reqd
            , ic_penalty
            , ic_bal_inclusion
            , aml_monitoring
            , product_cat
            , intraday_release
            , ib_in_lcy
            , stmt_dt_basis
            , acumen_trn_code
            , ignore_lm_bvt_processing
            , etl_status
            , etl_log_date)
            VALUES ( b.trn_code
                   , b.trn_desc
                   , b.trn_swift_code
                   , b.avl_info
                   , b.cheque_mandatory
                   , b.ic_txn_count
                   , b.ic_tover_inclusion
                   , b.consider_for_activity
                   , b.mis_head
                   , b.mod_no
                   , b.maker_dt_stamp
                   , b.record_stat
                   , b.auth_stat
                   , b.once_auth
                   , b.maker_id
                   , b.new_val_date
                   , b.avl_days
                   , b.checker_dt_stamp
                   , b.checker_id
                   , b.ic_txn_count_number
                   , b.avail_bal_reqd
                   , b.ic_penalty
                   , b.ic_bal_inclusion
                   , b.aml_monitoring
                   , b.product_cat
                   , b.intraday_release
                   , b.ib_in_lcy
                   , b.stmt_dt_basis
                   , b.acumen_trn_code
                   , b.ignore_lm_bvt_processing
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'STTM_TRN_CODE', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            detb_rtl_teller a
        USING (SELECT *
               FROM detb_rtl_teller@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.xref = b.xref
                )
        WHEN MATCHED THEN
            UPDATE
            SET product_code       = b.product_code
              , branch_code        = b.branch_code
              , trn_ref_no         = b.trn_ref_no
              , txn_acc            = b.txn_acc
              , txn_ccy            = b.txn_ccy
              , txn_amount         = b.txn_amount
              , txn_branch         = b.txn_branch
              , txn_trn_code       = b.txn_trn_code
              , ofs_acc            = b.ofs_acc
              , ofs_ccy            = b.ofs_ccy
              , ofs_amount         = b.ofs_amount
              , ofs_branch         = b.ofs_branch
              , ofs_trn_code       = b.ofs_trn_code
              , exch_rate          = b.exch_rate
              , lcy_amount         = b.lcy_amount
              , trn_dt             = b.trn_dt
              , value_dt           = b.value_dt
              , dr_instrument_code = b.dr_instrument_code
              , cr_instrument_code = b.cr_instrument_code
              , rel_customer       = b.rel_customer
              , charge_account     = b.charge_account
              , chg_gl             = b.chg_gl
              , chg_ccy            = b.chg_ccy
              , chg_amt            = b.chg_amt
              , chg_in_acy         = b.chg_in_acy
              , chg_in_lcy         = b.chg_in_lcy
              , chg_ccy_acy_rate   = b.chg_ccy_acy_rate
              , acy_lcy_rate       = b.acy_lcy_rate
              , netting_ind        = b.netting_ind
              , txn_code           = b.txn_code
              , mis_head_1         = b.mis_head_1
              , chg_gl_1           = b.chg_gl_1
              , chg_ccy_1          = b.chg_ccy_1
              , chg_amt_1          = b.chg_amt_1
              , chg_in_acy_1       = b.chg_in_acy_1
              , chg_in_lcy_1       = b.chg_in_lcy_1
              , chg_ccy_acy_rate_1 = b.chg_ccy_acy_rate_1
              , acy_lcy_rate_1     = b.acy_lcy_rate_1
              , netting_ind_1      = b.netting_ind_1
              , txn_code_1         = b.txn_code_1
              , mis_head_2         = b.mis_head_2
              , chg_gl_2           = b.chg_gl_2
              , chg_ccy_2          = b.chg_ccy_2
              , chg_amt_2          = b.chg_amt_2
              , chg_in_acy_2       = b.chg_in_acy_2
              , chg_in_lcy_2       = b.chg_in_lcy_2
              , chg_ccy_acy_rate_2 = b.chg_ccy_acy_rate_2
              , acy_lcy_rate_2     = b.acy_lcy_rate_2
              , netting_ind_2      = b.netting_ind_2
              , txn_code_2         = b.txn_code_2
              , mis_head_3         = b.mis_head_3
              , chg_gl_3           = b.chg_gl_3
              , chg_ccy_3          = b.chg_ccy_3
              , chg_amt_3          = b.chg_amt_3
              , chg_in_acy_3       = b.chg_in_acy_3
              , chg_in_lcy_3       = b.chg_in_lcy_3
              , chg_ccy_acy_rate_3 = b.chg_ccy_acy_rate_3
              , acy_lcy_rate_3     = b.acy_lcy_rate_3
              , netting_ind_3      = b.netting_ind_3
              , txn_code_3         = b.txn_code_3
              , mis_head_4         = b.mis_head_4
              , chg_gl_4           = b.chg_gl_4
              , chg_ccy_4          = b.chg_ccy_4
              , chg_amt_4          = b.chg_amt_4
              , chg_in_acy_4       = b.chg_in_acy_4
              , chg_in_lcy_4       = b.chg_in_lcy_4
              , chg_ccy_acy_rate_4 = b.chg_ccy_acy_rate_4
              , acy_lcy_rate_4     = b.acy_lcy_rate_4
              , netting_ind_4      = b.netting_ind_4
              , txn_code_4         = b.txn_code_4
              , mis_head_5         = b.mis_head_5
              , rem_acc            = b.rem_acc
              , rem_bank           = b.rem_bank
              , rem_branch         = b.rem_branch
              , routing_no         = b.routing_no
              , end_point          = b.end_point
              , serial_no          = b.serial_no
              , record_stat        = b.record_stat
              , auth_stat          = b.auth_stat
              , maker_id           = b.maker_id
              , maker_dt_stamp     = b.maker_dt_stamp
              , checker_id         = b.checker_id
              , checker_dt_stamp   = b.checker_dt_stamp
              , repair_reason      = b.repair_reason
              , mod_no             = b.mod_no
              , scode              = b.scode
              , mis_head           = b.mis_head
              , narrative          = b.narrative
              , dr_acc             = b.dr_acc
              , module             = b.module
              , esn                = b.esn
              , event_code         = b.event_code
              , route_code         = b.route_code
              , ft_problem         = b.ft_problem
              , track_receivable   = b.track_receivable
              , time_received      = b.time_received
              , their_chgs2        = b.their_chgs2
              , their_chgs3        = b.their_chgs3
              , their_chgs4        = b.their_chgs4
              , their_chgs         = b.their_chgs
              , their_chgs1        = b.their_chgs1
              , their_acc          = b.their_acc
              , their_acc1         = b.their_acc1
              , their_acc2         = b.their_acc2
              , their_acc3         = b.their_acc3
              , their_acc4         = b.their_acc4
              , lcy_exch_rate      = b.lcy_exch_rate
              , tot_chg_in_tcy     = b.tot_chg_in_tcy
              , chg_desc           = b.chg_desc
              , chg_desc1          = b.chg_desc1
              , chg_desc2          = b.chg_desc2
              , chg_desc3          = b.chg_desc3
              , chg_desc4          = b.chg_desc4
              , chg_type           = b.chg_type
              , chg_type1          = b.chg_type1
              , chg_type2          = b.chg_type2
              , chg_type3          = b.chg_type3
              , chg_type4          = b.chg_type4
              , waiver             = b.waiver
              , waiver1            = b.waiver1
              , waiver2            = b.waiver2
              , waiver3            = b.waiver3
              , waiver4            = b.waiver4
              , lcy_chg_exch_rate  = b.lcy_chg_exch_rate
              , lcy_chg_exch_rate1 = b.lcy_chg_exch_rate1
              , lcy_chg_exch_rate2 = b.lcy_chg_exch_rate2
              , lcy_chg_exch_rate3 = b.lcy_chg_exch_rate3
              , lcy_chg_exch_rate4 = b.lcy_chg_exch_rate4
              , chg_contra_leg     = b.chg_contra_leg
              , netting_reqd       = b.netting_reqd
              , service_provider   = b.service_provider
              , bill_number        = b.bill_number
              , bill_issue_date    = b.bill_issue_date
              , consumer_no        = b.consumer_no
              , txn_status         = b.txn_status
              , cashback_amount    = b.cashback_amount
              , txn_tanked         = b.txn_tanked
              , etl_status         = 'UPDATE'
              , etl_log_date       = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( xref
            , product_code
            , branch_code
            , trn_ref_no
            , txn_acc
            , txn_ccy
            , txn_amount
            , txn_branch
            , txn_trn_code
            , ofs_acc
            , ofs_ccy
            , ofs_amount
            , ofs_branch
            , ofs_trn_code
            , exch_rate
            , lcy_amount
            , trn_dt
            , value_dt
            , dr_instrument_code
            , cr_instrument_code
            , rel_customer
            , charge_account
            , chg_gl
            , chg_ccy
            , chg_amt
            , chg_in_acy
            , chg_in_lcy
            , chg_ccy_acy_rate
            , acy_lcy_rate
            , netting_ind
            , txn_code
            , mis_head_1
            , chg_gl_1
            , chg_ccy_1
            , chg_amt_1
            , chg_in_acy_1
            , chg_in_lcy_1
            , chg_ccy_acy_rate_1
            , acy_lcy_rate_1
            , netting_ind_1
            , txn_code_1
            , mis_head_2
            , chg_gl_2
            , chg_ccy_2
            , chg_amt_2
            , chg_in_acy_2
            , chg_in_lcy_2
            , chg_ccy_acy_rate_2
            , acy_lcy_rate_2
            , netting_ind_2
            , txn_code_2
            , mis_head_3
            , chg_gl_3
            , chg_ccy_3
            , chg_amt_3
            , chg_in_acy_3
            , chg_in_lcy_3
            , chg_ccy_acy_rate_3
            , acy_lcy_rate_3
            , netting_ind_3
            , txn_code_3
            , mis_head_4
            , chg_gl_4
            , chg_ccy_4
            , chg_amt_4
            , chg_in_acy_4
            , chg_in_lcy_4
            , chg_ccy_acy_rate_4
            , acy_lcy_rate_4
            , netting_ind_4
            , txn_code_4
            , mis_head_5
            , rem_acc
            , rem_bank
            , rem_branch
            , routing_no
            , end_point
            , serial_no
            , record_stat
            , auth_stat
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , repair_reason
            , mod_no
            , scode
            , mis_head
            , narrative
            , dr_acc
            , module
            , esn
            , event_code
            , route_code
            , ft_problem
            , track_receivable
            , time_received
            , their_chgs2
            , their_chgs3
            , their_chgs4
            , their_chgs
            , their_chgs1
            , their_acc
            , their_acc1
            , their_acc2
            , their_acc3
            , their_acc4
            , lcy_exch_rate
            , tot_chg_in_tcy
            , chg_desc
            , chg_desc1
            , chg_desc2
            , chg_desc3
            , chg_desc4
            , chg_type
            , chg_type1
            , chg_type2
            , chg_type3
            , chg_type4
            , waiver
            , waiver1
            , waiver2
            , waiver3
            , waiver4
            , lcy_chg_exch_rate
            , lcy_chg_exch_rate1
            , lcy_chg_exch_rate2
            , lcy_chg_exch_rate3
            , lcy_chg_exch_rate4
            , chg_contra_leg
            , netting_reqd
            , service_provider
            , bill_number
            , bill_issue_date
            , consumer_no
            , txn_status
            , cashback_amount
            , txn_tanked
            , etl_status
            , etl_log_date)
            VALUES ( b.xref
                   , b.product_code
                   , b.branch_code
                   , b.trn_ref_no
                   , b.txn_acc
                   , b.txn_ccy
                   , b.txn_amount
                   , b.txn_branch
                   , b.txn_trn_code
                   , b.ofs_acc
                   , b.ofs_ccy
                   , b.ofs_amount
                   , b.ofs_branch
                   , b.ofs_trn_code
                   , b.exch_rate
                   , b.lcy_amount
                   , b.trn_dt
                   , b.value_dt
                   , b.dr_instrument_code
                   , b.cr_instrument_code
                   , b.rel_customer
                   , b.charge_account
                   , b.chg_gl
                   , b.chg_ccy
                   , b.chg_amt
                   , b.chg_in_acy
                   , b.chg_in_lcy
                   , b.chg_ccy_acy_rate
                   , b.acy_lcy_rate
                   , b.netting_ind
                   , b.txn_code
                   , b.mis_head_1
                   , b.chg_gl_1
                   , b.chg_ccy_1
                   , b.chg_amt_1
                   , b.chg_in_acy_1
                   , b.chg_in_lcy_1
                   , b.chg_ccy_acy_rate_1
                   , b.acy_lcy_rate_1
                   , b.netting_ind_1
                   , b.txn_code_1
                   , b.mis_head_2
                   , b.chg_gl_2
                   , b.chg_ccy_2
                   , b.chg_amt_2
                   , b.chg_in_acy_2
                   , b.chg_in_lcy_2
                   , b.chg_ccy_acy_rate_2
                   , b.acy_lcy_rate_2
                   , b.netting_ind_2
                   , b.txn_code_2
                   , b.mis_head_3
                   , b.chg_gl_3
                   , b.chg_ccy_3
                   , b.chg_amt_3
                   , b.chg_in_acy_3
                   , b.chg_in_lcy_3
                   , b.chg_ccy_acy_rate_3
                   , b.acy_lcy_rate_3
                   , b.netting_ind_3
                   , b.txn_code_3
                   , b.mis_head_4
                   , b.chg_gl_4
                   , b.chg_ccy_4
                   , b.chg_amt_4
                   , b.chg_in_acy_4
                   , b.chg_in_lcy_4
                   , b.chg_ccy_acy_rate_4
                   , b.acy_lcy_rate_4
                   , b.netting_ind_4
                   , b.txn_code_4
                   , b.mis_head_5
                   , b.rem_acc
                   , b.rem_bank
                   , b.rem_branch
                   , b.routing_no
                   , b.end_point
                   , b.serial_no
                   , b.record_stat
                   , b.auth_stat
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.repair_reason
                   , b.mod_no
                   , b.scode
                   , b.mis_head
                   , b.narrative
                   , b.dr_acc
                   , b.module
                   , b.esn
                   , b.event_code
                   , b.route_code
                   , b.ft_problem
                   , b.track_receivable
                   , b.time_received
                   , b.their_chgs2
                   , b.their_chgs3
                   , b.their_chgs4
                   , b.their_chgs
                   , b.their_chgs1
                   , b.their_acc
                   , b.their_acc1
                   , b.their_acc2
                   , b.their_acc3
                   , b.their_acc4
                   , b.lcy_exch_rate
                   , b.tot_chg_in_tcy
                   , b.chg_desc
                   , b.chg_desc1
                   , b.chg_desc2
                   , b.chg_desc3
                   , b.chg_desc4
                   , b.chg_type
                   , b.chg_type1
                   , b.chg_type2
                   , b.chg_type3
                   , b.chg_type4
                   , b.waiver
                   , b.waiver1
                   , b.waiver2
                   , b.waiver3
                   , b.waiver4
                   , b.lcy_chg_exch_rate
                   , b.lcy_chg_exch_rate1
                   , b.lcy_chg_exch_rate2
                   , b.lcy_chg_exch_rate3
                   , b.lcy_chg_exch_rate4
                   , b.chg_contra_leg
                   , b.netting_reqd
                   , b.service_provider
                   , b.bill_number
                   , b.bill_issue_date
                   , b.consumer_no
                   , b.txn_status
                   , b.cashback_amount
                   , b.txn_tanked
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'DETB_RTL_TELLER', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            lmtm_collat a
        USING (SELECT *
               FROM lmtm_collat@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                        a.liab_id = b.liab_id
                    AND a.collateral_code = b.collateral_code
                )
        WHEN MATCHED THEN
            UPDATE
            SET line_cd                = b.line_cd
              , line_serial            = b.line_serial
              , sequence_number        = b.sequence_number
              , collateral_description = b.collateral_description
              , number_of_units        = b.number_of_units
              , cap_amount             = b.cap_amount
              , collateral_value       = b.collateral_value
              , margin                 = b.margin
              , limit_contribution     = b.limit_contribution
              , start_date             = b.start_date
              , end_date               = b.end_date
              , review_date            = b.review_date
              , last_reval_price       = b.last_reval_price
              , collateral_currency    = b.collateral_currency
              , collateral_type        = b.collateral_type
              , price_code             = b.price_code
              , security_id            = b.security_id
              , maker_id               = b.maker_id
              , checker_id             = b.checker_id
              , maker_dt_stamp         = b.maker_dt_stamp
              , checker_dt_stamp       = b.checker_dt_stamp
              , record_stat            = b.record_stat
              , auth_stat              = b.auth_stat
              , mod_no                 = b.mod_no
              , once_auth              = b.once_auth
              , market_value_based     = b.market_value_based
              , lm_ccy_amt             = b.lm_ccy_amt
              , liab_br                = b.liab_br
              , reval_collat           = b.reval_collat
              , lendable_margin        = b.lendable_margin
              , fccref                 = b.fccref
              , etl_status             = 'UPDATE'
              , etl_log_date           = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( liab_id
            , line_cd
            , line_serial
            , sequence_number
            , collateral_description
            , number_of_units
            , cap_amount
            , collateral_value
            , margin
            , limit_contribution
            , start_date
            , end_date
            , review_date
            , last_reval_price
            , collateral_currency
            , collateral_type
            , price_code
            , security_id
            , maker_id
            , checker_id
            , maker_dt_stamp
            , checker_dt_stamp
            , record_stat
            , auth_stat
            , mod_no
            , once_auth
            , market_value_based
            , lm_ccy_amt
            , liab_br
            , collateral_code
            , reval_collat
            , lendable_margin
            , fccref
            , etl_status
            , etl_log_date)
            VALUES ( b.liab_id
                   , b.line_cd
                   , b.line_serial
                   , b.sequence_number
                   , b.collateral_description
                   , b.number_of_units
                   , b.cap_amount
                   , b.collateral_value
                   , b.margin
                   , b.limit_contribution
                   , b.start_date
                   , b.end_date
                   , b.review_date
                   , b.last_reval_price
                   , b.collateral_currency
                   , b.collateral_type
                   , b.price_code
                   , b.security_id
                   , b.maker_id
                   , b.checker_id
                   , b.maker_dt_stamp
                   , b.checker_dt_stamp
                   , b.record_stat
                   , b.auth_stat
                   , b.mod_no
                   , b.once_auth
                   , b.market_value_based
                   , b.lm_ccy_amt
                   , b.liab_br
                   , b.collateral_code
                   , b.reval_collat
                   , b.lendable_margin
                   , b.fccref
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'LMTM_COLLAT', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            cltm_product a
        USING (SELECT *
               FROM cltm_product@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                a.product_code = b.product_code
                )
        WHEN MATCHED THEN
            UPDATE
            SET product_desc             = b.product_desc
              , product_category         = b.product_category
              , product_end_date         = b.product_end_date
              , record_stat              = b.record_stat
              , auth_stat                = b.auth_stat
              , once_auth                = b.once_auth
              , mod_no                   = b.mod_no
              , maker_id                 = b.maker_id
              , maker_dt_stamp           = b.maker_dt_stamp
              , checker_id               = b.checker_id
              , checker_dt_stamp         = b.checker_dt_stamp
              , ccy_list                 = b.ccy_list
              , cuscat_list              = b.cuscat_list
              , branch_list              = b.branch_list
              , auto_man_rollover        = b.auto_man_rollover
              , schedule_basis           = b.schedule_basis
              , ude_rollover_basis       = b.ude_rollover_basis
              , rollover_with_interest   = b.rollover_with_interest
              , normal_rate_variance     = b.normal_rate_variance
              , maximum_rate_variance    = b.maximum_rate_variance
              , min_tenor                = b.min_tenor
              , std_tenor                = b.std_tenor
              , max_tenor                = b.max_tenor
              , tenor_unit               = b.tenor_unit
              , ignore_holidays          = b.ignore_holidays
              , move_across_month        = b.move_across_month
              , schedule_movement        = b.schedule_movement
              , rate_code_pref           = b.rate_code_pref
              , passbook_facility        = b.passbook_facility
              , track_receivable         = b.track_receivable
              , atm_facility             = b.atm_facility
              , track_receivable_aliq    = b.track_receivable_aliq
              , track_receivable_mliq    = b.track_receivable_mliq
              , liquidation_mode         = b.liquidation_mode
              , amend_past_paid_schedule = b.amend_past_paid_schedule
              , cheque_book_facility     = b.cheque_book_facility
              , cascade_schedules        = b.cascade_schedules
              , etl_status               = 'UPDATE'
              , etl_log_date             = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( product_code
            , product_desc
            , product_category
            , product_end_date
            , record_stat
            , auth_stat
            , once_auth
            , mod_no
            , maker_id
            , maker_dt_stamp
            , checker_id
            , checker_dt_stamp
            , ccy_list
            , cuscat_list
            , branch_list
            , auto_man_rollover
            , schedule_basis
            , ude_rollover_basis
            , rollover_with_interest
            , normal_rate_variance
            , maximum_rate_variance
            , min_tenor
            , std_tenor
            , max_tenor
            , tenor_unit
            , ignore_holidays
            , move_across_month
            , schedule_movement
            , rate_code_pref
            , passbook_facility
            , track_receivable
            , atm_facility
            , track_receivable_aliq
            , track_receivable_mliq
            , liquidation_mode
            , amend_past_paid_schedule
            , cheque_book_facility
            , cascade_schedules
            , liq_comp_dates_flag
            , disbursement_mode
            , recomputation_basis
            , prepmt_effective_from
            , vami_action
            , reference_no_format
            , rate_type
            , liq_back_val_sch_flag
            , int_stmt
            , allow_back_period_entry
            , draj_entries_pref
            , craj_entries_pref
            , partial_liquidation
            , aliq_reversed_pmt
            , retries_auto_liq
            , roll_by
            , vami_emi_type
            , prepay_emi_type
            , acop_emi_type
            , min_emi_amount
            , min_emi_ccy
            , adhoc_hol_treatment_reqd
            , spl_int_accrual
            , yacr_freq
            , prepayment_tbd_treatment
            , acq_type
            , product_type
            , rollover_allowed
            , cl_against_bill
            , holiday_check
            , holiday_check_mat
            , notice_day_basis
            , holiday_default_basis
            , consider_branch_holiday_sch
            , holiday_ccy_sch
            , consider_branch_holiday_mat
            , ignore_holidays_mat_val_dt
            , move_across_month_mat_val_dt
            , apply_facility_hol_ccy
            , apply_contract_hol_ccy
            , apply_local_hol_ccy
            , schedule_movement_mat_val_dt
            , apply_facility_hol_ccy_mat
            , apply_contract_hol_ccy_mat
            , apply_local_hol_ccy_mat
            , holiday_ccy_mat
            , module_code
            , roll_int_to_intcomp
            , etl_status
            , etl_log_date)
            VALUES ( b.product_code
                   , b.product_desc
                   , b.product_category
                   , b.product_end_date
                   , b.record_stat
                   , b.auth_stat
                   , b.once_auth
                   , b.mod_no
                   , b.maker_id
                   , b.maker_dt_stamp
                   , b.checker_id
                   , b.checker_dt_stamp
                   , b.ccy_list
                   , b.cuscat_list
                   , b.branch_list
                   , b.auto_man_rollover
                   , b.schedule_basis
                   , b.ude_rollover_basis
                   , b.rollover_with_interest
                   , b.normal_rate_variance
                   , b.maximum_rate_variance
                   , b.min_tenor
                   , b.std_tenor
                   , b.max_tenor
                   , b.tenor_unit
                   , b.ignore_holidays
                   , b.move_across_month
                   , b.schedule_movement
                   , b.rate_code_pref
                   , b.passbook_facility
                   , b.track_receivable
                   , b.atm_facility
                   , b.track_receivable_aliq
                   , b.track_receivable_mliq
                   , b.liquidation_mode
                   , b.amend_past_paid_schedule
                   , b.cheque_book_facility
                   , b.cascade_schedules
                   , b.liq_comp_dates_flag
                   , b.disbursement_mode
                   , b.recomputation_basis
                   , b.prepmt_effective_from
                   , b.vami_action
                   , b.reference_no_format
                   , b.rate_type
                   , b.liq_back_val_sch_flag
                   , b.int_stmt
                   , b.allow_back_period_entry
                   , b.draj_entries_pref
                   , b.craj_entries_pref
                   , b.partial_liquidation
                   , b.aliq_reversed_pmt
                   , b.retries_auto_liq
                   , b.roll_by
                   , b.vami_emi_type
                   , b.prepay_emi_type
                   , b.acop_emi_type
                   , b.min_emi_amount
                   , b.min_emi_ccy
                   , b.adhoc_hol_treatment_reqd
                   , b.spl_int_accrual
                   , b.yacr_freq
                   , b.prepayment_tbd_treatment
                   , b.acq_type
                   , b.product_type
                   , b.rollover_allowed
                   , b.cl_against_bill
                   , b.holiday_check
                   , b.holiday_check_mat
                   , b.notice_day_basis
                   , b.holiday_default_basis
                   , b.consider_branch_holiday_sch
                   , b.holiday_ccy_sch
                   , b.consider_branch_holiday_mat
                   , b.ignore_holidays_mat_val_dt
                   , b.move_across_month_mat_val_dt
                   , b.apply_facility_hol_ccy
                   , b.apply_contract_hol_ccy
                   , b.apply_local_hol_ccy
                   , b.schedule_movement_mat_val_dt
                   , b.apply_facility_hol_ccy_mat
                   , b.apply_contract_hol_ccy_mat
                   , b.apply_local_hol_ccy_mat
                   , b.holiday_ccy_mat
                   , b.module_code
                   , b.roll_int_to_intcomp
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'CLTM_PRODUCT', 'RUN_SPECIFIC_DAY', row_count);

        MERGE
        INTO
            lmtm_limits a
        USING (SELECT *
               FROM lmtm_limits@"Corepro.Localdomain"
               WHERE TRUNC(checker_dt_stamp) = TRUNC(run_date))
            b
        ON
            (
                        a.liab_id = b.liab_id
                    AND a.line_cd = b.line_cd
                    AND a.line_serial = b.line_serial
                )
        WHEN MATCHED THEN
            UPDATE
            SET lm_temple               = b.lm_temple
              , main_line               = b.main_line
              , ccy_restriction         = b.ccy_restriction
              , line_currency           = b.line_currency
              , revolving_line          = b.revolving_line
              , line_start_date         = b.line_start_date
              , line_expiry_date        = b.line_expiry_date
              , last_new_util_date      = b.last_new_util_date
              , availability_flag       = b.availability_flag
              , internal_remarks        = b.internal_remarks
              , limit_amount            = b.limit_amount
              , collateral_contribution = b.collateral_contribution
              , uncollected_funds_limit = b.uncollected_funds_limit
              , reporting_amount        = b.reporting_amount
              , available_amount        = b.available_amount
              , date_of_first_od        = b.date_of_first_od
              , date_of_last_od         = b.date_of_last_od
              , amount_utilised_today   = b.amount_utilised_today
              , amount_reinstated_today = b.amount_reinstated_today
              , uncollected_amount      = b.uncollected_amount
              , excess_tenor            = b.excess_tenor
              , maker_id                = b.maker_id
              , checker_id              = b.checker_id
              , maker_dt_stamp          = b.maker_dt_stamp
              , checker_dt_stamp        = b.checker_dt_stamp
              , matured_util            = b.matured_util
              , record_stat             = b.record_stat
              , auth_stat               = b.auth_stat
              , mod_no                  = b.mod_no
              , once_auth               = b.once_auth
              , utilisation             = b.utilisation
              , liab_br                 = b.liab_br
              , collateral_pct          = b.collateral_pct
              , netting_required        = b.netting_required
              , brn                     = b.brn
              , lmt_amt_basis           = b.lmt_amt_basis
              , unadvised               = b.unadvised
              , transfer_amount         = b.transfer_amount
              , interest_reqd           = b.interest_reqd
              , interest_calc_acc       = b.interest_calc_acc
              , dsp_eff_line_amount     = b.dsp_eff_line_amount
              , etl_status              = 'UPDATE'
              , etl_log_date            = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT
            ( liab_id
            , line_cd
            , line_serial
            , lm_temple
            , main_line
            , ccy_restriction
            , line_currency
            , revolving_line
            , line_start_date
            , line_expiry_date
            , last_new_util_date
            , availability_flag
            , internal_remarks
            , limit_amount
            , collateral_contribution
            , uncollected_funds_limit
            , reporting_amount
            , available_amount
            , date_of_first_od
            , date_of_last_od
            , amount_utilised_today
            , amount_reinstated_today
            , uncollected_amount
            , excess_tenor
            , maker_id
            , checker_id
            , maker_dt_stamp
            , checker_dt_stamp
            , matured_util
            , record_stat
            , auth_stat
            , mod_no
            , once_auth
            , utilisation
            , liab_br
            , collateral_pct
            , netting_required
            , brn
            , lmt_amt_basis
            , unadvised
            , transfer_amount
            , interest_reqd
            , interest_calc_acc
            , dsp_eff_line_amount
            , etl_status
            , etl_log_date)
            VALUES ( b.liab_id
                   , b.line_cd
                   , b.line_serial
                   , b.lm_temple
                   , b.main_line
                   , b.ccy_restriction
                   , b.line_currency
                   , b.revolving_line
                   , b.line_start_date
                   , b.line_expiry_date
                   , b.last_new_util_date
                   , b.availability_flag
                   , b.internal_remarks
                   , b.limit_amount
                   , b.collateral_contribution
                   , b.uncollected_funds_limit
                   , b.reporting_amount
                   , b.available_amount
                   , b.date_of_first_od
                   , b.date_of_last_od
                   , b.amount_utilised_today
                   , b.amount_reinstated_today
                   , b.uncollected_amount
                   , b.excess_tenor
                   , b.maker_id
                   , b.checker_id
                   , b.maker_dt_stamp
                   , b.checker_dt_stamp
                   , b.matured_util
                   , b.record_stat
                   , b.auth_stat
                   , b.mod_no
                   , b.once_auth
                   , b.utilisation
                   , b.liab_br
                   , b.collateral_pct
                   , b.netting_required
                   , b.brn
                   , b.lmt_amt_basis
                   , b.unadvised
                   , b.transfer_amount
                   , b.interest_reqd
                   , b.interest_calc_acc
                   , b.dsp_eff_line_amount
                   , 'INSERT NEW'
                   , SYSDATE);
        row_count := SQL%ROWCOUNT;
        INSERT
        INTO etl_log
            (log_date, status, source, table_name, action, row_affected)
        VALUES (SYSDATE, 'SUCCESS', 'CORE', 'LMTM_LIMITS', 'RUN_SPECIFIC_DAY', row_count);

    EXCEPTION
        WHEN
            OTHERS THEN
            ROLLBACK;
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT
            INTO error_log (error_proc, error_code, error_message, error_line, error_date)
            VALUES ( 'proc_etl_core_run_specific_day', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;
    END;
END;