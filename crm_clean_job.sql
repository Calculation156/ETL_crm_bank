CREATE PACKAGE pkg_etl_crm_clean_job AS
    PROCEDURE clean_crm_tables;

END;


CREATE PACKAGE BODY pkg_etl_crm_clean_job AS
    PROCEDURE clean_crm_tables
    AS
    BEGIN

        -- TODO: Clean crm_partner
        DELETE
        FROM crm_partner
        WHERE partner_id IN (WITH data AS
            (SELECT cif_no FROM crm_partner GROUP BY cif_no HAVING COUNT(cif_no) > 1)
                                , redundant AS (SELECT ROW_NUMBER() OVER (ORDER BY partner_id DESC) row_num, partner_id
                                                FROM crm_partner
                                                WHERE cif_no IN (SELECT cif_no FROM data))
                             SELECT partner_id
                             FROM redundant
                             WHERE row_num > 1);

        COMMIT;

        -- TODO: Clean crm_partner_bank_account_casa
        DELETE FROM crm_partner_bank_account_casa WHERE account_no = 'undefined';
        DELETE FROM crm_partner_bank_account_casa WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner);

        DELETE
        FROM crm_partner_bank_account_casa
        WHERE account_casa_id IN (WITH data AS
            (SELECT account_no FROM crm_partner_bank_account_casa GROUP BY account_no HAVING COUNT(account_no) > 1)
                                      , redundant AS (SELECT ROW_NUMBER() OVER (ORDER BY account_casa_id) row_num, account_casa_id
                                                FROM crm_partner_bank_account_casa
                                                WHERE account_no IN (SELECT account_no FROM data))
                                  SELECT account_casa_id
                                  FROM redundant
                                  WHERE row_num > 1);

        COMMIT;

        -- TODO: Clean crm_partner_bank_account_loan
        DELETE FROM crm_partner_bank_account_loan WHERE account_no = 'undefined';
        DELETE FROM crm_partner_bank_account_loan WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner);

        DELETE
        FROM crm_partner_bank_account_loan
        WHERE account_loan_id IN (WITH data AS
            (SELECT account_no FROM crm_partner_bank_account_loan GROUP BY account_no HAVING COUNT(account_no) > 1)
                                      , redundant AS (SELECT ROW_NUMBER() OVER (ORDER BY account_loan_id) row_num, account_loan_id
                                                FROM crm_partner_bank_account_loan
                                                WHERE account_no IN (SELECT account_no FROM data))
                                  SELECT account_loan_id
                                  FROM redundant
                                  WHERE row_num > 1);

        COMMIT;

        -- TODO: Clean crm_partner_bank_account_fd
        DELETE FROM crm_partner_bank_account_fd WHERE account_no = 'undefined';
        DELETE FROM crm_partner_bank_account_fd WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner);

        DELETE
        FROM crm_partner_bank_account_fd
        WHERE account_fd_id IN (WITH data AS
            (SELECT account_no FROM crm_partner_bank_account_fd GROUP BY account_no HAVING COUNT(account_no) > 1)
                                      , redundant AS (SELECT ROW_NUMBER() OVER (ORDER BY account_fd_id) row_num, account_fd_id
                                                FROM crm_partner_bank_account_fd
                                                WHERE account_no IN (SELECT account_no FROM data))
                                  SELECT account_fd_id
                                  FROM redundant
                                  WHERE row_num > 1);

        COMMIT;

        -- TODO: Clean crm_partner_bank_account_overdraft
        DELETE FROM crm_partner_bank_account_overdraft WHERE account_no = 'undefined';
        DELETE FROM crm_partner_bank_account_overdraft WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner);

        DELETE
        FROM crm_partner_bank_account_overdraft
        WHERE account_overdraft_id IN (WITH data AS
            (SELECT account_no FROM crm_partner_bank_account_overdraft GROUP BY account_no HAVING COUNT(account_no) > 1)
                                      , redundant AS (SELECT ROW_NUMBER() OVER (ORDER BY account_overdraft_id) row_num, account_overdraft_id
                                                FROM crm_partner_bank_account_overdraft
                                                WHERE account_no IN (SELECT account_no FROM data))
                                  SELECT account_overdraft_id
                                  FROM redundant
                                  WHERE row_num > 1);

        COMMIT;

        -- TODO: Clean crm_partner_bank_account_credit_card
        DELETE FROM crm_partner_bank_account_credit_card WHERE card_no = 'undefined';
        DELETE FROM crm_partner_bank_account_credit_card WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner);

        DELETE
        FROM crm_partner_bank_account_credit_card
        WHERE account_credit_card_id IN (WITH data AS
            (SELECT card_no FROM crm_partner_bank_account_credit_card GROUP BY card_no HAVING COUNT(card_no) > 1)
                                      , redundant AS (SELECT ROW_NUMBER() OVER (ORDER BY account_credit_card_id) row_num, account_credit_card_id
                                                FROM crm_partner_bank_account_credit_card
                                                WHERE card_no IN (SELECT card_no FROM data))
                                  SELECT account_credit_card_id
                                  FROM redundant
                                  WHERE row_num > 1);

        COMMIT;

        -- TODO: Clean crm_partner_collateral
        DELETE FROM crm_partner_collateral WHERE collateral_code = 'undefined';
        DELETE FROM crm_partner_collateral WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner);

        DELETE
        FROM crm_partner_collateral
        WHERE collateral_id IN (WITH data AS
            (SELECT collateral_code FROM crm_partner_collateral GROUP BY collateral_code HAVING COUNT(collateral_code) > 1)
                                      , redundant AS (SELECT ROW_NUMBER() OVER (ORDER BY collateral_id) row_num, collateral_id
                                                FROM crm_partner_collateral
                                                WHERE collateral_code IN (SELECT collateral_code FROM data))
                                  SELECT collateral_id
                                  FROM redundant
                                  WHERE row_num > 1);

        COMMIT;

        -- TODO: Clean crm_partner_contract
        DELETE FROM crm_partner_contract WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner_bank_account_casa);
        DELETE FROM crm_partner_contract WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner_bank_account_loan);
        DELETE FROM crm_partner_contract WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner_bank_account_fd);
        DELETE FROM crm_partner_contract WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner_bank_account_overdraft);
        DELETE FROM crm_partner_contract WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner_collateral);
        DELETE FROM crm_partner_contract WHERE partner_id NOT IN (SELECT partner_id FROM crm_partner_bank_account_credit_card);

        COMMIT;

        -- TODO: Clean crm_partner_contract_assign
        DELETE
        FROM crm_partner_contract_assign
        WHERE assign_id IN (WITH data AS
            (SELECT contract_no FROM crm_partner_contract_assign GROUP BY contract_no, kpi_type_id HAVING COUNT(contract_no) > 1)
                                      , redundant AS (SELECT ROW_NUMBER() OVER (ORDER BY assign_id) row_num, assign_id
                                                FROM crm_partner_contract_assign
                                                WHERE contract_no IN (SELECT contract_no FROM data))
                                  SELECT assign_id
                                  FROM redundant
                                  WHERE row_num > 1);

        COMMIT;

    END;
END;