# CRM ETL — pkg_etl_crm_job_daily (Overview & Runbook)

## Purpose
This package performs the daily CRM ETL that ingests core/staging data (via DB links) into CRM tables. It:
- logs ETL start,
- cleans staging/temp data,
- runs domain ETL procedures (partners, accounts, contracts, transactions, collateral, holdings),
- logs per-table row counts and errors.

## Scope
Files: `crm_etl.sql`, `crm_clean_job.sql`  
Main package: `pkg_etl_crm_job_daily` (package body)  
Clean package: `pkg_etl_crm_clean_job.clean_crm_tables`

## Scheduling
Expected daily run (nightly). The package inserts `etl_log` with status `'START_DAILY'` at start. Each sub-procedure writes `etl_log` entries after processing.

## High-level flow (execution sequence)
1. Insert START_DAILY into `etl_log` and COMMIT.
2. `pkg_etl_crm_clean_job.clean_crm_tables` — remove duplicates/orphans/hard-coded invalid rows.
3. Call sub-procedures (in order):
   - `proc_crm_partner_daily`
   - `proc_crm_boundary_address_daily`
   - `proc_etl_bank_loan_daily`
   - `proc_etl_bank_casa_daily`
   - `proc_etl_bank_fd_daily`
   - `proc_etl_bank_ovd_daily`
   - `proc_etl_bank_lcbg_daily`
   - `proc_etl_bank_credit_card_daily`
   - `proc_etl_bank_ibmb_daily` (IB/MB)
   - `proc_etl_bank_transaction_daily`
   - `proc_etl_bank_collateral_daily`
   - `proc_crm_partner_holding_daily`
   - `proc_crm_partner_contract_daily`
   - `proc_crm_partner_contract_assign_daily`

## Per-procedure summary (brief)

- proc_crm_partner_daily
  - Merge core customer view (`vw_customer`) into `crm_partner`.
  - Inserts new corporate/individual customers; updates identity/contact fields.
  - Uses multiple MERGEs for different matching keys (id, registration_code, mobile, recent updates).

- proc_crm_boundary_address_daily
  - Syncs `crm_partner_address` from `vw_customer`.
  - Syncs `crm_boundary_address` from collateral staging (`lmtm_collat`).

- proc_etl_bank_loan_daily
  - Merge loan accounts (`crm_partner_bank_account_loan`) from loan views/staging.
  - This mean to update **loan information** to loan contract
  - Calculates `lcy_curr_balance` using `crmstaging.cytm_rates` when currency != VND.

- proc_etl_bank_casa_daily
  - Merge CASA accounts (`crm_partner_bank_account_casa`) from `vw_sttm_cust_account`.
  - Secondary merge updates rate and lcy balance from `bo_rptw_deposit_new`.

- proc_etl_bank_fd_daily
  - Merge fixed deposits (`crm_partner_bank_account_fd`) and update rates from daily backup.

- proc_etl_bank_ovd_daily
  - Merge overdraft accounts (`crm_partner_bank_account_overdraft`) from multiple staging sources.
  - Sets secured/unsecured flags; updates terms and rate information.

- proc_etl_bank_lcbg_daily
  - Merge Letters of Credit / Bank Guarantees into `crm_partner_bank_account_lcbg`.
  - Computes lcy balances using exchange rates.

- proc_etl_bank_credit_card_daily
  - Merge credit card info into `crm_partner_bank_account_credit_card`.
  - Post-updates card type codes using PAN prefixes.

- proc_etl_bank_ibmb_daily
  - Merge internet/mobile banking accounts into `crm_partner_bank_account_banking`.
  - Flags e-commerce/digital wallet usage and computes transaction limits.

- proc_etl_bank_transaction_daily
  - Deletes today's transactions and inserts previous-day transactions into `crm_partner_transaction`.

- proc_etl_bank_collateral_daily
  - Merge collateral (`crm_partner_collateral`) with mapping to `crm_base_meta_select` for collateral type.

- proc_crm_partner_holding_daily
  - Builds `crm_partner_holding` summary (product holdings flags) from `base_partner_holding` staging.

- proc_crm_partner_contract_daily
  - Creates/updates `crm_partner_contract` rows for loans, CASA, cards, FD, LCBG, IB/MB and overdrafts.
  - Contains many business-rule updates to set `contract_type_code`.

- proc_crm_partner_contract_assign_daily
  - Assigns contracts to users (kpi/source mapping) using several staging joins and `crm_auth_user` lookups.

## Data sources and DB links
Common staging sources (DB link suffix shown in SQL):
- crmstaging.vw_sttm_cust_account@"Dbstaging580.Localdomain"
- crmstaging.bo_rptw_deposit_new@"Dbstaging580.Localdomain"
- crmstaging.cytm_rates@"Dbstaging580.Localdomain"
- crmstaging.mt_ib_customer@"Dbstaging580.Localdomain"
- crmstaging.mt_ib_ecom_register@"Dbstaging580.Localdomain"
- crmstaging.mt_ib_momo_register@"Dbstaging580.Localdomain"
- crmstaging.vw_transaction@"Dbstaging580.Localdomain"
- crmstaging.lmtm_collat@"Dbstaging580.Localdomain"
- crmstaging.vw_lcbg@"Dbstaging580.Localdomain"
- crmstaging.card@"Dbstaging580.Localdomain"
- crmstaging.cltb_account_master@"Dbstaging580.Localdomain"
- crmstaging.* other staging views used across procedures

Target CRM tables (non-exhaustive)
- crm_partner
- crm_partner_address
- crm_boundary_address
- crm_partner_bank_account_casa
- crm_partner_bank_account_loan
- crm_partner_bank_account_fd
- crm_partner_bank_account_overdraft
- crm_partner_bank_account_lcbg
- crm_partner_bank_account_credit_card
- crm_partner_bank_account_banking
- crm_partner_transaction
- crm_partner_collateral
- crm_partner_holding
- crm_partner_contract
- crm_partner_contract_assign
- etl_log, error_log

## Logging & error handling
- Each procedure captures exceptions (WHEN OTHERS), ROLLBACKs, writes `error_log` with proc name, SQLCODE, SQLERRM and backtrace, then COMMITs.
- Each successful domain proc inserts into `etl_log` with `table_name` and `row_affected`.
- Top-level `excute_etl_crm_job_daily` currently writes only START_DAILY; it lacks an END/SUCCESS/FAIL log and global exception handling.

## Key operational notes / risks
- Many COMMITs inside procedures — desirable for ETL but can make rollback/recovery coarse.
- Several MERGE sources join to existing target table aliases inside USING clause — redundant and may affect execution plan.
- Filtering by TRUNC(etl_log_date) = TRUNC(SYSDATE) assumes staging timestamps align with run date; late-arriving rows may be missed.
- Use of NOT IN (...) appears in a few places; prefer NOT EXISTS to avoid NULL pitfalls.
- Large deletes (clean job) should be batched to avoid long transactions and excessive undo.
- DB links create dependency/latency and can fail; monitor link availability.
- No top-level failure log entry or retry mechanism — consider adding.

## Recommended documentation artifacts (to add)
- docs/etl_crm.md (this file)
- docs/data-dictionary.csv or .xlsx — list every important field (partner_id, cif_no, account_no, contract_no, collateral_code, etc.) with description, source, owner.
- docs/procedure-inventory.md — full list of procedures with input, output and last-modified.
- diagrams/etl_flow.png — sequence diagram of procedure order and major data flows.
- docs/runbook.md — concise run/verify/recovery checklist.

## Minimal runbook / how to run manually
1. Connect to target DB as ETL user in Windows Terminal / SQL*Plus / SQL Developer.
2. Optional: validate DB links reachable: SELECT 1 FROM dual@Dbstaging580.Localdomain;
3. Start run: EXEC pkg_etl_crm_job_daily.excute_etl_crm_job_daily(NULL);
4. Monitor `etl_log` and `error_log`:
   - SELECT * FROM etl_log WHERE TRUNC(log_date) = TRUNC(SYSDATE) ORDER BY log_date;
   - SELECT * FROM error_log WHERE TRUNC(error_date) = TRUNC(SYSDATE) ORDER BY error_date;
5. If a domain proc fails, inspect `error_log`, fix data or staging issue, then re-run the single procedure (e.g., EXEC pkg_etl_crm_job_daily.proc_etl_bank_casa_daily;).

## Validation queries (examples)
- Count rows updated per table:
  - SELECT table_name, SUM(row_affected) FROM etl_log WHERE TRUNC(log_date) = TRUNC(SYSDATE) GROUP BY table_name;
- Spot-check partner data:
  - SELECT * FROM crm_partner WHERE cif_no IS NULL OR id_number IS NULL FETCH FIRST 20 ROWS ONLY;
- Transactions completeness for previous day:
  - SELECT COUNT(*) FROM crm_partner_transaction WHERE TRUNC(created_date) = TRUNC(SYSDATE - 1);

## Known TODOs / improvement opportunities
- Add top-level exception handling in `excute_etl_crm_job_daily` to log END/FAIL and duration.
- Reduce redundant COMMITs where possible; batch large deletes in clean job.
- Replace legacy outer join (+) and comma joins with explicit ANSI JOINs for clarity and maintainability.
- Consolidate duplicate-removal logic in `pkg_etl_crm_clean_job` using ROW_NUMBER() over partitions and batch deletes.
- Create a data dictionary and glossary (IBMB = Internet Banking & Mobile Banking).
- Consider foreign keys or soft constraints for referential integrity where business semantics allow.

## Contacts / ownership
- ETL package owner: add team/individual name & contact here.
- Data stewards: core systems, payments, cards, collateral teams (fill in internal contacts).

## File references
- Active script: `e:\HTC\VRB BANK project\ETL script\crm_etl.sql`
- Clean package: `e:\HTC\VRB BANK project\ETL script\crm_clean_job.sql`
- Add docs in repo path: `e:\HTC\VRB BANK project\ETL script\docs\`

----
Notes: this document is a concise developer-runbook; fill data dictionary and contact info before handover.