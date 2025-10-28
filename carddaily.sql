CREATE OR REPLACE PACKAGE BODY package_etl_card_sync_job
AS
    PROCEDURE excute_etl_card_sync_job(
        t VARCHAR2
    )
    AS
        error_code    VARCHAR2(10);
        error_message VARCHAR2(512);
        row_count     NUMBER;
        sys_date      DATE;
    BEGIN

        -- SYNC tcard TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE tcard';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO tcard ( branch, branchpart, pan, mbr, release, pinoffset, idclient, crd_typ, crd_stat, createdate
                   , createclerkcode, updatedate, updateclerkcode, updatesysdate, signstat, orderdate, canceldate
                   , makingdate, distributiondate, closedate, currencyno, remark, insure, remake, cvv, cvv2
                   , makingpriority, insuredate, parentpan, parentmbr, grplimit, finprofile, pinblock, cardproduct, ipvv
                   , cnsscheme, cnschannel, cnsaddress, nameoncard, createsysdate, closesysdate, remakedisable
                   , risklevel, updaterevision, ecstatus, objectuid, externalid, f4dbc, plastictype, ecsettings
                   , grplimitint, useperssett, reissued, mainpan, mainmbr, grpcounter, etustatus, etunewchain
                   , supplementary, guid, etl_status, etl_log_date)
        SELECT branch
             , branchpart
             , pan
             , mbr
             , release
             , pinoffset
             , idclient
             , crd_typ
             , crd_stat
             , createdate
             , createclerkcode
             , updatedate
             , updateclerkcode
             , updatesysdate
             , signstat
             , orderdate
             , canceldate
             , makingdate
             , distributiondate
             , closedate
             , currencyno
             , remark
             , insure
             , remake
             , cvv
             , cvv2
             , makingpriority
             , insuredate
             , parentpan
             , parentmbr
             , grplimit
             , finprofile
             , pinblock
             , cardproduct
             , ipvv
             , cnsscheme
             , cnschannel
             , cnsaddress
             , nameoncard
             , createsysdate
             , closesysdate
             , remakedisable
             , risklevel
             , updaterevision
             , ecstatus
             , objectuid
             , externalid
             , f4dbc
             , plastictype
             , ecsettings
             , grplimitint
             , useperssett
             , reissued
             , mainpan
             , mainmbr
             , grpcounter
             , etustatus
             , etunewchain
             , supplementary
             , guid
             , 'INSERT NEW'
             , SYSDATE
        FROM a4m.tcard@"Cmspro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM tcard;
        SELECT DISTINCT(etl_log_date) INTO sys_date FROM tcard;

        IF (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE)) THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'tcard', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'tcard', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC tacc2card TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE tacc2card';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO tacc2card( branch, accountno, pan, mbr, acct_typ, acct_stat, description, limitgrpid, etl_status
                      , etl_log_date)
        SELECT branch
             , accountno
             , pan
             , mbr
             , acct_typ
             , acct_stat
             , description
             , limitgrpid
             , 'INSERT NEW'
             , SYSDATE
        FROM a4m.tacc2card@"Cmspro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM tacc2card;
        SELECT DISTINCT(etl_log_date) INTO sys_date FROM tacc2card;

        IF (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE)) THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'tacc2card', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'tacc2card', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC tclientpersone TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE tclientpersone';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO tclientpersone( branch, idclient, fio, countryres, sex, latfio, birthday, birthfio, birthplace, foreignpass
                           , foreignexp, countryreg, regionreg, cityreg, zipreg, addressreg, countrylive, regionlive
                           , citylive, ziplive, address, countrycont, regioncont, citycont, zipcont, addresscont, phone
                           , fax, email, mobilephone, pager, company, office, tabno, jobtitle, jobphone, startjobtime
                           , prevjob, salary, inn, family, education, visano, visareg, visaexp, secretquery
                           , secretanswer, additional, clerkcreate, clerkmodify, datecreate, datemodify, occupation, vip
                           , insider, resident, objrestricted, statementpath, cnsscheme, cnschannel, cnsaddress, title
                           , personalcode, statementlang, riskgroup, externalid, affiliate, streetreg, housereg
                           , buildingreg, framereg, flatreg, streetlive, houselive, buildinglive, framelive, flatlive
                           , streetcont, housecont, buildingcont, framecont, flatcont, deathdate, bankrupt, firstname
                           , middlename, lastname, categoryid, grplimit, grplimitint, limitcurrency, latfirstname
                           , latmiddlename, latlastname, grpcounters, personaldataexist, dateofcompletion, shareholder
                           , etl_status, etl_log_date)
        SELECT branch
             , idclient
             , fio
             , countryres
             , sex
             , latfio
             , birthday
             , birthfio
             , birthplace
             , foreignpass
             , foreignexp
             , countryreg
             , regionreg
             , cityreg
             , zipreg
             , addressreg
             , countrylive
             , regionlive
             , citylive
             , ziplive
             , address
             , countrycont
             , regioncont
             , citycont
             , zipcont
             , addresscont
             , phone
             , fax
             , email
             , mobilephone
             , pager
             , company
             , office
             , tabno
             , jobtitle
             , jobphone
             , startjobtime
             , prevjob
             , salary
             , inn
             , family
             , education
             , visano
             , visareg
             , visaexp
             , secretquery
             , secretanswer
             , additional
             , clerkcreate
             , clerkmodify
             , datecreate
             , datemodify
             , occupation
             , vip
             , insider
             , resident
             , objrestricted
             , statementpath
             , cnsscheme
             , cnschannel
             , cnsaddress
             , title
             , personalcode
             , statementlang
             , riskgroup
             , externalid
             , affiliate
             , streetreg
             , housereg
             , buildingreg
             , framereg
             , flatreg
             , streetlive
             , houselive
             , buildinglive
             , framelive
             , flatlive
             , streetcont
             , housecont
             , buildingcont
             , framecont
             , flatcont
             , deathdate
             , bankrupt
             , firstname
             , middlename
             , lastname
             , categoryid
             , grplimit
             , grplimitint
             , limitcurrency
             , latfirstname
             , latmiddlename
             , latlastname
             , grpcounters
             , personaldataexist
             , dateofcompletion
             , shareholder
             , 'INSERT NEW'
             , SYSDATE
        FROM a4m.tclientpersone@"Cmspro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM tclientpersone;
        SELECT DISTINCT(etl_log_date) INTO sys_date FROM tclientpersone;

        IF (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE)) THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'tclientpersone', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'tclientpersone', 'SYNC', row_count);
            COMMIT;
        END IF;


        -- SYNC treferencecardproduct TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE treferencecardproduct';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO treferencecardproduct( branch, code, ficode, prefix, name, period, servicecode, pvki, accgroupid
                                  , checkmethod, grplimitid, makecvv, makecvv2, finprofile, useexpiration, checkmethod2
                                  , state, status, finprofilecms, acceptdomain, cardtype, creditdebit, ps, copylimit
                                  , copyprofile, expdatetype, expday, cns, userattributes, remakestate, remakestatus
                                  , branchpart, generatorstrid, ecommerce, remakeecommerce, remakenameoncard
                                  , remakeissuepriority, remakecardrisklevel, inactive, ident, virtual
                                  , extgeneratorstrid, objectuid, copyremakeecommerce, copyremakeplastictype, ecsettings
                                  , remakeecsettings, grplimitid2, copylimit2, ecuseperssett, remakeecuseperssett
                                  , createdate, updatedate, createclerk, updateclerk, chipcapable, magneticstripecapable
                                  , contactlesscapable, cptype, inherittype, grpcounterid, copycounter, copyexpdate
                                  , etustatus, remakeetustatus, copyetustatus, cardsortdefinemethod, etl_status
                                  , etl_log_date)
        SELECT branch
             , code
             , ficode
             , prefix
             , name
             , period
             , servicecode
             , pvki
             , accgroupid
             , checkmethod
             , grplimitid
             , makecvv
             , makecvv2
             , finprofile
             , useexpiration
             , checkmethod2
             , state
             , status
             , finprofilecms
             , acceptdomain
             , cardtype
             , creditdebit
             , ps
             , copylimit
             , copyprofile
             , expdatetype
             , expday
             , cns
             , userattributes
             , remakestate
             , remakestatus
             , branchpart
             , generatorstrid
             , ecommerce
             , remakeecommerce
             , remakenameoncard
             , remakeissuepriority
             , remakecardrisklevel
             , inactive
             , ident
             , virtual
             , extgeneratorstrid
             , objectuid
             , copyremakeecommerce
             , copyremakeplastictype
             , ecsettings
             , remakeecsettings
             , grplimitid2
             , copylimit2
             , ecuseperssett
             , remakeecuseperssett
             , createdate
             , updatedate
             , createclerk
             , updateclerk
             , chipcapable
             , magneticstripecapable
             , contactlesscapable
             , cptype
             , inherittype
             , grpcounterid
             , copycounter
             , copyexpdate
             , etustatus
             , remakeetustatus
             , copyetustatus
             , cardsortdefinemethod
             , 'INSERT NEW'
             , SYSDATE
        FROM a4m.treferencecardproduct@"Cmspro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM treferencecardproduct;
        SELECT DISTINCT(etl_log_date) INTO sys_date FROM treferencecardproduct;

        IF (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE)) THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'treferencecardproduct', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'treferencecardproduct', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC taccount TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE taccount';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO taccount( branch, accountno, accounttype, currencyno, idclient, remain, available, overdraft, noplan
                     , createdate, createclerkcode, updatedate, updateclerkcode, updatesysdate, stat, acct_typ
                     , acct_stat, closedate, remark, debitreserve, creditreserve, lowremain, branchpart, limitgrpid
                     , objectuid, internallimitsgroupsysid, countergrpid, etl_status, etl_log_date)
        SELECT branch
             , accountno
             , accounttype
             , currencyno
             , idclient
             , remain
             , available
             , overdraft
             , noplan
             , createdate
             , createclerkcode
             , updatedate
             , updateclerkcode
             , updatesysdate
             , stat
             , acct_typ
             , acct_stat
             , closedate
             , remark
             , debitreserve
             , creditreserve
             , lowremain
             , branchpart
             , limitgrpid
             , objectuid
             , internallimitsgroupsysid
             , countergrpid
             , 'INSERT NEW'
             , SYSDATE
        FROM a4m.taccount@"Cmspro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM taccount;
        SELECT DISTINCT(etl_log_date) INTO sys_date FROM taccount;

        IF (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE)) THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'taccount', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'taccount', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC treferenceretailer TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE treferenceretailer';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO treferenceretailer( branch, code, ficode, ident, name, accountgroupcode, external_name, external_city
                               , external_country, external_zipcode, external_siccode, external_cpscode, panentrytype
                               , holderauthtype, bankname, bankaccount, bic, externalaccount, inn, country, city
                               , address, phones, email, bossfio, servicepersonefio, country_code, havetransit, acc_act
                               , acc_pass, postalcode, region_code, city_code, fax, code_client, createdate, updatedate
                               , createclerk, updateclerk, note, profile, branchpart, street, house, building, frame
                               , flat, isprototype, prototype, inactive, location, merchantid, objectuid, parentcode
                               , parentinactive, external_ident, limitgrp, limitcurrency, reimbursement, etl_status
                               , etl_log_date)
        SELECT branch
             , code
             , ficode
             , ident
             , name
             , accountgroupcode
             , external_name
             , external_city
             , external_country
             , external_zipcode
             , external_siccode
             , external_cpscode
             , panentrytype
             , holderauthtype
             , bankname
             , bankaccount
             , bic
             , externalaccount
             , inn
             , country
             , city
             , address
             , phones
             , email
             , bossfio
             , servicepersonefio
             , country_code
             , havetransit
             , acc_act
             , acc_pass
             , postalcode
             , region_code
             , city_code
             , fax
             , code_client
             , createdate
             , updatedate
             , createclerk
             , updateclerk
             , note
             , profile
             , branchpart
             , street
             , house
             , building
             , frame
             , flat
             , isprototype
             , prototype
             , inactive
             , location
             , merchantid
             , objectuid
             , parentcode
             , parentinactive
             , external_ident
             , limitgrp
             , limitcurrency
             , reimbursement
             , 'INSERT NEW'
             , SYSDATE
        FROM a4m.treferenceretailer@"Cmspro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM treferenceretailer;
        SELECT DISTINCT(etl_log_date) INTO sys_date FROM treferenceretailer;

        IF (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE)) THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'treferenceretailer', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'treferenceretailer', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC treferenceterminal TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE treferenceterminal';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO treferenceterminal( branch, code, ficode, retailercode, ident, name, device, accountgroupcode
                               , external_name, external_city, external_country, external_zipcode, external_siccode
                               , external_cpscode, panentrytype, holderauthtype, poscapability, country, city, address
                               , isdefault, country_code, region_code, city_code, createdate, updatedate, createclerk
                               , updateclerk, location, note, primarycurrency, branchpart, timeoffset, pincapturecap
                               , inactive, street, house, building, frame, flat, acqgroupid, isprototype, prototype
                               , objectuid, batchno, longitude, latitude, parentinactive, selfservice
                               , contactlesscapable, ecommercecapable, motocapable, cashincapable, parentcode, limitgrp
                               , limitcurrency, mobilepos, pinpadcapable, serialnumber, transactionsunloadingdelay
                               , poscategory, createoperdate, entrycapsextended, etl_status, etl_log_date)
        SELECT branch
             , code
             , ficode
             , retailercode
             , ident
             , name
             , device
             , accountgroupcode
             , external_name
             , external_city
             , external_country
             , external_zipcode
             , external_siccode
             , external_cpscode
             , panentrytype
             , holderauthtype
             , poscapability
             , country
             , city
             , address
             , isdefault
             , country_code
             , region_code
             , city_code
             , createdate
             , updatedate
             , createclerk
             , updateclerk
             , location
             , note
             , primarycurrency
             , branchpart
             , timeoffset
             , pincapturecap
             , inactive
             , street
             , house
             , building
             , frame
             , flat
             , acqgroupid
             , isprototype
             , prototype
             , objectuid
             , batchno
             , longitude
             , latitude
             , parentinactive
             , selfservice
             , contactlesscapable
             , ecommercecapable
             , motocapable
             , cashincapable
             , parentcode
             , limitgrp
             , limitcurrency
             , mobilepos
             , pinpadcapable
             , serialnumber
             , transactionsunloadingdelay
             , poscategory
             , createoperdate
             , entrycapsextended
             , 'INSERT NEW'
             , SYSDATE
        FROM a4m.treferenceterminal@"Cmspro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM treferenceterminal;
        SELECT DISTINCT(etl_log_date) INTO sys_date FROM treferenceterminal;

        IF (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE)) THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'treferenceterminal', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'treferenceterminal', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC crd_card TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE crd_card';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO crd_card( pan, mbr, type, fiid, status, expdate, cardprofile, personid, authcustpan, authcustmbr
                     , limcurrency
                     , emvappcurrency, firstusedtime, lasttranid, lastatmused, lastposused, lastresettime
                     , lastrefreshtime, lastchangestatustime, lastatc, statuschangepackno, limitschangepackno
                     , declinedresponse, declinedretain, cnsdisabled, risklevel, riskcontroldisabled, osc_algo
                     , offlinepending
                     , offlinelimit, prevexpdate, branch, reissuevariant
                     , lasttrantime, nameoncard, ecstatus, lastchangeecstatustime
                     , ecstatuschangepackno, prevlastatc
                     , backcardprofile, subtype, parentpan, parentmbr, emvoptionsdisabled, childpan, childmbr
                     , current4dbc
                     , prev4dbc
                     , prevstatus, lastprevstatuschangetime, prevstatuschangepackno, userfieldspackno
                     , plastictype, cardprofilechangepackno, lasttelebankused, lastfimiused, lastecommerceused
                     , lastcmsused, dynpwdgentime, dynpwdcount, minallowedatc, ec_needstaticauth, ec_needdynpwdauth
                     , ec_needcapauth
                     , ec_needtokenauth, ec_authsettingschangepackno, ec_usecardsettings
                     , lastapproval2codepart, lastapproval3codepart, lastapproval4codepart
                     , lastapproval5codepart, lastapproval6codepart, lastatc_cap, prevlastatc_cap, lastapproval7codepart
                     , lastapproval8codepart, refid, emvscriptcounter, reissued, contactlessinterfacestate
                     , changecontactlessstatetranid, prevcontactlessinterfacestate, ectmpstatus, ectmpstatusstarttime
                     , ectmpstatusexpiration, ec_needexternalauth, lasttranrespcode, lasttrandetailedrespcode
                     , etl_status
                     , etl_log_date)
        SELECT pan
             , mbr
             , type
             , fiid
             , status
             , expdate
             , cardprofile
             , personid
             , authcustpan
             , authcustmbr
             , limcurrency
             , emvappcurrency
             , firstusedtime
             , lasttranid
             , lastatmused
             , lastposused
             , lastresettime
             , lastrefreshtime
             , lastchangestatustime
             , lastatc
             , statuschangepackno
             , limitschangepackno
             , declinedresponse
             , declinedretain
             , cnsdisabled
             , risklevel
             , riskcontroldisabled
             , osc_algo
             , offlinepending
             , offlinelimit
             , prevexpdate
             , branch
             , reissuevariant
             , lasttrantime
             , nameoncard
             , ecstatus
             , lastchangeecstatustime
             , ecstatuschangepackno
             , prevlastatc
             , backcardprofile
             , subtype
             , parentpan
             , parentmbr
             , emvoptionsdisabled
             , childpan
             , childmbr
             , current4dbc
             , prev4dbc
             , prevstatus
             , lastprevstatuschangetime
             , prevstatuschangepackno
             , userfieldspackno
             , plastictype
             , cardprofilechangepackno
             , lasttelebankused
             , lastfimiused
             , lastecommerceused
             , lastcmsused
             , dynpwdgentime
             , dynpwdcount
             , minallowedatc
             , ec_needstaticauth
             , ec_needdynpwdauth
             , ec_needcapauth
             , ec_needtokenauth
             , ec_authsettingschangepackno
             , ec_usecardsettings
             , lastapproval2codepart
             , lastapproval3codepart
             , lastapproval4codepart
             , lastapproval5codepart
             , lastapproval6codepart
             , lastatc_cap
             , prevlastatc_cap
             , lastapproval7codepart
             , lastapproval8codepart
             , refid
             , emvscriptcounter
             , reissued
             , contactlessinterfacestate
             , changecontactlessstatetranid
             , prevcontactlessinterfacestate
             , ectmpstatus
             , ectmpstatusstarttime
             , ectmpstatusexpiration
             , ec_needexternalauth
             , lasttranrespcode
             , lasttrandetailedrespcode
             , 'INSERT NEW'
             , SYSDATE
        FROM crd_card@"Twopro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM crd_card;
        SELECT DISTINCT(etl_log_date) INTO sys_date FROM crd_card;

        IF (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE)) THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'crd_card', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'crd_card', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC dic_cardstatus TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE dic_cardstatus';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO dic_cardstatus(id, name, constname, etl_status, etl_log_date)
        SELECT id
             , name
             , constname
             , 'INSERT NEW'
             , SYSDATE
        FROM dic_cardstatus@"Twopro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1) INTO row_count FROM dic_cardstatus;
        SELECT DISTINCT(etl_log_date) INTO sys_date FROM dic_cardstatus;

        IF (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE)) THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'dic_cardstatus', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'dic_cardstatus', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC tla TABLE FROM CARD
        SELECT COUNT(1)
        INTO row_count
        FROM tla@"Twopro.Localdomain"
        WHERE TRUNC(time) = TRUNC(SYSDATE - 1)
           OR TRUNC(origtime) = TRUNC(SYSDATE - 1);

        IF row_count > 0
        THEN
            MERGE
            INTO
                tla a
            USING (SELECT *
                   FROM tla@"Twopro.Localdomain"
                   WHERE TRUNC(time) = TRUNC(SYSDATE - 1)
                      OR TRUNC(origtime) = TRUNC(SYSDATE - 1))
                b
            ON
                (
                    a.id = b.id
                    )
            WHEN
                MATCHED THEN
                UPDATE
                SET packno                      = b.packno
                  , trannumber                  = b.trannumber
                  , type                        = b.type
                  , origtype                    = b.origtype
                  , origid                      = b.origid
                  , origunit                    = b.origunit
                  , host                        = b.host
                  , hostinterface               = b.hostinterface
                  , time                        = b.time
                  , origtime                    = b.origtime
                  , phase                       = b.phase
                  , termclass                   = b.termclass
                  , termname                    = b.termname
                  , termname2                   = b.termname2
                  , termdate                    = b.termdate
                  , termpsname                  = b.termpsname
                  , termfiid                    = b.termfiid
                  , termfiname                  = b.termfiname
                  , terminstid                  = b.terminstid
                  , termretailer                = b.termretailer
                  , termretailername            = b.termretailername
                  , termsic                     = b.termsic
                  , terminstcountry             = b.terminstcountry
                  , termcountry                 = b.termcountry
                  , termcounty                  = b.termcounty
                  , termstate                   = b.termstate
                  , termregion                  = b.termregion
                  , termcity                    = b.termcity
                  , termzip                     = b.termzip
                  , termlocation                = b.termlocation
                  , termdescription             = b.termdescription
                  , termbranch                  = b.termbranch
                  , termowner                   = b.termowner
                  , termtimeoffset              = b.termtimeoffset
                  , termapprvcodelen            = b.termapprvcodelen
                  , termentrycaps               = b.termentrycaps
                  , authfiid                    = b.authfiid
                  , authfiname                  = b.authfiname
                  , authpsname                  = b.authpsname
                  , sponsorfiid                 = b.sponsorfiid
                  , trancode                    = b.trancode
                  , trancategory                = b.trancategory
                  , draftcapture                = b.draftcapture
                  , fromaccttype                = b.fromaccttype
                  , fromacct                    = b.fromacct
                  , fromacctdescr               = b.fromacctdescr
                  , toaccttype                  = b.toaccttype
                  , toacct                      = b.toacct
                  , toacctdescr                 = b.toacctdescr
                  , toacct2                     = b.toacct2
                  , correspacct                 = b.correspacct
                  , correspamount               = b.correspamount
                  , fromdate                    = b.fromdate
                  , todate                      = b.todate
                  , amount                      = b.amount
                  , amount2                     = b.amount2
                  , fee                         = b.fee
                  , issuerfee                   = b.issuerfee
                  , currency                    = b.currency
                  , message                     = b.message
                  , pan                         = b.pan
                  , cardmember                  = b.cardmember
                  , pan2                        = b.pan2
                  , cardmember2                 = b.cardmember2
                  , authpan                     = b.authpan
                  , authmbr                     = b.authmbr
                  , prefix                      = b.prefix
                  , cardprofile                 = b.cardprofile
                  , acqgroup                    = b.acqgroup
                  , track1                      = b.track1
                  , track2                      = b.track2
                  , invoicenum                  = b.invoicenum
                  , originalinvoicenum          = b.originalinvoicenum
                  , seqnum                      = b.seqnum
                  , originalseqnum              = b.originalseqnum
                  , clerk                       = b.clerk
                  , poscondition                = b.poscondition
                  , posentrymode                = b.posentrymode
                  , preauthhold                 = b.preauthhold
                  , respcode                    = b.respcode
                  , declinereason               = b.declinereason
                  , responsecondition           = b.responsecondition
                  , retaincard                  = b.retaincard
                  , approvalcode                = b.approvalcode
                  , acctaccessed                = b.acctaccessed
                  , limitimpact                 = b.limitimpact
                  , recvinstid                  = b.recvinstid
                  , legerbalance                = b.legerbalance
                  , legerbalance2               = b.legerbalance2
                  , availbalance                = b.availbalance
                  , availbalance2               = b.availbalance2
                  , bonus                       = b.bonus
                  , bonus2                      = b.bonus2
                  , frombonusdeltaorig          = b.frombonusdeltaorig
                  , tobonusdeltaorig            = b.tobonusdeltaorig
                  , overdraftlimit              = b.overdraftlimit
                  , debithold                   = b.debithold
                  , credithold                  = b.credithold
                  , balancecurrencyacct         = b.balancecurrencyacct
                  , currencyacct                = b.currencyacct
                  , currencyacctto              = b.currencyacctto
                  , currencysettle              = b.currencysettle
                  , currencyorig                = b.currencyorig
                  , amountacct                  = b.amountacct
                  , amountacctto                = b.amountacctto
                  , amountsettle                = b.amountsettle
                  , amountorig                  = b.amountorig
                  , exchangerateacct            = b.exchangerateacct
                  , exchangerateacctto          = b.exchangerateacctto
                  , exchangeratesettle          = b.exchangeratesettle
                  , statement                   = b.statement
                  , settledate                  = b.settledate
                  , hostnetid                   = b.hostnetid
                  , hosttimestamp               = b.hosttimestamp
                  , revactualamount             = b.revactualamount
                  , revactualamountacct         = b.revactualamountacct
                  , revactualamountorig         = b.revactualamountorig
                  , revactualfee                = b.revactualfee
                  , revactualissuerfee          = b.revactualissuerfee
                  , revrequestid                = b.revrequestid
                  , admininitiator              = b.admininitiator
                  , reason                      = b.reason
                  , textmess                    = b.textmess
                  , error                       = b.error
                  , extaid                      = b.extaid
                  , extfid                      = b.extfid
                  , exttermname                 = b.exttermname
                  , exttermowner                = b.exttermowner
                  , extrrn                      = b.extrrn
                  , finalrrn                    = b.finalrrn
                  , extstan                     = b.extstan
                  , exttranattr                 = b.exttranattr
                  , extcurrency                 = b.extcurrency
                  , extrespcode                 = b.extrespcode
                  , procduration                = b.procduration
                  , authduration                = b.authduration
                  , icc_termcaps                = b.icc_termcaps
                  , icc_tvr                     = b.icc_tvr
                  , icc_random                  = b.icc_random
                  , icc_termsn                  = b.icc_termsn
                  , icc_issuerdata              = b.icc_issuerdata
                  , icc_cryptogram              = b.icc_cryptogram
                  , icc_apptrancount            = b.icc_apptrancount
                  , icc_terrmtrancount          = b.icc_terrmtrancount
                  , icc_appprofile              = b.icc_appprofile
                  , icc_iad                     = b.icc_iad
                  , icc_issuerscript1           = b.icc_issuerscript1
                  , icc_issuerscript2           = b.icc_issuerscript2
                  , icc_trantype                = b.icc_trantype
                  , icc_termcountry             = b.icc_termcountry
                  , icc_trandate                = b.icc_trandate
                  , icc_amount                  = b.icc_amount
                  , icc_currency                = b.icc_currency
                  , icc_cbamount                = b.icc_cbamount
                  , icc_cryptinformdata         = b.icc_cryptinformdata
                  , icc_cvmres                  = b.icc_cvmres
                  , icc_caok                    = b.icc_caok
                  , nexttran                    = b.nexttran
                  , prevtran                    = b.prevtran
                  , cnsent                      = b.cnsent
                  , safstatus                   = b.safstatus
                  , ectranid                    = b.ectranid
                  , ecauthtracknum              = b.ecauthtracknum
                  , ecmessage                   = b.ecmessage
                  , ecauthresultcode            = b.ecauthresultcode
                  , parentacct                  = b.parentacct
                  , parentacctto                = b.parentacctto
                  , exchangerateparentacct      = b.exchangerateparentacct
                  , exchangerateparentacctto    = b.exchangerateparentacctto
                  , offlinependinginc           = b.offlinependinginc
                  , icc_prevapptrancount        = b.icc_prevapptrancount
                  , network                     = b.network
                  , issuercountry               = b.issuercountry
                  , issuercardbrand             = b.issuercardbrand
                  , authresptext                = b.authresptext
                  , tmpoverdraft                = b.tmpoverdraft
                  , icc_issuerscriptresults     = b.icc_issuerscriptresults
                  , captoken                    = b.captoken
                  , capchallenge                = b.capchallenge
                  , icc_respcode                = b.icc_respcode
                  , icc_cardmember              = b.icc_cardmember
                  , issuerbranch                = b.issuerbranch
                  , receiptflag                 = b.receiptflag
                  , authfiid2                   = b.authfiid2
                  , authfiname2                 = b.authfiname2
                  , prefix2                     = b.prefix2
                  , debittranid                 = b.debittranid
                  , changereason                = b.changereason
                  , bonusaccumulation           = b.bonusaccumulation
                  , bonusprogramname            = b.bonusprogramname
                  , extpsfields                 = b.extpsfields
                  , irf                         = b.irf
                  , host1                       = b.host1
                  , host2                       = b.host2
                  , billingexchange             = b.billingexchange
                  , cardtype                    = b.cardtype
                  , icc_termtype                = b.icc_termtype
                  , legerbalancebefore          = b.legerbalancebefore
                  , legerbalance2before         = b.legerbalance2before
                  , availbalancebefore          = b.availbalancebefore
                  , availbalance2before         = b.availbalance2before
                  , bonusbefore                 = b.bonusbefore
                  , bonus2before                = b.bonus2before
                  , debitholdbefore             = b.debitholdbefore
                  , creditholdbefore            = b.creditholdbefore
                  , cashoutcycle                = b.cashoutcycle
                  , cashincycle                 = b.cashincycle
                  , termlanguage                = b.termlanguage
                  , p2psenderdata               = b.p2psenderdata
                  , tbcustomercomment           = b.tbcustomercomment
                  , amountorigdcc               = b.amountorigdcc
                  , currencyorigdcc             = b.currencyorigdcc
                  , prepaidcodeid               = b.prepaidcodeid
                  , challenge                   = b.challenge
                  , frontpaymentid              = b.frontpaymentid
                  , backpaymentid               = b.backpaymentid
                  , customeraddress             = b.customeraddress
                  , detailaddenda               = b.detailaddenda
                  , overdraftacctamount         = b.overdraftacctamount
                  , availbalanceacctamount      = b.availbalanceacctamount
                  , incstan                     = b.incstan
                  , finalfromaccttype           = b.finalfromaccttype
                  , finaltoaccttype             = b.finaltoaccttype
                  , paymenthostid               = b.paymenthostid
                  , conversiondate              = b.conversiondate
                  , inctranattr                 = b.inctranattr
                  , termretailertaxid           = b.termretailertaxid
                  , feeorig                     = b.feeorig
                  , extpsrrn                    = b.extpsrrn
                  , exttrannumber               = b.exttrannumber
                  , trancondition3dsecure       = b.trancondition3dsecure
                  , extpaymentfields            = b.extpaymentfields
                  , debitrrn                    = b.debitrrn
                  , posbatchnumber              = b.posbatchnumber
                  , exchangerateaccthierarchy   = b.exchangerateaccthierarchy
                  , exchangerateaccttohierarchy = b.exchangerateaccttohierarchy
                  , issuerfeeaccurate           = b.issuerfeeaccurate
                  , localsettledate             = b.localsettledate
                  , exttermretailername         = b.exttermretailername
                  , incpsfields                 = b.incpsfields
                  , proccnt                     = b.proccnt
                  , catlevel                    = b.catlevel
                  , auar                        = b.auar
                  , eophost                     = b.eophost
                  , authhost                    = b.authhost
                  , tranrisklevel               = b.tranrisklevel
                  , hosthistory                 = b.hosthistory
                  , bai                         = b.bai
                  , backcardproduct             = b.backcardproduct
                  , fpi                         = b.fpi
                  , termcontactlesscapable      = b.termcontactlesscapable
                  , detailedrespcode            = b.detailedrespcode
                  , termsupportpartialauth      = b.termsupportpartialauth
                  , currencyorigrequested       = b.currencyorigrequested
                  , amountorigrequested         = b.amountorigrequested
                  , exchangerateorig            = b.exchangerateorig
                  , termtransitprogram          = b.termtransitprogram
                  , cnsid                       = b.cnsid
                  , acctimpactlist              = b.acctimpactlist
                  , fromexchrates               = b.fromexchrates
                  , authdataint                 = b.authdataint
                  , authdataext                 = b.authdataext
                  , issuerfeelist               = b.issuerfeelist
                  , termsupport3dsec            = b.termsupport3dsec
                  , detailaddendaext            = b.detailaddendaext
                  , authdatainttmp              = b.authdatainttmp
                  , hiadditional                = b.hiadditional
                  , hiadditionalsensitive       = b.hiadditionalsensitive
                  , nii                         = b.nii
                  , termalias                   = b.termalias
                  , retaileralias               = b.retaileralias
                  , installmentdata             = b.installmentdata
                  , mpi                         = b.mpi
                  , primaryorigtype             = b.primaryorigtype
                  , primaryorigid               = b.primaryorigid
                  , origtermretailername        = b.origtermretailername
                  , parenttermid                = b.parenttermid
                  , parenttermretailer          = b.parenttermretailer
                  , parenttermname              = b.parenttermname
                  , parenttermretailername      = b.parenttermretailername
                  , tipamount                   = b.tipamount
                  , tipamount2                  = b.tipamount2
                  , hostdataint                 = b.hostdataint
                  , walletdata                  = b.walletdata
                  , ecdata                      = b.ecdata
                  , postauthhost                = b.postauthhost
                  , postrespcode                = b.postrespcode
                  , postauthadviceid            = b.postauthadviceid
                  , dccfee                      = b.dccfee
                  , tlecompliant                = b.tlecompliant
                  , dukptcompliant              = b.dukptcompliant
                  , icc_additionaldata          = b.icc_additionaldata
                  , termadditionaldata          = b.termadditionaldata
                  , oif                         = b.oif
                  , tokendata                   = b.tokendata
                  , acqdataint                  = b.acqdataint
                  , p2plimitdetails             = b.p2plimitdetails
                  , prizeid                     = b.prizeid
                  , prizequantity               = b.prizequantity
                  , extdescription              = b.extdescription
                  , capok                       = b.capok
                  , acqfiidorig                 = b.acqfiidorig
                  , chequedata                  = b.chequedata
                  , merchantdata                = b.merchantdata
                  , authprefix                  = b.authprefix
                  , fptti                       = b.fptti
                  , personid                    = b.personid
                  , acqduration                 = b.acqduration
                  , cancellationid              = b.cancellationid
                  , revrequestid2               = b.revrequestid2
                  , sdfromdb                    = b.sdfromdb
                  , miscverificationresult      = b.miscverificationresult
                  , disputedata                 = b.disputedata
                  , debittrandata               = b.debittrandata
                  , countersimpactlist          = b.countersimpactlist
                  , prefixdata                  = b.prefixdata
                  , messtocardholder            = b.messtocardholder
                  , cbamount                    = b.cbamount
                  , aliasdata                   = b.aliasdata
                  , exttrancode                 = b.exttrancode
                  , exttermclass                = b.exttermclass
                  , termentrycapsextended       = b.termentrycapsextended
                  , additionalamounts           = b.additionalamounts
                  , etl_status                  = 'UPDATE'
                  , etl_log_date                = SYSDATE
            WHEN
                NOT
                MATCHED THEN
                INSERT
                ( id, packno, trannumber, type, origtype, origid, origunit, host, hostinterface, time, origtime, phase
                , termclass, termname, termname2, termdate, termpsname, termfiid, termfiname, terminstid, termretailer
                , termretailername, termsic, terminstcountry, termcountry, termcounty, termstate, termregion, termcity
                , termzip
                , termlocation, termdescription, termbranch, termowner, termtimeoffset, termapprvcodelen, termentrycaps
                , authfiid, authfiname, authpsname, sponsorfiid, trancode, trancategory, draftcapture, fromaccttype
                , fromacct
                , fromacctdescr, toaccttype, toacct, toacctdescr, toacct2, correspacct, correspamount, fromdate, todate
                , amount
                , amount2, fee, issuerfee, currency, message, pan, cardmember, pan2, cardmember2, authpan, authmbr
                , prefix
                , cardprofile, acqgroup, track1, track2
                , invoicenum, originalinvoicenum, seqnum, originalseqnum, clerk, poscondition, posentrymode, preauthhold
                , respcode, declinereason, responsecondition, retaincard, approvalcode, acctaccessed, limitimpact
                , recvinstid
                , legerbalance, legerbalance2, availbalance, availbalance2, bonus, bonus2, frombonusdeltaorig
                , tobonusdeltaorig
                , overdraftlimit, debithold, credithold, balancecurrencyacct, currencyacct, currencyacctto
                , currencysettle
                , currencyorig, amountacct, amountacctto, amountsettle, amountorig, exchangerateacct, exchangerateacctto
                , exchangeratesettle, statement, settledate, hostnetid, hosttimestamp, revactualamount
                , revactualamountacct
                , revactualamountorig, revactualfee, revactualissuerfee, revrequestid, admininitiator, reason, textmess
                , error
                , extaid, extfid, exttermname, exttermowner, extrrn, finalrrn, extstan, exttranattr, extcurrency
                , extrespcode
                , procduration, authduration, icc_termcaps, icc_tvr, icc_random, icc_termsn, icc_issuerdata
                , icc_cryptogram
                , icc_apptrancount, icc_terrmtrancount, icc_appprofile, icc_iad, icc_issuerscript1, icc_issuerscript2
                , icc_trantype, icc_termcountry, icc_trandate, icc_amount, icc_currency, icc_cbamount
                , icc_cryptinformdata
                , icc_cvmres, icc_caok, nexttran, prevtran, cnsent, safstatus, ectranid, ecauthtracknum, ecmessage
                , ecauthresultcode, parentacct, parentacctto, exchangerateparentacct
                , exchangerateparentacctto, offlinependinginc, icc_prevapptrancount, network, issuercountry
                , issuercardbrand
                , authresptext, tmpoverdraft, icc_issuerscriptresults, captoken, capchallenge, icc_respcode
                , icc_cardmember
                , issuerbranch, receiptflag, authfiid2, authfiname2, prefix2, debittranid, changereason
                , bonusaccumulation
                , bonusprogramname, extpsfields, irf, host1, host2, billingexchange, cardtype, icc_termtype
                , legerbalancebefore
                , legerbalance2before, availbalancebefore, availbalance2before, bonusbefore, bonus2before
                , debitholdbefore
                , creditholdbefore, cashoutcycle, cashincycle, termlanguage, p2psenderdata, tbcustomercomment
                , amountorigdcc
                , currencyorigdcc, prepaidcodeid, challenge, frontpaymentid, backpaymentid
                , customeraddress, detailaddenda, overdraftacctamount, availbalanceacctamount
                , incstan, finalfromaccttype, finaltoaccttype, paymenthostid, conversiondate, inctranattr
                , termretailertaxid
                , feeorig, extpsrrn, exttrannumber, trancondition3dsecure, extpaymentfields, debitrrn, posbatchnumber
                , exchangerateaccthierarchy, exchangerateaccttohierarchy, issuerfeeaccurate, localsettledate
                , exttermretailername, incpsfields, proccnt, catlevel, auar, eophost, authhost, tranrisklevel
                , hosthistory, bai
                , backcardproduct, fpi, termcontactlesscapable, detailedrespcode, termsupportpartialauth
                , currencyorigrequested
                , amountorigrequested, exchangerateorig, termtransitprogram, cnsid, acctimpactlist, fromexchrates
                , authdataint
                , authdataext, issuerfeelist, termsupport3dsec, detailaddendaext, authdatainttmp, hiadditional
                , hiadditionalsensitive, nii, termalias, retaileralias, installmentdata, mpi, primaryorigtype
                , primaryorigid
                , origtermretailername, parenttermid, parenttermretailer, parenttermname, parenttermretailername
                , tipamount
                , tipamount2, hostdataint, walletdata, ecdata, postauthhost, postrespcode, postauthadviceid, dccfee
                , tlecompliant, dukptcompliant, icc_additionaldata, termadditionaldata, oif, tokendata, acqdataint
                , p2plimitdetails, prizeid, prizequantity, extdescription, capok, acqfiidorig, chequedata, merchantdata
                , authprefix, fptti, personid, acqduration, cancellationid, revrequestid2, sdfromdb
                , miscverificationresult
                , disputedata, debittrandata, countersimpactlist, prefixdata, messtocardholder, cbamount, aliasdata
                , exttrancode
                , exttermclass, termentrycapsextended, additionalamounts, etl_status, etl_log_date)
                VALUES ( b.id, b.packno, b.trannumber, b.type, b.origtype, b.origid, b.origunit, b.host, b.hostinterface
                       , b.time, b.origtime, b.phase, b.termclass, b.termname, b.termname2, b.termdate, b.termpsname
                       , b.termfiid, b.termfiname, b.terminstid, b.termretailer, b.termretailername, b.termsic
                       , b.terminstcountry, b.termcountry, b.termcounty, b.termstate, b.termregion, b.termcity
                       , b.termzip, b.termlocation, b.termdescription, b.termbranch, b.termowner, b.termtimeoffset
                       , b.termapprvcodelen, b.termentrycaps, b.authfiid, b.authfiname, b.authpsname, b.sponsorfiid
                       , b.trancode, b.trancategory, b.draftcapture, b.fromaccttype, b.fromacct, b.fromacctdescr
                       , b.toaccttype, b.toacct, b.toacctdescr, b.toacct2, b.correspacct, b.correspamount, b.fromdate
                       , b.todate, b.amount, b.amount2, b.fee, b.issuerfee, b.currency, b.message, b.pan, b.cardmember
                       , b.pan2, b.cardmember2, b.authpan, b.authmbr, b.prefix, b.cardprofile, b.acqgroup, b.track1
                       , b.track2, b.invoicenum, b.originalinvoicenum, b.seqnum, b.originalseqnum, b.clerk
                       , b.poscondition, b.posentrymode, b.preauthhold, b.respcode, b.declinereason, b.responsecondition
                       , b.retaincard, b.approvalcode, b.acctaccessed, b.limitimpact, b.recvinstid, b.legerbalance
                       , b.legerbalance2, b.availbalance, b.availbalance2, b.bonus, b.bonus2, b.frombonusdeltaorig
                       , b.tobonusdeltaorig, b.overdraftlimit, b.debithold, b.credithold, b.balancecurrencyacct
                       , b.currencyacct, b.currencyacctto, b.currencysettle, b.currencyorig, b.amountacct
                       , b.amountacctto, b.amountsettle, b.amountorig, b.exchangerateacct, b.exchangerateacctto
                       , b.exchangeratesettle, b.statement, b.settledate, b.hostnetid, b.hosttimestamp
                       , b.revactualamount, b.revactualamountacct, b.revactualamountorig, b.revactualfee
                       , b.revactualissuerfee, b.revrequestid, b.admininitiator, b.reason, b.textmess, b.error, b.extaid
                       , b.extfid, b.exttermname, b.exttermowner, b.extrrn, b.finalrrn, b.extstan, b.exttranattr
                       , b.extcurrency, b.extrespcode, b.procduration, b.authduration, b.icc_termcaps, b.icc_tvr
                       , b.icc_random, b.icc_termsn, b.icc_issuerdata, b.icc_cryptogram, b.icc_apptrancount
                       , b.icc_terrmtrancount, b.icc_appprofile, b.icc_iad, b.icc_issuerscript1, b.icc_issuerscript2
                       , b.icc_trantype, b.icc_termcountry, b.icc_trandate, b.icc_amount, b.icc_currency, b.icc_cbamount
                       , b.icc_cryptinformdata, b.icc_cvmres, b.icc_caok, b.nexttran, b.prevtran, b.cnsent, b.safstatus
                       , b.ectranid, b.ecauthtracknum, b.ecmessage, b.ecauthresultcode, b.parentacct, b.parentacctto
                       , b.exchangerateparentacct, b.exchangerateparentacctto, b.offlinependinginc
                       , b.icc_prevapptrancount, b.network, b.issuercountry, b.issuercardbrand, b.authresptext
                       , b.tmpoverdraft, b.icc_issuerscriptresults, b.captoken, b.capchallenge, b.icc_respcode
                       , b.icc_cardmember, b.issuerbranch, b.receiptflag, b.authfiid2, b.authfiname2, b.prefix2
                       , b.debittranid, b.changereason, b.bonusaccumulation, b.bonusprogramname, b.extpsfields, b.irf
                       , b.host1, b.host2, b.billingexchange, b.cardtype, b.icc_termtype, b.legerbalancebefore
                       , b.legerbalance2before, b.availbalancebefore, b.availbalance2before, b.bonusbefore
                       , b.bonus2before, b.debitholdbefore, b.creditholdbefore, b.cashoutcycle, b.cashincycle
                       , b.termlanguage, b.p2psenderdata, b.tbcustomercomment, b.amountorigdcc, b.currencyorigdcc
                       , b.prepaidcodeid, b.challenge, b.frontpaymentid, b.backpaymentid, b.customeraddress
                       , b.detailaddenda, b.overdraftacctamount, b.availbalanceacctamount, b.incstan
                       , b.finalfromaccttype, b.finaltoaccttype, b.paymenthostid, b.conversiondate, b.inctranattr
                       , b.termretailertaxid, b.feeorig, b.extpsrrn, b.exttrannumber, b.trancondition3dsecure
                       , b.extpaymentfields, b.debitrrn, b.posbatchnumber, b.exchangerateaccthierarchy
                       , b.exchangerateaccttohierarchy, b.issuerfeeaccurate, b.localsettledate, b.exttermretailername
                       , b.incpsfields, b.proccnt, b.catlevel, b.auar, b.eophost, b.authhost, b.tranrisklevel
                       , b.hosthistory, b.bai, b.backcardproduct, b.fpi, b.termcontactlesscapable, b.detailedrespcode
                       , b.termsupportpartialauth, b.currencyorigrequested, b.amountorigrequested, b.exchangerateorig
                       , b.termtransitprogram, b.cnsid, b.acctimpactlist, b.fromexchrates, b.authdataint, b.authdataext
                       , b.issuerfeelist, b.termsupport3dsec, b.detailaddendaext, b.authdatainttmp, b.hiadditional
                       , b.hiadditionalsensitive, b.nii, b.termalias, b.retaileralias, b.installmentdata, b.mpi
                       , b.primaryorigtype, b.primaryorigid, b.origtermretailername, b.parenttermid
                       , b.parenttermretailer, b.parenttermname, b.parenttermretailername, b.tipamount, b.tipamount2
                       , b.hostdataint, b.walletdata, b.ecdata, b.postauthhost, b.postrespcode, b.postauthadviceid
                       , b.dccfee, b.tlecompliant, b.dukptcompliant, b.icc_additionaldata, b.termadditionaldata, b.oif
                       , b.tokendata, b.acqdataint, b.p2plimitdetails, b.prizeid, b.prizequantity, b.extdescription
                       , b.capok, b.acqfiidorig, b.chequedata, b.merchantdata, b.authprefix, b.fptti, b.personid
                       , b.acqduration, b.cancellationid, b.revrequestid2, b.sdfromdb, b.miscverificationresult
                       , b.disputedata, b.debittrandata, b.countersimpactlist, b.prefixdata, b.messtocardholder
                       , b.cbamount, b.aliasdata, b.exttrancode, b.exttermclass, b.termentrycapsextended
                       , b.additionalamounts, 'INSERT NEW', SYSDATE);
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'tla', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'No new or updated data', 'CARD', 'tla', 'SYNC', row_count);
            COMMIT;
        END IF;

        UPDATE tla a
        SET a.revrequestid = (SELECT f.revrequestid FROM tla@twopro.localdomain f WHERE a.id = f.id AND rownum = 1)
        WHERE TRUNC(a.time) = TRUNC(SYSDATE - 1)
           OR TRUNC(a.origtime) = TRUNC(SYSDATE - 1);
        COMMIT;

        -- SYNC dic_respcode TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE dic_respcode';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO dic_respcode(id, name, constname, internal, etl_status, etl_log_date)
        SELECT id
             , name
             , constname
             , internal
             , 'INSERT NEW'
             , SYSDATE
        FROM dic_respcode@"Twopro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1)
        INTO row_count
        FROM dic_respcode;
        SELECT DISTINCT(etl_log_date)
        INTO sys_date
        FROM dic_respcode;

        IF
            (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE))
        THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'dic_respcode', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'dic_respcode', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC dic_trancode TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE dic_trancode';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO dic_trancode(id, class, name, constname, supported, logintlg, notified, etl_status, etl_log_date)
        SELECT id
             , class
             , name
             , constname
             , supported
             , logintlg
             , notified
             , 'INSERT NEW'
             , SYSDATE
        FROM dic_trancode@"Twopro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1)
        INTO row_count
        FROM dic_trancode;
        SELECT DISTINCT(etl_log_date)
        INTO sys_date
        FROM dic_trancode;

        IF
            (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE))
        THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'dic_trancode', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'dic_trancode', 'SYNC', row_count);
            COMMIT;
        END IF;

        -- SYNC tcontractitem TABLE FROM CARD
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE tcontractitem';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        INSERT
        INTO tcontractitem(branch, no, itemtype, itemcode, key, actualdate, itemdescr, etl_status, etl_log_date)
        SELECT branch
             , no
             , itemtype
             , itemcode
             , key
             , actualdate
             , itemdescr
             , 'INSERT NEW'
             , SYSDATE
        FROM a4m.tcontractitem@"Cmspro.Localdomain" a;
        COMMIT;

        SELECT COUNT(1)
        INTO row_count
        FROM tcontractitem;
        SELECT DISTINCT(etl_log_date)
        INTO sys_date
        FROM tcontractitem;

        IF
            (row_count > 0 AND TRUNC(sys_date) = TRUNC(SYSDATE))
        THEN
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'SUCCESS', 'CARD', 'tcontractitem', 'SYNC', row_count);
            COMMIT;
        ELSE
            INSERT
            INTO etl_log
                (log_date, status, source, table_name, action, row_affected)
            VALUES (SYSDATE, 'FAILED', 'CARD', 'tcontractitem', 'SYNC', row_count);
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
            VALUES ( 'proc_etl_card_sync', error_code, error_message
                   , TO_CHAR(dbms_utility.format_error_backtrace), SYSDATE);
            COMMIT;

    END excute_etl_card_sync_job;
END package_etl_card_sync_job;
/

