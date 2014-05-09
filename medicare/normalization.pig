Medicare = LOAD './HW4/Medicare/Input/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt' USING PigStorage('\t') AS (npi:chararray, nppes_provider_last_org_name:chararray, nppes_provider_first_name:chararray, nppes_provider_mi:chararray, nppes_credentials:chararray, nppes_provider_gender:chararray, nppes_entity_code:chararray, nppes_provider_street1:chararray, nppes_provider_street2:chararray, nppes_provider_city:chararray, nppes_provider_zip:chararray, nppes_provider_state:chararray, nppes_provider_country:chararray, provider_type:chararray, medicare_participation_indicator:chararray, place_of_service:chararray, hcpcs_code:chararray, hcpcs_description:chararray, line_srvc_cnt:float, bene_unique_cnt:float, bene_day_srvc_cnt:float, average_Medicare_allowed_amt:float, stdev_Medicare_allowed_amt:float, average_submitted_chrg_amt:float, stdev_submitted_chrg_amt:float, average_Medicare_payment_amt:float, stdev_Medicare_payment_amt:float);

B = FILTER Medicare BY npi != 'npi';

C = GROUP B ALL;

Dmin = FOREACH C GENERATE MIN(B.line_srvc_cnt) as min_line_srvc_cnt, MIN(B.bene_unique_cnt) as min_bene_unique_cnt, MIN(B.bene_day_srvc_cnt) as min_bene_day_srvc_cnt, MIN(B.average_Medicare_allowed_amt) as min_average_Medicare_allowed_amt, MIN(B.stdev_Medicare_allowed_amt) as min_stdev_Medicare_allowed_amt, MIN(B.average_submitted_chrg_amt) as min_average_submitted_chrg_amt, MIN(B.stdev_submitted_chrg_amt) as min_stdev_submitted_chrg_amt, MIN(B.average_Medicare_payment_amt) as min_average_Medicare_payment_amt, MIN(B.stdev_Medicare_payment_amt) as min_stdev_Medicare_payment_amt;

Dmax = FOREACH C GENERATE MAX(B.line_srvc_cnt) as max_line_srvc_cnt, MAX(B.bene_unique_cnt) as max_bene_unique_cnt, MAX(B.bene_day_srvc_cnt) as max_bene_day_srvc_cnt, MAX(B.average_Medicare_allowed_amt) as max_average_Medicare_allowed_amt, MAX(B.stdev_Medicare_allowed_amt) as max_stdev_Medicare_allowed_amt, MAX(B.average_submitted_chrg_amt) as max_average_submitted_chrg_amt, MAX(B.stdev_submitted_chrg_amt) as max_stdev_submitted_chrg_amt, MAX(B.average_Medicare_payment_amt) as max_average_Medicare_payment_amt, MAX(B.stdev_Medicare_payment_amt) as max_stdev_Medicare_payment_amt;

E = FOREACH B GENERATE npi, ((line_srvc_cnt - Dmin.min_line_srvc_cnt)/(Dmax.max_line_srvc_cnt - Dmin.min_line_srvc_cnt)) as norm_line_srvc_cnt, ((bene_unique_cnt - Dmin.min_bene_unique_cnt)/(Dmax.max_bene_unique_cnt - Dmin.min_bene_unique_cnt)) as norm_bene_unique_cnt, ((bene_day_srvc_cnt - Dmin.min_bene_day_srvc_cnt)/(Dmax.max_bene_day_srvc_cnt - Dmin.min_bene_day_srvc_cnt)) as norm_bene_day_srvc_cnt, ((average_Medicare_allowed_amt - Dmin.min_average_Medicare_allowed_amt)/(Dmax.max_average_Medicare_allowed_amt - Dmin.min_average_Medicare_allowed_amt)) as norm_average_Medicare_allowed_amt, ((stdev_Medicare_allowed_amt - Dmin.min_stdev_Medicare_allowed_amt)/(Dmax.max_stdev_Medicare_allowed_amt - Dmin.min_stdev_Medicare_allowed_amt)) as norm_stdev_Medicare_allowed_amt, ((average_submitted_chrg_amt - Dmin.min_average_submitted_chrg_amt)/(Dmax.max_average_submitted_chrg_amt - Dmin.min_average_submitted_chrg_amt)) as norm_average_submitted_chrg_amt, ((stdev_submitted_chrg_amt - Dmin.min_stdev_submitted_chrg_amt)/(Dmax.max_stdev_submitted_chrg_amt - Dmin.min_stdev_submitted_chrg_amt)) as norm_stdev_submitted_chrg_amt, ((average_Medicare_payment_amt - Dmin.min_average_Medicare_payment_amt)/(Dmax.max_average_Medicare_payment_amt - Dmin.min_average_Medicare_payment_amt)) as norm_average_Medicare_payment_amt, ((stdev_Medicare_payment_amt - Dmin.min_stdev_Medicare_payment_amt)/(Dmax.max_stdev_Medicare_payment_amt - Dmin.min_stdev_Medicare_payment_amt)) as norm_stdev_Medicare_payment_amt;

STORE E INTO './HW4/Medicare/Norm' USING PigStorage();