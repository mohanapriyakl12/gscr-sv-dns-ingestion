-- Databricks notebook source
-- MAGIC %md
-- MAGIC Below are two sql script one is DDL and another one is DML
-- MAGIC
-- MAGIC DDL script is used to create main table
-- MAGIC
-- MAGIC DML Script is used to insert values from stage table into main table while we are inserting data into main table we are doing type casting as per to the column data type defined in DDL

-- COMMAND ----------

-- --DDL Script
-- CREATE TABLE pbi_tables_vw.supplier_visibility_2023_reports ( 

-- year int, 

-- week int, 

-- start_date string, 

-- site string, 

-- site_name string, 

-- part_no string, 

-- zebra_part_number string, 

-- manufacturer_part_number string, 

-- description string, 

-- tier2_supplier_name string, 

-- po_extension_at_52_weeks string, 

-- product_family string, 

-- sub_family string, 

-- product_family_list string, 

-- lead_time double, 

-- lt_match_sti string, 

-- raw_cost double, 

-- safety_stock double, 

-- lblty string, 

-- ltb_number string, 

-- ss_match_sti string, 

-- psm_num string, 

-- psm_supplier_name string, 

-- psm_escalation_level string, 

-- gsp string, 

-- wks_1_to_3_short_score double, 

-- wks_4_to_7_short_score double, 

-- wks_8_to_9_short_score double, 

-- wks_10_to_11_short_score double, 

-- overall_score double, 

-- supply_risk string, 

-- lt_in_wks double, 

-- values_po_coverage_within_lt double, 

-- shortage_at_lt_y_or_n string, 

-- po_coverage_at_lt_y_or_n string, 

-- po_coverage_at_52_wks double, 

-- coverage_52_wks_y_or_n string, 

-- shortage_on_wk_num double, 

-- shortage_weekly_bucket_num string, 

-- risk_at_lt string, 

-- oh double, 

-- soi double, 

-- zoi double, 

-- hub double, 

-- total_open_po_qty double, 

-- 52_wks_demand double, 

-- values string, 

-- num double, 

-- past_due double, 

-- wk1 double, 

-- wk2 double, 

-- wk3 double, 

-- wk4 double, 

-- wk5 double, 

-- wk6 double, 

-- wk7 double, 

-- wk8 double, 

-- wk9 double, 

-- wk10 double, 

-- wk11 double, 

-- wk12 double, 

-- wk13 double, 

-- wk14 double, 

-- wk15 double, 

-- wk16 double, 

-- wk17 double, 

-- wk18 double, 

-- wk19 double, 

-- wk20 double, 

-- wk21 double, 

-- wk22 double, 

-- wk23 double, 

-- wk24 double, 

-- wk25 double, 

-- wk26 double, 

-- wk27 double, 

-- wk28 double, 

-- wk29 double, 

-- wk30 double, 

-- wk31 double, 

-- wk32 double, 

-- wk33 double, 

-- wk34 double, 

-- wk35 double, 

-- wk36 double, 

-- wk37 double, 

-- wk38 double, 

-- wk39 double, 

-- wk40 double, 

-- wk41 double, 

-- wk42 double, 

-- wk43 double, 

-- wk44 double, 

-- wk45 double, 

-- wk46 double, 

-- wk47 double, 

-- wk48 double, 

-- wk49 double, 

-- wk50 double, 

-- wk51 double, 

-- wk52 double, 

-- wk53 double, 

-- wk54 double); 

-- COMMAND ----------

-- import datetime
-- get the current year and week number
-- year, week, _day = datetime.date.today().isocalendar()

-- --DML script
-- insert into pbi_tables_vw.supplier_visibility_2023_reports(
-- select
-- cast(year as int) as year,
-- cast(week as int) as week,
-- start_date,
-- site,
-- site_name,
-- part_no,
-- zebra_part_number,
-- manufacturer_part_number,
-- description,
-- tier2_supplier_name,
-- po_extension_at_52_weeks,
-- product_family,
-- sub_family,
-- product_family_list,
-- cast(lead_time as double) as lead_time,
-- lt_match_sti,
-- cast(raw_cost as double) as raw_cost,
-- cast(safety_stock as double) as safety_stock ,
-- lblty,
-- ltb_number,
-- ss_match_sti,
-- psmnum as psm_num,
-- psm_supplier_name,
-- psm_escalation_level,
-- gsp,
-- cast(wks_1_to_3_short_score as double) as wks_1_to_3_short_score,
-- cast(wks_4_to_7_short_score as double) as wks_4_to_7_short_score ,
-- cast(wks_8_to_9_short_score as double) as wks_8_to_9_short_score ,
-- cast(wks_10_to_11_short_score as double) as wks_10_to_11_short_score ,
-- cast(overall_score as double) as overall_score,
-- supply_risk,
-- cast(lt_in_wks as double) as lt_in_wks,
-- cast(values_po_coverage_within_lt as double) as values_po_coverage_within_lt ,
-- shortage_at_lt_y_or_n,
-- po_coverage_at_lt_y_or_n,
-- cast(po_coverage_at_52_wks as double) as po_coverage_at_52_wks,
-- coverage_52_wks_y_or_n,
-- cast(shortage_on_wknum as double) as shortage_on_wk_num,
-- shortage_weekly_bucketnum as shortage_weekly_bucket_num,
-- risk_at_lt,
-- cast(oh as double) as oh,
-- cast(soi as double) as soi,
-- cast(zoi as double) as zoi,
-- cast(hub as double) as hub,
-- cast(total_open_po_qty as double) as total_open_po_qty,
-- cast(52_wks_demand as double) as 52_wks_demand,
-- values,
-- cast(num as double) as num,
-- cast(past_due as double) as past_due,
-- cast(wk1 as double) as wk1,
-- cast(wk2 as double) as wk2,
-- cast(wk3 as double) as wk3,
-- cast(wk4 as double) as wk4,
-- cast(wk5 as double) as wk5,
-- cast(wk6 as double) as wk6,
-- cast(wk7 as double) as wk7,
-- cast(wk8 as double) as wk8,
-- cast(wk9 as double) as wk9,
-- cast(wk10 as double) as wk10,
-- cast(wk11 as double) as wk11,
-- cast(wk12 as double) as wk12,
-- cast(wk13 as double) as wk13,
-- cast(wk14 as double) as wk14,
-- cast(wk15 as double) as wk15,
-- cast(wk16 as double) as wk16,
-- cast(wk17 as double) as wk17,
-- cast(wk18 as double) as wk18,
-- cast(wk19 as double) as wk19,
-- cast(wk20 as double) as wk20,
-- cast(wk21 as double) as wk21,
-- cast(wk22 as double) as wk22,
-- cast(wk23 as double) as wk23,
-- cast(wk24 as double) as wk24,
-- cast(wk25 as double) as wk25,
-- cast(wk26 as double) as wk26,
-- cast(wk27 as double) as wk27,
-- cast(wk28 as double) as wk28,
-- cast(wk29 as double) as wk29,
-- cast(wk30 as double) as wk30,
-- cast(wk31 as double) as wk31,
-- cast(wk32 as double) as wk32,
-- cast(wk33 as double) as wk33,
-- cast(wk34 as double) as wk34,
-- cast(wk35 as double) as wk35,
-- cast(wk36 as double) as wk36,
-- cast(wk37 as double) as wk37,
-- cast(wk38 as double) as wk38,
-- cast(wk39 as double) as wk39,
-- cast(wk40 as double) as wk40,
-- cast(wk41 as double) as wk41,
-- cast(wk42 as double) as wk42,
-- cast(wk43 as double) as wk43,
-- cast(wk44 as double) as wk44,
-- cast(wk45 as double) as wk45,
-- cast(wk46 as double) as wk46,
-- cast(wk47 as double) as wk47,
-- cast(wk48 as double) as wk48,
-- cast(wk49 as double) as wk49,
-- cast(wk50 as double) as wk50,
-- cast(wk51 as double) as wk51,
-- cast(wk52 as double) as wk52,
-- cast(wk53 as double) as wk53,
-- cast(wk54 as double) as wk54
-- from pbi_tables_vw.supplier_visibility_reports_2023_stage
-- where week=={week} and year=={year});

-- COMMAND ----------

--DML script
insert into pbi_tables_vw.supplier_visibility_2023_reports(
select
cast(year as int) as year,
cast(week as int) as week,
start_date,
site,
site_name,
part_no,
zebra_part_number,
manufacturer_part_number,
description,
tier2_supplier_name,
po_extension_at_52_weeks,
product_family,
sub_family,
product_family_list,
cast(lead_time as double) as lead_time,
lt_match_sti,
cast(raw_cost as double) as raw_cost,
cast(safety_stock as double) as safety_stock ,
lblty,
ltb_number,
ss_match_sti,
psmnum as psm_num,
psm_supplier_name,
psm_escalation_level,
gsp,
cast(wks_1_to_3_short_score as double) as wks_1_to_3_short_score,
cast(wks_4_to_7_short_score as double) as wks_4_to_7_short_score ,
cast(wks_8_to_9_short_score as double) as wks_8_to_9_short_score ,
cast(wks_10_to_11_short_score as double) as wks_10_to_11_short_score ,
cast(overall_score as double) as overall_score,
supply_risk,
cast(lt_in_wks as double) as lt_in_wks,
cast(values_po_coverage_within_lt as double) as values_po_coverage_within_lt ,
shortage_at_lt_y_or_n,
po_coverage_at_lt_y_or_n,
cast(po_coverage_at_52_wks as double) as po_coverage_at_52_wks,
coverage_52_wks_y_or_n,
cast(shortage_on_wknum as double) as shortage_on_wk_num,
shortage_weekly_bucketnum as shortage_weekly_bucket_num,
risk_at_lt,
cast(oh as double) as oh,
cast(soi as double) as soi,
cast(zoi as double) as zoi,
cast(hub as double) as hub,
cast(total_open_po_qty as double) as total_open_po_qty,
cast(52_wks_demand as double) as 52_wks_demand,
values,
cast(num as double) as num,
cast(past_due as double) as past_due,
cast(wk1 as double) as wk1,
cast(wk2 as double) as wk2,
cast(wk3 as double) as wk3,
cast(wk4 as double) as wk4,
cast(wk5 as double) as wk5,
cast(wk6 as double) as wk6,
cast(wk7 as double) as wk7,
cast(wk8 as double) as wk8,
cast(wk9 as double) as wk9,
cast(wk10 as double) as wk10,
cast(wk11 as double) as wk11,
cast(wk12 as double) as wk12,
cast(wk13 as double) as wk13,
cast(wk14 as double) as wk14,
cast(wk15 as double) as wk15,
cast(wk16 as double) as wk16,
cast(wk17 as double) as wk17,
cast(wk18 as double) as wk18,
cast(wk19 as double) as wk19,
cast(wk20 as double) as wk20,
cast(wk21 as double) as wk21,
cast(wk22 as double) as wk22,
cast(wk23 as double) as wk23,
cast(wk24 as double) as wk24,
cast(wk25 as double) as wk25,
cast(wk26 as double) as wk26,
cast(wk27 as double) as wk27,
cast(wk28 as double) as wk28,
cast(wk29 as double) as wk29,
cast(wk30 as double) as wk30,
cast(wk31 as double) as wk31,
cast(wk32 as double) as wk32,
cast(wk33 as double) as wk33,
cast(wk34 as double) as wk34,
cast(wk35 as double) as wk35,
cast(wk36 as double) as wk36,
cast(wk37 as double) as wk37,
cast(wk38 as double) as wk38,
cast(wk39 as double) as wk39,
cast(wk40 as double) as wk40,
cast(wk41 as double) as wk41,
cast(wk42 as double) as wk42,
cast(wk43 as double) as wk43,
cast(wk44 as double) as wk44,
cast(wk45 as double) as wk45,
cast(wk46 as double) as wk46,
cast(wk47 as double) as wk47,
cast(wk48 as double) as wk48,
cast(wk49 as double) as wk49,
cast(wk50 as double) as wk50,
cast(wk51 as double) as wk51,
cast(wk52 as double) as wk52,
cast(wk53 as double) as wk53,
cast(wk54 as double) as wk54
from pbi_tables_vw.supplier_visibility_reports_2023_stage
where week==36 and year==2023);

-- COMMAND ----------

--DML script
insert into pbi_tables_vw.supplier_visibility_2023_reports(
select
cast(year as int) as year,
cast(week as int) as week,
start_date,
site,
site_name,
part_no,
zebra_part_number,
manufacturer_part_number,
description,
tier2_supplier_name,
po_extension_at_52_weeks,
product_family,
sub_family,
product_family_list,
cast(lead_time as double) as lead_time,
lt_match_sti,
cast(raw_cost as double) as raw_cost,
cast(safety_stock as double) as safety_stock ,
lblty,
ltb_number,
ss_match_sti,
psmnum as psm_num,
psm_supplier_name,
psm_escalation_level,
gsp,
cast(wks_1_to_3_short_score as double) as wks_1_to_3_short_score,
cast(wks_4_to_7_short_score as double) as wks_4_to_7_short_score ,
cast(wks_8_to_9_short_score as double) as wks_8_to_9_short_score ,
cast(wks_10_to_11_short_score as double) as wks_10_to_11_short_score ,
cast(overall_score as double) as overall_score,
supply_risk,
cast(lt_in_wks as double) as lt_in_wks,
cast(values_po_coverage_within_lt as double) as values_po_coverage_within_lt ,
shortage_at_lt_y_or_n,
po_coverage_at_lt_y_or_n,
cast(po_coverage_at_52_wks as double) as po_coverage_at_52_wks,
coverage_52_wks_y_or_n,
cast(shortage_on_wknum as double) as shortage_on_wk_num,
shortage_weekly_bucketnum as shortage_weekly_bucket_num,
risk_at_lt,
cast(oh as double) as oh,
cast(soi as double) as soi,
cast(zoi as double) as zoi,
cast(hub as double) as hub,
cast(total_open_po_qty as double) as total_open_po_qty,
cast(52_wks_demand as double) as 52_wks_demand,
values,
cast(num as double) as num,
cast(past_due as double) as past_due,
cast(wk1 as double) as wk1,
cast(wk2 as double) as wk2,
cast(wk3 as double) as wk3,
cast(wk4 as double) as wk4,
cast(wk5 as double) as wk5,
cast(wk6 as double) as wk6,
cast(wk7 as double) as wk7,
cast(wk8 as double) as wk8,
cast(wk9 as double) as wk9,
cast(wk10 as double) as wk10,
cast(wk11 as double) as wk11,
cast(wk12 as double) as wk12,
cast(wk13 as double) as wk13,
cast(wk14 as double) as wk14,
cast(wk15 as double) as wk15,
cast(wk16 as double) as wk16,
cast(wk17 as double) as wk17,
cast(wk18 as double) as wk18,
cast(wk19 as double) as wk19,
cast(wk20 as double) as wk20,
cast(wk21 as double) as wk21,
cast(wk22 as double) as wk22,
cast(wk23 as double) as wk23,
cast(wk24 as double) as wk24,
cast(wk25 as double) as wk25,
cast(wk26 as double) as wk26,
cast(wk27 as double) as wk27,
cast(wk28 as double) as wk28,
cast(wk29 as double) as wk29,
cast(wk30 as double) as wk30,
cast(wk31 as double) as wk31,
cast(wk32 as double) as wk32,
cast(wk33 as double) as wk33,
cast(wk34 as double) as wk34,
cast(wk35 as double) as wk35,
cast(wk36 as double) as wk36,
cast(wk37 as double) as wk37,
cast(wk38 as double) as wk38,
cast(wk39 as double) as wk39,
cast(wk40 as double) as wk40,
cast(wk41 as double) as wk41,
cast(wk42 as double) as wk42,
cast(wk43 as double) as wk43,
cast(wk44 as double) as wk44,
cast(wk45 as double) as wk45,
cast(wk46 as double) as wk46,
cast(wk47 as double) as wk47,
cast(wk48 as double) as wk48,
cast(wk49 as double) as wk49,
cast(wk50 as double) as wk50,
cast(wk51 as double) as wk51,
cast(wk52 as double) as wk52,
cast(wk53 as double) as wk53,
cast(wk54 as double) as wk54
from pbi_tables_vw.supplier_visibility_reports_2023_stage
where week==36 and year==2023);

-- COMMAND ----------

--CREATE TABLE supplier_visibility_reports_2021_clone CLONE supplier_visibility_reports_2021_stage
