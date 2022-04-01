-- Databricks notebook source
select state_id as state, count(zip) as nzips
from uszips_delta_unmanaged
where state_id not in ('AS','GU','MP','PR','VI')
group by state
order by nzips

-- COMMAND ----------

select state_id as state, sum(population) as population
from uszips_delta_unmanaged
where state_id not in ('AS','GU','MP','PR','VI')
group by state
order by state

-- COMMAND ----------


