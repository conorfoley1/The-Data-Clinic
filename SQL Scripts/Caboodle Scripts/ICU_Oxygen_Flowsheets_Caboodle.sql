------------------------------------------------------------------------------------------------------------------------------------
-- Database: Caboodle
-- Author: Peter Shakeshaft. Tim Bonnici
-- Date: 09-Mar-2022
--
-- This is a script for taking ICU oxygen flowsheets, separating them into "oxygen", "HFNO", "CPAP" and "Intubation".
-- It also calculates time on each type oxygen and calendar days.
-- Assumes good completion of ICU flowsheets, including going back to room air.
-- Oxygen type definitions (case statements) provided by Tim Bonnici.
------------------------------------------------------------------------------------------------------------------------------------

-- Extract the relevant flowsheets
drop table if exists #flowsheets
select 
enc.EncounterEpicCsn
,fv.TakenInstant as 'DateTimeRecorded'
,fsd.FlowsheetRowEpicId as 'FlowsheetId'
,fsd.Name as 'FlowsheetName'
,fv.Value as 'FlowsheetValue'
,enc.EndInstant
into #flowsheets
from FlowsheetValueFact fv -- flo_name
inner join FlowsheetRowDim fsd ON fv.FlowsheetRowKey = fsd.FlowsheetRowKey
inner join EncounterFact enc ON fv.EncounterKey = enc.EncounterKey
where 
fsd.FlowsheetRowEpicId in ('3040109305','3040102607')
and enc.EndInstant between '01-Jun-2021' and '29-Jun-2021'


-- Limit to ventilation and turn rows into columns
drop table if exists #vent_fls
select 
EncounterEpicCsn
,EndInstant
,DateTimeRecorded
,min(case when FlowsheetId = '3040102607' then FlowsheetValue end) as 'VentilationMode'
,min(case when FlowsheetId = '3040109305' then FlowsheetValue end) as 'O2DeliveryMethod'
into #vent_fls
from #flowsheets f
where 
FlowsheetId in ('3040102607','3040109305')
group by 
DateTimeRecorded
,EncounterEpicCsn
,EndInstant

-- Separate out into "type" of oxygen received based on ventilation mode and delivery method
DROP TABLE IF EXISTS #Oxygen_Inst 
select 
vf.EncounterEpicCsn
,vf.EndInstant
,DateTimeRecorded
,lead(DateTimeRecorded,1) over (partition by vf.EncounterEpicCsn order by DateTimeRecorded asc) as 'NextDateTimeRecorded'
,vf.O2DeliveryMethod
,vf.VentilationMode
,CASE 
    WHEN O2DeliveryMethod = 'Tracheostomy' AND VentilationMode IS NOT NULL 
    THEN 'Intubation'
    WHEN O2DeliveryMethod = 'Endotracheal tube' 
    THEN 'Intubation'
    WHEN O2DeliveryMethod = 'CPAP/Bi-PAP mask' 
        OR O2DeliveryMethod = 'Oxyhood' 
    THEN 'CPAP/NIV'
    WHEN O2DeliveryMethod = 'High-flow nasal cannula (HFNC)' 
    THEN 'HFNO'
    WHEN O2DeliveryMethod IN('No respiratory support provided','')
    THEN 'Room Air'
    ELSE 'O2'
    END AS OxygenType
INTO #Oxygen_INST
from #vent_fls vf

-- Find start and stop points of CPAP
DROP TABLE IF EXISTS #Oxygen_Change
select 
*
,CASE 
	WHEN OxygenType <> ISNULL(LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded),'')
		AND OxygenType = 'O2'
	THEN 'StartO2'
	WHEN ISNULL(LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded),'') = 'O2'
		AND OxygenType <> 'O2'
	THEN 'EndO2'
	WHEN LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded) IS NULL
	THEN 'Start'
	WHEN LEAD(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded) IS NULL
	THEN 'End'
	END AS 'O2StatusChange'
,CASE 
	WHEN OxygenType <> ISNULL(LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded),'')
		AND OxygenType = 'HFNO'
	THEN 'StartHFNO'
	WHEN  ISNULL(LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded),'') = 'HFNO'
		AND OxygenType <> 'HFNO'
	THEN 'EndHFNO'
	WHEN LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded) IS NULL
	THEN 'Start'
	WHEN LEAD(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded) IS NULL
	THEN 'End'
	END AS 'HFNOStatusChange'
,CASE 
	WHEN OxygenType <> ISNULL(LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded),'')
		AND OxygenType = 'CPAP/NIV'
	THEN 'StartCPAP'
	WHEN  ISNULL(LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded),'') = 'CPAP/NIV'
		AND OxygenType <> 'CPAP/NIV'
	THEN 'EndCPAP'
	WHEN LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded) IS NULL
	THEN 'Start'
	WHEN LEAD(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded) IS NULL
	THEN 'End'
	END AS 'CPAPStatusChange'
,CASE
	WHEN OxygenType <> ISNULL(LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded),'')
		AND OxygenType = 'Intubation'
	THEN 'StartIntubation'
	WHEN  ISNULL(LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded),'') = 'Intubation'
		AND OxygenType <> 'Intubation'
	THEN 'EndIntubation'
	WHEN LAG(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded) IS NULL
	THEN 'Start'
	WHEN LEAD(OxygenType,1) OVER (PARTITION BY EncounterEpicCsn ORDER BY DateTimeRecorded) IS NULL
	THEN 'End'
	END AS 'IntubationStatusChange'
Into #Oxygen_Change
from #Oxygen_INST
order by 
EncounterEpicCsn
,DateTimeRecorded


-- Coalesce the start and end points so theyre in subsequent rows - ie remove any "continuation" flowsheet rows
-- also adds a flowsheet Id to allow joining on to itself more easily
DROP TABLE IF EXISTS #Oxygen_Times
select distinct
*
,DATEFROMPARTS(DATEPART(YY, cpp.DateTimeRecorded),DATEPART(MM, cpp.DateTimeRecorded),DATEPART(DD, cpp.DateTimeRecorded)) AS DateRecorded
,ROW_NUMBER() OVER(PARTITION BY cpp.EncounterEpicCsn ORDER BY cpp.DateTimeRecorded ASC) AS 'FlowId'

INTO #Oxygen_Times
from #Oxygen_Change cpp 
where 
(cpp.O2StatusChange IS NOT NULL
or cpp.HFNOStatusChange IS NOT NULL
or cpp.CPAPStatusChange IS NOT NULL
or cpp.IntubationStatusChange IS NOT NULL)

ORDER BY
cpp.EncounterEpicCsn
,cpp.DateTimeRecorded

-- join the table on to itself with a "delay" of one row - i.e. make it so that starting a type of oxygen and ending that period on oxygen are in the same row
-- separated into different types of oxygen for ease of use and understanding
drop table if exists #O2_rows
select
o.EncounterEpicCsn
,o.EndInstant
,o.FlowId
,o.DateRecorded
,o.DateTimeRecorded
,o.O2StatusChange 
,ox_next.FlowId AS 'EndFlowId'
,ox_next.DateRecorded AS 'EndDateRecorded'
,ox_next.DateTimeRecorded AS 'EndDateTimeRecorded'
,ox_next.O2StatusChange AS 'EndStatusChange'
into #O2_rows
from #Oxygen_Times o
left join #Oxygen_Times ox_next on o.EncounterEpicCsn = ox_next.EncounterEpicCsn
	and o.FlowId = ox_next.FlowId - 1
where 
o.O2StatusChange = 'StartO2'
order by
o.EncounterEpicCsn, o.DateTimeRecorded

drop table if exists #HFNO_rows
select
o.EncounterEpicCsn
,o.EndInstant
,o.FlowId
,o.DateRecorded
,o.DateTimeRecorded
,o.HFNOStatusChange 
,ox_next.FlowId AS 'EndFlowId'
,ox_next.DateRecorded AS 'EndDateRecorded'
,ox_next.DateTimeRecorded AS 'EndDateTimeRecorded'
,ox_next.HFNOStatusChange AS 'EndStatusChange'
into #HFNO_rows
from #Oxygen_Times o
left join #Oxygen_Times ox_next on o.EncounterEpicCsn = ox_next.EncounterEpicCsn
	and o.FlowId = ox_next.FlowId - 1
where 
o.HFNOStatusChange = 'StartHFNO'
order by
o.EncounterEpicCsn, o.DateTimeRecorded

drop table if exists #CPAP_rows
select
o.EncounterEpicCsn
,o.EndInstant
,o.FlowId
,o.DateRecorded
,o.DateTimeRecorded
,o.CPAPStatusChange 
,ox_next.FlowId AS 'EndFlowId'
,ox_next.DateRecorded AS 'EndDateRecorded'
,ox_next.DateTimeRecorded AS 'EndDateTimeRecorded'
,ox_next.CPAPStatusChange AS 'EndStatusChange'
into #CPAP_rows
from #Oxygen_Times o
left join #Oxygen_Times ox_next on o.EncounterEpicCsn = ox_next.EncounterEpicCsn
	and o.FlowId = ox_next.FlowId - 1
where 
o.CPAPStatusChange = 'StartCPAP'
order by
o.EncounterEpicCsn, o.DateTimeRecorded

drop table if exists #Intubation_rows
select
o.EncounterEpicCsn
,o.EndInstant
,o.FlowId
,o.DateRecorded
,o.DateTimeRecorded
,o.IntubationStatusChange 
,ox_next.FlowId AS 'EndFlowId'
,ox_next.DateRecorded AS 'EndDateRecorded'
,ox_next.DateTimeRecorded AS 'EndDateTimeRecorded'
,ox_next.IntubationStatusChange AS 'EndStatusChange'
into #Intubation_rows
from #Oxygen_Times o
left join #Oxygen_Times ox_next on o.EncounterEpicCsn = ox_next.EncounterEpicCsn
	and o.FlowId = ox_next.FlowId - 1
where 
o.IntubationStatusChange = 'StartIntubation'
order by
o.EncounterEpicCsn, o.DateTimeRecorded

-- Sum the distinct calendar days
-- use the inner join to select the type of oxygen of interest via the correct table
select  
o.EncounterEpicCsn
,COUNT(DISTINCT dd.DateValue) AS 'CalDaysO2'
--,dd.DateValue
from DateDim dd
inner join #O2_rows o ON dd.DateValue between o.DateRecorded and ISNULL(o.EndDateRecorded,CAST(o.EndInstant as date)) -- EndInstant (discharge date) as an upper bound - in case any oxygen types have not been "finished" - may lead to strange behaviour
group by o.EncounterEpicCsn

select 
o.EncounterEpicCsn
,COUNT(DISTINCT dd.DateValue) AS 'CalDaysHFNO'
--,dd.DateValue
from DateDim dd
inner join #HFNO_rows o ON dd.DateValue between o.DateRecorded and ISNULL(o.EndDateRecorded,CAST(o.EndInstant as date)) -- EndInstant (discharge date) as an upper bound - in case any oxygen types have not been "finished" - may lead to strange behaviour
group by o.EncounterEpicCsn

select 
o.EncounterEpicCsn
,COUNT(DISTINCT dd.DateValue) AS 'CalDaysCPAP'
--,dd.DateValue
from DateDim ddW
inner join #CPAP_rows o ON dd.DateValue between o.DateRecorded and ISNULL(o.EndDateRecorded,CAST(o.EndInstant as date)) -- EndInstant (discharge date) as an upper bound - in case any oxygen types have not been "finished" - may lead to strange behaviour
group by o.EncounterEpicCsn

select 
o.EncounterEpicCsn
,COUNT(DISTINCT dd.DateValue) AS 'CalDaysIntubation'
--,dd.DateValue
from DateDim dd
inner join #Intubation_rows o ON dd.DateValue between o.DateRecorded and ISNULL(o.EndDateRecorded,CAST(o.EndInstant as date)) -- EndInstant (discharge date) as an upper bound - in case any oxygen types have not been "finished" - may lead to strange behaviour
group by o.EncounterEpicCsn