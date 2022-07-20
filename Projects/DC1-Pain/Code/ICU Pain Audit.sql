set nocount on
set ansi_warnings off
set datefirst 1

-- Create cohort of ICU patients 
drop table if exists #movements
select pat.PrimaryMrn
,cast(haf.EncounterEpicCsn as varchar) as  EncounterEpicCsn
,plef.EncounterKey
,plef.PatientLocationEventKey
,plef.CensusLocationKey
,cen.LocationAbbreviation as 'CensusSite'
,cen.DepartmentAbbreviation as 'CensusDepartment'
,cen.RoomName as 'CensusRoom'
,cen.BedName as 'CensusBed'
,cen.BedInCensus
,plef.EventType
,plef.EventLengthInMinutes
,plef.StartInstant
,plef.EndInstant
,ROW_NUMBER() over(partition by plef.EncounterKey order by plef.StartInstant) as 'Allorder'
,ROW_NUMBER() over(partition by plef.EncounterKey,cen.DepartmentAbbreviation order by plef.StartInstant) as 'Wardorder'
,ROW_NUMBER() over(partition by plef.EncounterKey,cen.DepartmentAbbreviation,cen.RoomName order by plef.StartInstant) as 'Roomorder'
,ROW_NUMBER() over(partition by plef.EncounterKey,cen.DepartmentAbbreviation,cen.RoomName,cen.BedName order by plef.StartInstant) as 'Bedorder'
,ROW_NUMBER() over(partition by plef.EncounterKey order by plef.StartInstant) - ROW_NUMBER() over(partition by plef.EncounterKey,cen.DepartmentAbbreviation order by plef.StartInstant) as 'WardGrouping'
,ROW_NUMBER() over(partition by plef.EncounterKey order by plef.StartInstant) - ROW_NUMBER() over(partition by plef.EncounterKey,cen.DepartmentAbbreviation,cen.RoomName,cen.BedName order by plef.StartInstant) as 'RoomGrouping'
,ROW_NUMBER() over(partition by plef.EncounterKey order by plef.StartInstant) - ROW_NUMBER() over(partition by plef.EncounterKey,cen.DepartmentAbbreviation,cen.RoomName,cen.BedName order by plef.StartInstant) as 'BedGrouping'
into #movements
from PatientLocationEventFact plef
	
inner join HospitalAdmissionFact haf on plef.EncounterKey = haf.EncounterKey
inner join PatientDim pat on plef.PatientKey = pat.PatientKey
inner join DepartmentDim cen on plef.CensusLocationKey = cen.DepartmentKey and cen.IsBed = 1 
	
order by haf.EncounterEpicCsn, plef.StartInstant

drop table if exists #wardtransfers
	;with wards as (
	select 
	EncounterEpicCsn
	,EncounterKey
	,CensusSite as 'Site'
	,CensusDepartment as 'Department'
	,min(StartInstant) as 'TransferStartDate'
	,case when max(case when EndInstant is null then 1 else 0 end) = 0 then max(EndInstant) end as 'TransferEndDate'
	,ROW_NUMBER() over(partition by encounterkey order by min(startInstant)) as 'TransferNumber'
	from #movements
	
	group by EncounterEpicCsn
	,Encounterkey
	,CensusSite
	,CensusDepartment
	,WardGrouping
	)
	select 
	wt.EncounterEpicCsn
	,wt.EncounterKey
	,wt.Site
	,wt.Department
	,sp.AdmissionDate
	,sp.DischargeDate
	,wt.TransferStartDate
	,wt.TransferEndDate
	,wt.TransferNumber
	,wtp.Site as 'SiteTransferredFrom'
	,wtp.Department as 'DepartmentTransferredFrom'
	,wtn.Site as 'SiteTransferredTo'
	,wtn.Department as 'DepartmentTransferredTo'
	,case 
		when wt.Department ='UCH ED' then 'AE'
		when CAST(FORMAT(wt.TransferStartDate,'yyyy-MM-dd HH:mm') AS datetime) = CAST(FORMAT(sp.AdmissionDate,'yyyy-MM-dd HH:mm') AS datetime) then 'A'
		when CAST(FORMAT(wt.TransferStartDate,'yyyy-MM-dd HH:mm') AS datetime) = CAST(FORMAT(sp.AdmissionDate,'yyyy-MM-dd HH:mm') AS datetime) then 'A'
		else'T'
		end as 'TransferTypeStart'
	,case 
		when CAST(FORMAT(wt.TransferEndDate,'yyyy-MM-dd HH:mm') AS datetime) = CAST(FORMAT(sp.DischargeDate,'yyyy-MM-dd HH:mm') AS datetime) then 'D'
		when wtn.Department ='UCH ED' then 'AE'
		when CAST(FORMAT(wt.TransferEndDate,'yyyy-MM-dd HH:mm') AS datetime) = CAST(FORMAT(sp.AdmissionDate,'yyyy-MM-dd HH:mm') AS datetime) then 'A'
		when CAST(FORMAT(wt.TransferEndDate,'yyyy-MM-dd HH:mm') AS datetime) = CAST(FORMAT(sp.AdmissionDate,'yyyy-MM-dd HH:mm') AS datetime) then 'A'
		when wt.Department ='UCH ED' and wtn.EncounterEpicCsn is null and wt.TransferEndDate is not null then 'D'
		when wt.TransferEndDate is null then null
		else 'T'
		end as 'TransferTypeEnd'
	into #wardtransfers
	from wards wt
	
	left join wards wtp on wt.EncounterKey = wtp.EncounterKey and wtp.TransferNumber = wt.TransferNumber - 1
	left join wards wtn on wt.EncounterKey = wtn.EncounterKey and wtn.TransferNumber = wt.TransferNumber + 1
	left join cas.IP_Spell sp on wt.EncounterEpicCsn = sp.HospitalProviderSpellNumber
	where wt.Department IN ('UCH T03 ICU','WMS CCU','GWB L01W'
	                                        ,'UCH P03 CV','UCH T07 CV','UCH T06 PACU')
	order by wt.EncounterEpicCsn, wt.TransferNumber

DROP TABLE IF EXISTS #cohort
select 
wt.EncounterEpicCsn
,wt.EncounterKey
,wt.Department
,convert(varchar(8),wt.TransferStartDate,112) as 'TransferStartDateKey'
,replace(left(cast(wt.TransferStartDate as time),5),':','') as 'TransferStartTimeKey'
,convert(varchar(8),wt.TransferEndDate,112) as 'TransferEndDateKey'
,replace(left(cast(wt.TransferEndDate as time),5),':','') as 'TransferEndTimeKey'
,wt.TransferStartDate
,wt.TransferEndDate
,wt.TransferTypeStart
,wt.TransferTypeEnd
,wt.DepartmentTransferredFrom
,wt.DepartmentTransferredTo
,datediff(day,wt.TransferStartDate,wt.TransferEndDate)as 'Meas_LoS_Days'
,datediff(hour,wt.TransferStartDate,wt.TransferEndDate)as 'Meas_LoS_Hrs'
,case when wt.TransferTypeStart = 'A' then 1 end as 'Meas_Admissions'
,case when wt.TransferTypeStart = 'T' then 1 end as 'Meas_TransferIn'
,case when wt.TransferTypeEnd = 'D' then 1 end as 'Meas_Discharges'
,case when wt.TransferTypeEnd = 'T' then 1 end as 'Meas_TransferOut'
,case 
	when wt.TransferTypeEnd = 'D' and sp.DischargeMethodCode = 4 
	then 1
	when wt.TransferEndDate <= GETDATE()
	then 0
	end as 'Meas_Death'
,1 as 'Meas_Pts'
INTO #cohort
from #wardtransfers wt

left join cas.IP_Spell sp on wt.EncounterEpicCsn = sp.HospitalProviderSpellNumber

-- Create tablw containing pain scores from relavent flowsheets
DROP TABLE IF EXISTS #flow 
SELECT flow.EncounterKey
       ,ef.EncounterEpicCsn 
	   ,pd.PrimaryMrn
	   ,pd.BirthDate
	   ,pd.FirstName
	   ,pd.LastName
       ,flow.TakenInstant
	   ,flowtemp.FlowsheetTemplateKey
	   ,flowtemp.FlowsheetTemplateEpicId
	   ,FlowsheetRowEpicId
	   ,flowdim.FlowsheetRowKey
	   ,flowtemp.Name AS 'template_name'
	   ,flowtemp.DisplayName AS 'template_display_name'
	   ,flowdim.Name
	   ,flowdim.DisplayName
	   ,flow.Value
	   ,ROW_NUMBER() OVER (PARTITION BY ef.EncounterEpicCsn , flowdim.FlowsheetRowKey ORDER BY TakenInstant ) AS 'testnum'
INTO #flow 
FROM FlowsheetValueFact AS flow
LEFT JOIN FlowsheetRowDim AS flowdim ON flow.FlowsheetRowKey = flowdim.FlowsheetRowKey
LEFT JOIN FlowsheetTemplateDim AS flowtemp ON flowtemp.FlowsheetTemplateKey = flow.FlowsheetTemplateKey
LEFT JOIN EncounterFact AS ef ON flow.EncounterKey = ef.EncounterKey
LEFT JOIN PatientDim AS pd ON ef.PatientDurableKey = pd.DurableKey AND pd.IsCurrent = 1
 WHERE  flowtemp.FlowsheetTemplateEpicId IN ('1071' -- NHNN ICU Obs
                                             ,'1131' -- Critical Care Obs
											 ,'417' -- ICU Assessment
										)
		AND
		flowdim.FlowsheetRowEpicId IN('39902' -- pain score
		                              ,'3040104280' -- at rest
									  ,'3040104281' -- movement
									  ,'301120' -- interventions
									  ,'3040109617' -- BPS intervention
									  ,'3040109755' -- non-verbal interventions
									  ,'3040109757' -- CC pain obs intervention 
		                  
		                        )

-- combine pain scores with ICU cohort data 
DROP TABLE IF EXISTS #pain
SELECT icu.EncounterEpicCsn
       ,pain.PrimaryMrn
	   ,pain.FirstName
	   ,pain.LastName
	   ,DATEDIFF(YEAR, pain.BirthDate, icu.TransferStartDate) AS 'Age_when_transfered_to_ITU'
       ,icu.Department
	   ,icu.TransferStartDate
	   ,icu.TransferEndDate
	   ,icu.Meas_LoS_Hrs
	   ,ROW_NUMBER() OVER(PARTITION BY icu.EncounterEpicCsn, icu.Department,icu.TransferStartDate ORDER BY pain.TakenInstant ASC ) AS 'pain_oder _number'
       ,pain.TakenInstant
	   ,pain.Value AS 'Pain_socre'
	   ,rest.Value AS 'at_rest'
	   ,move.Value AS 'movement'
	   ,inter.Value AS 'intervention'
	   ,otherinter.Value AS 'Other_recorded_interventions'
	   ,LAG(pain.TakenInstant,1) OVER(PARTITION BY icu.EncounterEpicCsn, icu.Department ORDER BY pain.TakenInstant ASC) AS 'previous_taken_instant' 
	   ,LAG(pain.Value,1) OVER(PARTITION BY icu.EncounterEpicCsn, icu.Department ORDER BY pain.TakenInstant ASC) AS 'previous_Pain_socre' 
	   ,LAG(rest.Value,1) OVER(PARTITION BY icu.EncounterEpicCsn, icu.Department ORDER BY pain.TakenInstant ASC) AS 'previous_at_rest'
	   ,LAG(move.Value,1) OVER(PARTITION BY icu.EncounterEpicCsn, icu.Department ORDER BY pain.TakenInstant ASC) AS 'previous_movement'
INTO #pain
FROM #cohort AS icu
LEFT JOIN #flow AS pain ON icu.EncounterEpicCsn = pain.EncounterEpicCsn 
                           AND pain.FlowsheetRowEpicId = '39902' -- pain score
						   AND pain.TakenInstant >= icu.TransferStartDate
						   AND pain.TakenInstant <= icu.TransferEndDate

LEFT JOIN #flow AS rest ON icu.EncounterEpicCsn = rest.EncounterEpicCsn 
                           AND pain.TakenInstant = rest.TakenInstant
                           AND rest.FlowsheetRowEpicId = '3040104280' -- at rest
						   AND rest.TakenInstant >= icu.TransferStartDate
						   AND rest.TakenInstant <= icu.TransferEndDate

LEFT JOIN #flow AS move ON icu.EncounterEpicCsn = move.EncounterEpicCsn 
                           AND pain.TakenInstant = move.TakenInstant
                           AND move.FlowsheetRowEpicId = '3040104281' -- movement
						   AND move.TakenInstant >= icu.TransferStartDate
						   AND move.TakenInstant <= icu.TransferEndDate

LEFT JOIN #flow AS inter ON icu.EncounterEpicCsn = inter.EncounterEpicCsn 
                           AND pain.TakenInstant = inter.TakenInstant
                           AND inter.FlowsheetRowEpicId = '301120' -- intervention
						   AND inter.TakenInstant >= icu.TransferStartDate
						   AND inter.TakenInstant <= icu.TransferEndDate

LEFT JOIN #flow AS otherinter ON icu.EncounterEpicCsn = inter.EncounterEpicCsn 
                           AND DATEDIFF(MINUTE,pain.TakenInstant, otherinter.TakenInstant) <= 5
						   AND DATEDIFF(MINUTE,pain.TakenInstant, otherinter.TakenInstant) >= -5
                           AND inter.FlowsheetRowEpicId IN ('3040109617','3040109755','3040109757') -- intervention recorded in other assesments 
						   AND inter.TakenInstant >= icu.TransferStartDate
						   AND inter.TakenInstant <= icu.TransferEndDate

WHERE pain.TakenInstant IS NOT NULL

ORDER  BY icu.EncounterEpicCsn
          ,icu.Department



-- ADD in meterics fo audit
SELECT *
-- flag for if the pain score used differes from the the previous one 
       ,CASE 
	    WHEN pain.Pain_socre = pain.previous_Pain_socre THEN 0
		WHEN pain.Pain_socre IS NULL THEN 0
		WHEN pain.previous_Pain_socre IS NULL THEN 0
		WHEN pain.previous_Pain_socre = 'None/denies pain' THEN 0
		ELSE 1
		END AS 'change_in_pain_score_used'
-- metric which denotes if thier has been a change in the at rest pain score
		,CASE
		WHEN pain.at_rest IS NULL OR pain.previous_at_rest IS NULL THEN NULL
		WHEN pain.at_rest = pain.previous_at_rest THEN 'No change in rest score'
		WHEN pain.at_rest > pain.previous_at_rest THEN 'increase in rest score'
		WHEN pain.at_rest < pain.previous_at_rest THEN 'decrease in rest score'
		END AS 'at_rest_change'
-- metric which denotes if thier has been a change in the movement pain score
		,CASE
		WHEN pain.movement IS NULL OR pain.previous_movement IS NULL THEN NULL
		WHEN pain.movement = pain.previous_movement THEN 'No change in movement score'
		WHEN pain.movement > pain.previous_movement THEN 'increase in movement score'
		WHEN pain.movement < pain.previous_movement THEN 'decrease in movement score'
		END AS 'movement_change'
	   ,DATEDIFF(MINUTE,pain.previous_taken_instant,pain.TakenInstant) AS 'time_between_scores_(mins)'
-- flag for if the pain score reassessment was measured with the target time frame 
	   ,CASE 
	    WHEN pain.previous_at_rest IS NULL AND pain.previous_movement IS NULL AND DATEDIFF(MINUTE,pain.previous_taken_instant,pain.TakenInstant) <= 240 THEN 1
		WHEN (pain.previous_at_rest >= 3 OR pain.previous_movement >= 3) AND DATEDIFF(MINUTE,pain.previous_taken_instant,pain.TakenInstant) <= 60 THEN 1
		WHEN (pain.previous_at_rest <= 2 OR pain.previous_movement <= 2) AND DATEDIFF(MINUTE,pain.previous_taken_instant,pain.TakenInstant) <= 240 THEN 1
		WHEN pain.previous_Pain_socre = 'None/denies pain' AND DATEDIFF(MINUTE,pain.previous_taken_instant,pain.TakenInstant) <= 240 THEN 1
		ELSE 0
		END AS 'within_pain_reassesment_targets'
		,CASE 
		 WHEN pain.movement >= pain.previous_movement AND pain.movement > 0  THEN 1
		WHEN pain.at_rest >= pain.previous_at_rest AND pain.at_rest >0  THEN 1
		ELSE 0
		END AS 'patient_experiencing_pain'
-- flags when a patients pain score has increased or stayed the same and an intervention has been recorded 		  
		,CASE
	    WHEN pain.movement >= pain.previous_movement AND pain.movement > 0 AND pain.intervention IS NOT NULL THEN 1
		WHEN pain.at_rest >= pain.previous_at_rest AND pain.at_rest >0 AND pain.intervention IS NOT NULL THEN 1
		ELSE 0
		END AS 'Pain_managed'

FROM #pain AS pain

WHERE YEAR(pain.TransferEndDate) = 2021
      AND
	  MONTH(pain.TransferEndDate) =10

ORDER BY pain.EncounterEpicCsn
         ,pain.TransferStartDate
		 ,pain.TakenInstant



-- aggrgated data 
;with #dt AS (
SELECT *
       ,CASE 
	    WHEN pain.Pain_socre = pain.previous_Pain_socre THEN 0
		WHEN pain.Pain_socre IS NULL THEN 0
		WHEN pain.previous_Pain_socre IS NULL THEN 0
		WHEN pain.previous_Pain_socre = 'None/denies pain' THEN 0
		ELSE 1
		END AS 'change_in_pain_score_used'
		,CASE
		WHEN pain.at_rest IS NULL OR pain.previous_at_rest IS NULL THEN NULL
		WHEN pain.at_rest = pain.previous_at_rest THEN 'No change in rest score'
		WHEN pain.at_rest > pain.previous_at_rest THEN 'increase in rest score'
		WHEN pain.at_rest < pain.previous_at_rest THEN 'decrease in rest score'
		END AS 'at_rest_change'
		,CASE
		WHEN pain.movement IS NULL OR pain.previous_movement IS NULL THEN NULL
		WHEN pain.movement = pain.previous_movement THEN 'No change in movement score'
		WHEN pain.movement > pain.previous_movement THEN 'increase in movement score'
		WHEN pain.movement < pain.previous_movement THEN 'decrease in movement score'
		END AS 'movement_change'
	   ,DATEDIFF(MINUTE,pain.previous_taken_instant,pain.TakenInstant) AS 'time_between_scores'
	   ,CASE 
	    WHEN pain.previous_at_rest IS NULL AND pain.previous_movement IS NULL AND DATEDIFF(MINUTE,pain.previous_taken_instant,pain.TakenInstant) <= 240 THEN 1
		WHEN (pain.previous_at_rest >= 3 OR pain.previous_movement >= 3) AND DATEDIFF(MINUTE,pain.previous_taken_instant,pain.TakenInstant) <= 30 THEN 1
		WHEN (pain.previous_at_rest <= 2 OR pain.previous_movement <= 2) AND DATEDIFF(MINUTE,pain.previous_taken_instant,pain.TakenInstant) <= 240 THEN 1
		WHEN pain.previous_Pain_socre = 'None/denies pain' AND DATEDIFF(MINUTE,pain.previous_taken_instant,pain.TakenInstant) <= 240 THEN 1
		ELSE 0
		END AS 'within_pain_reassesment_targets'
		,CASE 
		 WHEN pain.movement >= pain.previous_movement AND pain.movement > 0  THEN 1
		WHEN pain.at_rest >= pain.previous_at_rest AND pain.at_rest >0  THEN 1
		ELSE 0
		END AS 'patient_experiencing_pain'
       ,CASE
	    WHEN pain.movement >= pain.previous_movement AND pain.movement > 0 AND pain.intervention IS NOT NULL THEN 1
		WHEN pain.at_rest >= pain.previous_at_rest AND pain.at_rest >0 AND pain.intervention IS NOT NULL THEN 1
		ELSE 0
		END AS 'Pain_managed'

FROM #pain AS pain

)
, #agg AS (
SELECT  DATEFROMPARTS(YEAR(dt.TransferEndDate),MONTH(dt.TransferEndDate),'01') AS 'month'
         ,SUM(dt.within_pain_reassesment_targets) AS 'pain_reassesments_within_target'
		 ,SUM(CASE WHEN dt.previous_taken_instant IS NOT NULL THEN 1 ELSE 0 END) 'pain_reassesments_completed'
         ,CAST(SUM(dt.within_pain_reassesment_targets) AS FLOAT)/SUM(CASE WHEN dt.previous_taken_instant IS NOT NULL THEN 1 ELSE 0 END)  AS 'pain_reassesment_performance' 
		 ,SUM(dt.change_in_pain_score_used) AS 'changes_in pain_score_used_for_patients'
		 ,CAST(SUM(dt.change_in_pain_score_used) AS FLOAT)/SUM(CASE WHEN dt.previous_taken_instant IS NOT NULL THEN 1 ELSE 0 END)  AS 'pain_reassesment_changes_in_score' 
		 ,CAST(SUM(dt.Pain_managed) AS FLOAT)/SUM(dt.patient_experiencing_pain) AS 'Patients_with_intervention_recorded'
		 ,COUNT(DISTINCT CONCAT(dt.EncounterEpicCsn,dt.Department,dt.TransferStartDate) ) AS 'Number_of_ICU_stays'
		 ,COUNT(dt.TakenInstant) AS 'Number_of_pain_assemnts'
		 

FROM #dt AS dt
WHERE dt.TakenInstant IS NOT NULL 
      AND
	  YEAR(dt.TakenInstant) = 2021
GROUP BY DATEFROMPARTS(YEAR(dt.TransferEndDate),MONTH(dt.TransferEndDate),'01')

)  

SELECT agg.*
       ,icut.Total_time_in_icu
	   ,(CAST(agg.Number_of_pain_assemnts AS FLOAT)/icut.Total_time_in_icu)*24 AS 'average_number_of_pain_assesmwnts_per_day'
FROM #agg AS agg 
INNER JOIN (SELECT DATEFROMPARTS(YEAR(co.TransferEndDate),MONTH(co.TransferEndDate),'01') AS 'month'
                  ,SUM(co.Meas_LoS_Hrs) AS 'Total_time_in_icu'
           FROM #cohort AS co
		   WHERE CONCAT(co.EncounterEpicCsn,co.Department,co.TransferStartDate) IN (SELECT DISTINCT CONCAT(EncounterEpicCsn,Department,TransferStartDate) FROM #pain WHERE TakenInstant IS NOT NULL)
		   GROUP BY DATEFROMPARTS(YEAR(co.TransferEndDate),MONTH(co.TransferEndDate),'01')) AS icut ON agg.month = icut.month

ORDER BY agg.month




-- medication
;with #meds AS(
SELECT ef.EncounterEpicCsn
       ,pd.PrimaryMrn
	   ,pd.FirstName
	   ,pd.LastName
       ,ord.MedicationOrderKey
	   ,meddim.PharmaceuticalClass
	   ,meddim.PharmaceuticalSubclass
	   ,meddim.SimpleGenericName
--	   ,ROW_NUMBER() OVER(PARTITION BY ef.EncounterEpicCsn,meddim.SimpleGenericName ,pain.Department,pain.TransferStartDate ORDER BY ord.OrderedInstant ASC ) AS 'medication_oder _number'
	   ,ROW_NUMBER() OVER (PARTITION BY ef.EncounterEpicCsn, meddim.SimpleGenericName,admin.AdministrationInstant,pain.Department,pain.TransferStartDate ORDER BY score.TakenInstant DESC) AS 'pain_row_num'
	   ,ord.OrderedInstant
	   ,admin.ScheduledAdministrationInstant
	   ,admin.AdministrationInstant
	   ,admin.Dose
	   ,admin.DoseUnit
	   ,pain.Department
	   ,pain.TransferStartDate
	   ,pain.TransferEndDate
	   ,score.TakenInstant
	   ,score.Pain_socre
	   ,score.at_rest
	   ,score.movement

	
FROM dbo.MedicationOrderFact AS ord
--LEFT JOIN dbo.MedicationDispenseFact AS discp ON ord.MedicationOrderKey = discp.MedicationOrderKey
LEFT JOIN dbo.MedicationAdministrationFact AS admin ON ord.MedicationOrderKey = admin.MedicationOrderKey
LEFT JOIN dbo.EncounterFact AS ef ON ord.EncounterKey = ef.EncounterKey
LEFT JOIN dbo.MedicationDim AS meddim ON ord.MedicationKey = meddim.MedicationKey
LEFT JOIN PatientDim AS pd ON ef.PatientDurableKey = pd.DurableKey AND pd.IsCurrent = 1
INNER JOIN (SELECT DISTINCT EncounterEpicCsn
                            ,Department
							,TransferStartDate
							,TransferEndDate
							FROM #pain) AS pain ON ef.EncounterEpicCsn = pain.EncounterEpicCsn 
                        AND pain.TransferStartDate <= admin.AdministrationInstant
						AND pain.TransferEndDate >= admin.AdministrationInstant
LEFT JOIN #pain AS score ON ef.EncounterEpicCsn = score.EncounterEpicCsn
                            AND admin.AdministrationInstant >= score.TakenInstant

WHERE (meddim.PharmaceuticalClass = 'Analgesics'
       OR
	   meddim.PharmaceuticalSubclass = 'Other intravenous anaesthetics'
	   )
      AND 
	  ord.Class <> 'Fill Later'
	  AND
	  YEAR(pain.TransferEndDate) = 2021
      AND
	  MONTH(pain.TransferEndDate) =10



/*ORDER BY ef.EncounterEpicCsn
         ,meddim.SimpleGenericName 
		 ,pain.Department
		 ,pain.TransferStartDate*/

)	  
SELECT * 
       ,ROW_NUMBER() OVER(PARTITION BY EncounterEpicCsn,SimpleGenericName ,Department,TransferStartDate ORDER BY AdministrationInstant ASC ) AS 'medication_oder _number'
FROM #meds
WHERE pain_row_num = 1
ORDER BY EncounterEpicCsn
         ,SimpleGenericName 
		 ,AdministrationInstant
		 ,Department
		 ,TransferStartDate	 
