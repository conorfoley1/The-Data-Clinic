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


-- flow sheet data 

DROP TABLE IF EXISTS #flow
SELECT flow.EncounterKey
       ,ef.EncounterEpicCsn 
       ,flow.TakenInstant
	   ,flowtemp.FlowsheetTemplateKey
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
WHERE   
       flowdim.FlowsheetRowEpicId IN ( '5' -- BP
	                                   ,'8' --Pulse
									   ,'14'--weight
									   ,'38459' -- METARAMINOL PVI
									   ,'12946' -- METARAMINOL volume
									   )
		AND 
		ef.EncounterEpicCsn IN (SELECT EncounterEpicCsn FROM #cohort)
	   



-- All ECHOCARDIOGRAM proc keys
DROP TABLE IF EXISTS #echo
SELECT DISTINCT pd.ProcedureKey
INTO #echo
FROM ProcedureDim AS pd
WHERE 
     (Name LIKE '%ECHO%'
	  OR
	  Name LIKE '%TRANSTHORACIC%'
	  )


-- ECHO results 
DROP TABLE IF EXISTS #img
SELECT   ef.EncounterEpicCsn
        ,img.PatientDurableKey
        ,img.ImagingOrderEpicId
        ,img.OrderingInstant
		,img.ImagingKey
		,pd.Name
		,pd.Category
		,img.ProtocolledInstant
		,img.ScheduledExamInstant
		,img.ExamStartInstant
		,img.ExamEndInstant
		,img.PreliminaryInstant
		,img.FinalizingInstant
		,txt.Impression
	    ,txt.Narrative
		,echo.FindingSmartDataEpicId
		,echo.FindingType
		,echo.FindingName
		,echo.StringValue
		,echo.NumericValue
 INTO #img      
FROM dbo.EchoFindingFact AS echo
LEFT JOIN AttributeDim AS ad ON echo.FindingAttributeKey = ad.AttributeKey
LEFT JOIN ImagingFact AS img ON echo.ImagingKey = img.ImagingKey
LEFT JOIN dbo.ProcedureDim AS pd ON img.FirstProcedureKey = pd.ProcedureKey
LEFT JOIN EncounterFact AS ef ON img.OrderingEncounterKey = ef.EncounterKey
LEFT JOIN ImagingTextFact AS txt ON img.ImagingKey = txt.ImagingKey
WHERE pd.ProcedureKey IN (SELECT ProcedureKey FROM #echo)

-- flow sheet data 
-- BP & pulse prior to exam

--BP data 
DROP TABLE IF EXISTS #bp
SELECT *
INTO #bp
FROM #flow
WHERE FlowsheetRowEpicId = 5

DROP TABLE IF EXISTS #bptemp
SELECT img.EncounterEpicCsn
       ,img.ImagingOrderEpicId
	   ,bp.Value AS 'blood_pressure'
       ,ROW_NUMBER() OVER(PARTITION BY img.EncounterEpicCsn,img.ImagingOrderEpicId ORDER BY bp.TakenInstant DESC ) AS 'bp_order'
	   ,DATEDIFF(HOUR,bp.TakenInstant,img.ExamStartInstant) AS 'time_between_bp_and_scan'
INTO #bptemp
FROM (SELECT DISTINCT EncounterEpicCsn
                     ,ImagingOrderEpicId
					 ,ExamStartInstant FROM #img) AS img
INNER JOIN #bp AS bp  ON img.EncounterEpicCsn  = bp.EncounterEpicCsn 
                     AND bp.TakenInstant <= img.ExamStartInstant
					 AND DATEDIFF(HOUR,bp.TakenInstant,img.ExamStartInstant) <= 24
WHERE value <> ''



DROP TABLE IF EXISTS #bpprior
SELECT *
INTO #bpprior
FROM #bptemp AS temp
WHERE temp.bp_order = 1

 


-- Pulse 
DROP TABLE IF EXISTS #pulse
SELECT *
INTO #pulse
FROM #flow
WHERE FlowsheetRowEpicId = 8

DROP TABLE IF EXISTS #pulsetemp
SELECT img.EncounterEpicCsn
       ,img.ImagingOrderEpicId
	   ,pul.Value AS 'pulse'
       ,ROW_NUMBER() OVER(PARTITION BY img.EncounterEpicCsn,img.ImagingOrderEpicId ORDER BY pul.TakenInstant DESC ) AS 'pulse_order'
	   ,DATEDIFF(HOUR,pul.TakenInstant,img.ExamStartInstant) AS 'time_between_pulse_and_scan'
INTO #pulsetemp
FROM (SELECT DISTINCT EncounterEpicCsn
                     ,ImagingOrderEpicId
					 ,ExamStartInstant FROM #img) AS img
INNER JOIN #pulse AS pul  ON img.EncounterEpicCsn  = pul.EncounterEpicCsn 
                     AND pul.TakenInstant <= img.ExamStartInstant
					 AND DATEDIFF(HOUR,pul.TakenInstant,img.ExamStartInstant) <= 24
WHERE value <> ''
ORDER BY img.EncounterEpicCsn
        ,img.ImagingOrderEpicId


DROP TABLE IF EXISTS #pulseprior
SELECT *
INTO #pulseprior
FROM #pulsetemp AS temp
WHERE temp.pulse_order = 1


--weight 
DROP TABLE IF EXISTS #weight
SELECT *
INTO #weight
FROM #flow
WHERE FlowsheetRowEpicId = 14

DROP TABLE IF EXISTS #weighttemp
SELECT img.EncounterEpicCsn
       ,img.ImagingOrderEpicId
	   ,CAST(wght.Value AS FLOAT)/1000 AS 'weight_kg'
       ,ROW_NUMBER() OVER(PARTITION BY img.EncounterEpicCsn,img.ImagingOrderEpicId ORDER BY wght.TakenInstant DESC ) AS 'weight_order'
	   ,DATEDIFF(HOUR,wght.TakenInstant,img.ExamStartInstant) AS 'time_between_pulse_and_scan'
INTO #weighttemp
FROM (SELECT DISTINCT EncounterEpicCsn
                     ,ImagingOrderEpicId
					 ,ExamStartInstant FROM #img) AS img
INNER JOIN #weight AS wght  ON img.EncounterEpicCsn  = wght.EncounterEpicCsn 
                     AND wght.TakenInstant <= img.ExamStartInstant
	
WHERE value <> ''
ORDER BY img.EncounterEpicCsn
        ,img.ImagingOrderEpicId


DROP TABLE IF EXISTS #weightprior
SELECT *
INTO #weightprior
FROM #weighttemp AS temp
WHERE temp.weight_order = 1

-- lab data 

-- CRP
DROP TABLE IF EXISTS #crp
SELECT 
		 ef.EncounterEpicCsn
		,lcd.LabComponentKey
		,lcd.Name
		,lcrf.Value
		,lcrf.NumericValue
		,lcrf.Unit
		,lcrf.CollectionInstant
		,lcrf.ResultInstant
INTO #crp
FROM dbo.LabComponentResultFact AS lcrf

LEFT JOIN LabComponentDim lcd ON lcrf.LabComponentKey = lcd.LabComponentKey

LEFT JOIN EncounterFact AS ef ON lcrf.EncounterKey = ef.EncounterKey

WHERE ( lcrf.LabComponentKey IN ('22000','3498','3423','1257','2835','8908','9073','22001')
         AND 
		ef.EncounterEpicCsn IN (SELECT EncounterEpicCsn FROM #cohort)
		AND 
		lcrf.Value NOT LIKE '%Cancelled%'
		)


DROP TABLE IF EXISTS #crptemp
SELECT img.EncounterEpicCsn
       ,img.ImagingOrderEpicId
	   ,crp.Value AS 'crp_mg/L'
	   ,crp.CollectionInstant
       ,ROW_NUMBER() OVER(PARTITION BY img.EncounterEpicCsn,img.ImagingOrderEpicId ORDER BY crp.CollectionInstant DESC ) AS 'crp_order'
	   ,DATEDIFF(HOUR,crp.CollectionInstant,img.ExamStartInstant) AS 'crp_to_recho'
INTO #crptemp
FROM (SELECT DISTINCT EncounterEpicCsn
                     ,ImagingOrderEpicId
					 ,ExamStartInstant FROM #img) AS img
					 
INNER JOIN #crp AS crp ON img.EncounterEpicCsn = crp.EncounterEpicCsn
                      AND crp.CollectionInstant <= img.ExamStartInstant
					  AND DATEDIFF(HOUR,crp.CollectionInstant,img.ExamStartInstant) <= 24
                     

DROP TABLE IF EXISTS #crpprior
SELECT *
INTO #crpprior
FROM #crptemp AS temp
WHERE temp.crp_order = 1

/*-- blood culture 
SELECT ef.EncounterEpicCsn
       ,pd.ProcedureKey
       ,pd.Name AS 'procedure'
       ,dim.Name AS 'result_name'
       ,lab.CollectionInstant
	   ,lab.ResultInstant
	   ,lab.Value

FROM LabComponentResultFact AS lab
LEFT JOIN LabComponentResultTextFact AS txt ON lab.LabComponentResultKey = txt.LabComponentResultKey
LEFT JOIN  LabComponentDim AS dim ON lab.LabComponentKey = dim.LabComponentKey
LEFT JOIN ProcedureDim AS pd ON lab.ProcedureKey = pd.ProcedureKey
LEFT JOIN EncounterFact AS ef ON lab.EncounterKey = ef.EncounterKey
WHERE dim.Name = 'Culture'
      AND pd.ProcedureKey IN ('4054')
	  AND ef.EncounterEpicCsn IN (SELECT EncounterEpicCsn FROM #cohort)
/*	  AND (
	      lab.Value LIKE '%mixed%'
		  OR
		  lab.Value LIKE '%gram%'
		  OR
		  lab.Value LIKE '%fungal%'
		  OR
		  lab.Value LIKE '%mould%'
		  OR
		  lab.Value LIKE '%Nontuberculosus%'
		  OR
		  lab.Value LIKE '%yeast%'
		  OR
		  lab.Value LIKE '%Staphylococcus%'
	      )  */
*/

--urine culture 
DROP TABLE IF EXISTS #urine
SELECT ef.EncounterEpicCsn
       ,pd.ProcedureKey
       ,pd.Name AS 'procedure'
       ,dim.Name AS 'result_name'
       ,lab.CollectionInstant
	   ,lab.ResultInstant
	   ,lab.Value
INTO #urine
FROM LabComponentResultFact AS lab
LEFT JOIN LabComponentResultTextFact AS txt ON lab.LabComponentResultKey = txt.LabComponentResultKey
LEFT JOIN  LabComponentDim AS dim ON lab.LabComponentKey = dim.LabComponentKey
LEFT JOIN ProcedureDim AS pd ON lab.ProcedureKey = pd.ProcedureKey
LEFT JOIN EncounterFact AS ef ON lab.EncounterKey = ef.EncounterKey
WHERE dim.Name = 'Culture'
      AND pd.ProcedureKey IN ('4142')
	  AND ef.EncounterEpicCsn IN (SELECT EncounterEpicCsn FROM #cohort)
	  AND lab.Value NOT LIKE '%no growth%'
	  AND (
	      lab.Value LIKE '%mixed%'
		  OR
		  lab.Value LIKE '%cfu%'
		  OR
		  lab.Value LIKE '%coliform%'
	      )

DROP TABLE IF EXISTS #urntemp
SELECT img.EncounterEpicCsn
       ,img.ImagingOrderEpicId
	   ,urn.Value AS 'urine_culture'
	   ,urn.CollectionInstant
       ,ROW_NUMBER() OVER(PARTITION BY img.EncounterEpicCsn,img.ImagingOrderEpicId ORDER BY urn.CollectionInstant DESC ) AS 'culture_order'
	   ,DATEDIFF(HOUR,urn.CollectionInstant,img.ExamStartInstant) AS 'crp_to_recho'
INTO #urntemp
FROM (SELECT DISTINCT EncounterEpicCsn
                     ,ImagingOrderEpicId
					 ,ExamStartInstant FROM #img) AS img
					 
INNER JOIN #urine AS urn ON img.EncounterEpicCsn = urn.EncounterEpicCsn
                      AND urn.CollectionInstant <= img.ExamStartInstant

                     

DROP TABLE IF EXISTS #urnprior
SELECT *
INTO #urnprior
FROM #urntemp AS temp
WHERE temp.culture_order = 1

-- Mediacation

--Antibacterial drugs
DROP TABLE IF EXISTS #antibac
SELECT ef.EncounterEpicCsn
       ,ord.MedicationOrderKey
	   ,meddim.PharmaceuticalClass
	   ,meddim.PharmaceuticalSubclass
	   ,meddim.SimpleGenericName
	   ,ord.Class
	   ,ord.OrderedInstant
	   ,admin.AdministrationInstant
INTO #antibac
FROM dbo.MedicationOrderFact AS ord
LEFT JOIN dbo.MedicationAdministrationFact AS admin ON ord.MedicationOrderKey = admin.MedicationOrderKey
LEFT JOIN dbo.EncounterFact AS ef ON ord.EncounterKey = ef.EncounterKey
LEFT JOIN dbo.MedicationDim AS meddim ON ord.MedicationKey = meddim.MedicationKey
WHERE ( 
	   meddim.PharmaceuticalClass = 'Antibacterial drugs'
	   AND 
	   ord.Class <> 'Fill Later'
	   AND 
	   DATEDIFF(minute,ord.OrderedInstant,admin.AdministrationInstant) >= 0
	   AND 
	   ef.EncounterEpicCsn IN (SELECT EncounterEpicCsn FROM #cohort)
	   )

-- ordered prior to scan, administered 12 hours either side 
DROP TABLE IF EXISTS #antibacscan
SELECT img.EncounterEpicCsn
       ,img.ImagingOrderEpicId
	   ,1 AS 'on_antibacterial_at_scan'

INTO #antibacscan
FROM (SELECT DISTINCT EncounterEpicCsn
                     ,ImagingOrderEpicId
					 ,ExamStartInstant FROM #img) AS img
					 
INNER JOIN #antibac AS anti ON img.EncounterEpicCsn = anti.EncounterEpicCsn
                      AND anti.OrderedInstant <= img.ExamStartInstant
					  AND DATEDIFF(HOUR,anti.AdministrationInstant,img.ExamStartInstant) >= -12
					  AND DATEDIFF(HOUR,anti.AdministrationInstant,img.ExamStartInstant) <= 12

GROUP BY img.EncounterEpicCsn
       ,img.ImagingOrderEpicId					  

-- noradrenaline 
DROP TABLE IF EXISTS #noradrenaline
SELECT ef.EncounterEpicCsn
       ,ord.MedicationOrderKey
	   ,meddim.PharmaceuticalClass
	   ,meddim.PharmaceuticalSubclass
	   ,meddim.SimpleGenericName
	   ,ord.Class
	   ,ord.OrderedInstant
	   ,admin.AdministrationInstant
INTO #noradrenaline
FROM dbo.MedicationOrderFact AS ord
LEFT JOIN dbo.MedicationAdministrationFact AS admin ON ord.MedicationOrderKey = admin.MedicationOrderKey
LEFT JOIN dbo.EncounterFact AS ef ON ord.EncounterKey = ef.EncounterKey
LEFT JOIN dbo.MedicationDim AS meddim ON ord.MedicationKey = meddim.MedicationKey
WHERE ( 
	   meddim.SimpleGenericName LIKE '%Noradrenaline%'
	   AND 
	   ord.Class <> 'Fill Later'
	   AND 
	   DATEDIFF(minute,ord.OrderedInstant,admin.AdministrationInstant) >= 0 
	   AND 
	   ef.EncounterEpicCsn IN (SELECT EncounterEpicCsn FROM #cohort)
	   )

DROP TABLE IF EXISTS #noradrenalinetemp
SELECT img.EncounterEpicCsn
       ,img.ImagingOrderEpicId
	   ,nora.AdministrationInstant
	   ,ROW_NUMBER() OVER(PARTITION BY img.EncounterEpicCsn,img.ImagingOrderEpicId ORDER BY nora.AdministrationInstant DESC ) AS 'noradrenalinescan_order'
INTO #noradrenalinetemp
FROM (SELECT DISTINCT EncounterEpicCsn
                     ,ImagingOrderEpicId
					 ,ExamStartInstant FROM #img) AS img
					 
INNER JOIN #noradrenaline AS nora ON img.EncounterEpicCsn = nora.EncounterEpicCsn
                      AND nora.AdministrationInstant <= img.ExamStartInstant

DROP TABLE IF EXISTS #noradrenalinescan
SELECT *
INTO #noradrenalinescan
FROM #noradrenalinetemp
WHERE noradrenalinescan_order = 1

-- Metaraminol 

DROP TABLE IF EXISTS #metaraminol
SELECT *
INTO #metaraminol
FROM #flow
WHERE FlowsheetRowEpicId IN ('38459')

DROP TABLE IF EXISTS #metaraminoltemp
SELECT img.EncounterEpicCsn
       ,img.ImagingOrderEpicId
	   ,met.Value AS 'Metaraminol_dose'
	   ,met.TakenInstant
       ,ROW_NUMBER() OVER(PARTITION BY img.EncounterEpicCsn,img.ImagingOrderEpicId ORDER BY met.TakenInstant DESC ) AS 'Metaraminol_order'
INTO #metaraminoltemp 
FROM (SELECT DISTINCT EncounterEpicCsn
                     ,ImagingOrderEpicId
					 ,ExamStartInstant FROM #img) AS img
INNER JOIN #weight AS met  ON img.EncounterEpicCsn  = met.EncounterEpicCsn 
                     AND met.TakenInstant <= img.ExamStartInstant
					 AND DATEDIFF(HOUR,met.TakenInstant,img.ExamStartInstant) >= -12
					 AND DATEDIFF(HOUR,met.TakenInstant,img.ExamStartInstant) <=  12
	
WHERE value <> ''
ORDER BY img.EncounterEpicCsn
        ,img.ImagingOrderEpicId


DROP TABLE IF EXISTS #metaraminolprior
SELECT *
INTO #metaraminolprior
FROM #metaraminoltemp AS temp
WHERE temp.Metaraminol_order = 1



-- HaemOnc diagnosis 
DROP TABLE IF EXISTS #HaemOnc
SELECT DISTINCT  dxt.DiagnosisTerminologykey
                ,dxt.DiagnosisKey
				,dxt.Value
				,dxt.DisplayString
				,dx.Name
INTO #HaemOnc
FROM dbo.DiagnosisTerminologyDim dxt
LEFT JOIN DiagnosisDim dx ON dxt.DiagnosisKey = dx.DiagnosisKey
WHERE Type IN ('ICD-10-CM','ICD-10-UK')
AND LEFT(Value,3) IN ('C81'
					 ,'C82'
					 ,'C83'
					 ,'C84'
					 ,'C85'
					 ,'C86'
					 ,'C88'
					 ,'C90'
					 ,'C91'
					 ,'C92'
					 ,'C93'
					 ,'C94'
					 ,'C95'
					 ,'C96'
					 ,'D47'
					 )

      --AND Value IN ('C88.0'
				  -- ,'C90.0'
				  -- ,'C90.00'
				  -- ,'D47.2'
				  -- ,'C91'
				  -- ,'C91.0'
				  -- )
				--,'
/*
select distinct 
Type
,Value
,NameAndCode 
from DiagnosisTerminologyDim 
where 
LEFT(Value,3) IN ('C81'
					 ,'C82'
					 ,'C84'
					 ,'C85'
					 ,'C88'
					 ,'C90'
					 ,'C91'
					 ,'C92'
					 ,'C96'
					 ,'D47'
					 )
and Type = ('ICD-10-CM')
order by 
Value
,Type
*/

DROP TABLE IF EXISTS #HaemOncDx
SELECT DISTINCT ef.EncounterEpicCsn
--       ,sep.Value
--	   ,sep.DisplayString
	   ,start.DateValue AS 'diagnosis_date'
	   ,CASE 
	    WHEN  diag.EndDateKey = -2 THEN CAST(GETDATE() AS DATE)  
		ELSE cure.DateValue  
		END AS 'diagnosis_end_date'		
		,ho.DisplayString
		,ho.Value
		,ho.Name
INTO  #HaemOncDx
FROM dbo.DiagnosisEventFact AS diag
INNER JOIN #HaemOnc AS ho ON diag.DiagnosisKey = ho.DiagnosisKey
LEFT JOIN dbo.EncounterFact AS ef ON diag.EncounterKey = ef.EncounterKey
LEFT JOIN dbo.DateDim AS start ON diag.StartDateKey = start.DateKey
LEFT JOIN dbo.DateDim AS cure ON diag.StartDateKey = cure.DateKey
WHERE ef.EncounterEpicCsn IN (SELECT c.EncounterEpicCsn FROM #cohort c)

DROP TABLE IF EXISTS #HaemOncPrior
SELECT          img.EncounterEpicCsn
               ,img.ImagingOrderEpicId
	           ,1 AS 'haemOnc_diagnosis_prior_to_scan'
			   ,MAX(ho.diagnosis_date) AS 'latest_haemOnc_diagnosis'
INTO #HaemOncPrior
FROM (SELECT DISTINCT      EncounterEpicCsn
                          ,ImagingOrderEpicId
						  ,OrderingInstant
						  ,ExamStartInstant
                             FROM #img) AS img
INNER JOIN  #HaemOncDx AS ho ON img.EncounterEpicCsn = ho.EncounterEpicCsn
                          AND img.OrderingInstant >= ho.diagnosis_date

GROUP BY img.EncounterEpicCsn
         ,img.ImagingOrderEpicId

		 -- check problem lists for HaemOnc too
DROP TABLE IF EXISTS #HaemOncProbList
select
pl.PatientKey
,pl.PatientDurableKey
,ho.*
INTO #HaemOncProbList
from ProblemListFact pl
INNER JOIN #HaemOnc ho ON pl.DiagnosisKey = ho.DiagnosisKey


--
--select * from #HaemOncDx d
--left join EncounterFact enc ON enc.EncounterEpicCsn = d.EncounterEpicCsn
--left join PatientDim pat ON enc.PatientKey = pat.PatientKey
----left join DiagnosisTerminologyDim dxt On d
--where

/*
select
pat.DurableKey 'PatientDurableKey'
,dx.Name 'DiagnosisDimName'
,dx.DiagnosisEpicId 'DiagnosisDimDiagnosisEpicId'
,plf.DiagnosisKey
,plf.StartDateKey
,dxt.Value 'DiagnosisTerminologyDimValue'
,dxt.NameAndCode 'DiagnosisTerminologyDimNameAndCode'
,dxt.DiagnosisEpicId 
,dx.DiagnosisKey
,dxt.Type
from ProblemListFact plf
LEFT JOIN PatientDim pat ON plf.PatientDurableKey = pat.DurableKey
	and pat.IsCurrent = 1
LEFT JOIN DiagnosisDim dx ON plf.DiagnosisKey = dx.DiagnosisKey
LEFT JOIN DiagnosisTerminologyDim dxt ON dx.DiagnosisKey = dxt.DiagnosisKey
	and dxt.Type IN ('ICD-10-CM','ICD-10-UK')
WHERE

*/
--select * from DiagnosisTerminologyDim dxt where 

--select * from DiagnosisTerminologyDim dxt where NameAndCode LIKE '%Large%' and NameAndCode LIKE '%cell%' and NameAndCode LIKE '%anaplastic%' and NameAndCode LIKE '%lymphoma%' and Type LIKE '%ICD-10%'

--select
--*
--from DiagnosisTerminologyDim dxt where dxt.DiagnosisKey = 1254194
--

--echo count
DROP TABLE IF EXISTS #echocount
SELECT  img.EncounterEpicCsn
        ,COUNT(DISTINCT img.ImagingOrderEpicId) AS 'number_echos_during_spell'
INTO #echocount
FROM #cohort AS co
LEFT JOIN (SELECT DISTINCT EncounterEpicCsn
                           ,PatientDurableKey
                           ,ImagingKey
						   ,ImagingOrderEpicId
						   ,Name
						   ,OrderingInstant
						   ,ExamStartInstant
						   ,FinalizingInstant
						   ,Narrative
		   FROM #img) AS img ON  co.EncounterEpicCsn = img.EncounterEpicCsn 
		                     AND co.TransferStartDate <= img.OrderingInstant
							 AND co.TransferEndDate >= img.OrderingInstant
WHERE img.ImagingKey IS NOT NULL 
GROUP BY img.EncounterEpicCsn

-- imaging table
DROP TABLE IF EXISTS #echoimg
SELECT  pd.PrimaryMrn
       ,pd.FirstName
	   ,pd.LastName
       ,DATEDIFF(YEAR,pd.BirthDate,co.TransferStartDate) AS 'age_at_admission_to_icu'
       ,co.EncounterEpicCsn
	   ,echoc.number_echos_during_spell
	   ,ROW_NUMBER() OVER(PARTITION BY co.EncounterEpicCsn ORDER BY img.OrderingInstant  ) AS 'echo_number'
	   ,co.Department
	   ,co.TransferStartDate AS 'admitted_to_itu'
	   ,co.TransferEndDate AS 'discharged_from_itu'
	   ,co.Meas_LoS_Days
	   ,co.Meas_LoS_Hrs
	   ,co.Meas_Death
	   ,hop.haemOnc_diagnosis_prior_to_scan
	   ,hop.latest_haemOnc_diagnosis
	   ,STUFF(
			   (SELECT '; ' + pl.DisplayString
			   
			   FROM #HaemOncProbList pl
			   WHERE 
			   pl.PatientDurableKey = img.PatientDurableKey
			   FOR XML PATH('')
			   ) 
			   , 1, 1,'') AS 'HaemOnc_ProbList'
	   --,hpl.DisplayString AS 'HaemOnc_ProbList'
       ,urnflag.Positive_urine_culture
	   ,antibac.on_antibacterial_at_scan
	   ,nora.AdministrationInstant AS 'last_dose_of_noradrenaline'
	   ,met.TakenInstant AS 'last_dose_of_metaraminol'
       ,img.ImagingKey
	   ,img.Name
	   ,img.OrderingInstant
	   ,img.ExamStartInstant
	   ,img.FinalizingInstant
	   ,img.Narrative
	   ,gls.NumericValue AS 'GLS'
	   ,vti.NumericValue AS 'VTI'
	   ,sep.NumericValue AS 'e` septal'
	   ,lat.NumericValue AS 'e` lateral'
	   ,tapse.NumericValue AS 'TAPSE'
	   ,trjet.NumericValue AS 'TR jet velocity'
	   ,trgrad.NumericValue AS 'TR gradient'
	   ,eeratio.NumericValue AS 'E/E` ratio'
	   ,earatio.NumericValue AS 'E/A` ratio'
	   ,lvef.StringValue AS 'LVEF'
	   ,bp.blood_pressure
	   ,pul.pulse
	   ,crp.[crp_mg/L]
	   ,wght.weight_kg
	   ,urn.urine_culture 
INTO #echoimg
FROM #cohort AS co
LEFT JOIN (SELECT DISTINCT EncounterEpicCsn
                           ,PatientDurableKey
                           ,ImagingKey
						   ,ImagingOrderEpicId
						   ,Name
						   ,OrderingInstant
						   ,ExamStartInstant
						   ,FinalizingInstant
						   ,Narrative
		   FROM #img) AS img ON  co.EncounterEpicCsn = img.EncounterEpicCsn 
		                     AND co.TransferStartDate <= img.OrderingInstant
							 AND co.TransferEndDate >= img.OrderingInstant
-- GLS
LEFT JOIN (SELECT *
           FROM #img
           WHERE FindingName = 'GLS') AS gls ON img.ImagingKey = gls.ImagingKey

--VTI 
LEFT JOIN (SELECT *
           FROM #img
           WHERE FindingName = 'LVOT peak VTI') AS vti ON img.ImagingKey = vti.ImagingKey

--e' septal
LEFT JOIN (SELECT *
           FROM #img
           WHERE FindingName LIKE '%MV septal e%') AS sep ON img.ImagingKey = sep.ImagingKey

--e' lateral
LEFT JOIN (SELECT *
           FROM #img
           WHERE FindingName LIKE '%MV lateral e%') AS lat ON img.ImagingKey = lat.ImagingKey

--TAPSE results 
LEFT JOIN (SELECT *
           FROM #img
           WHERE FindingName LIKE '%TAPSE%') AS tapse ON img.ImagingKey = tapse.ImagingKey

--TR jet
LEFT JOIN (SELECT *
           FROM #img
           WHERE FindingName = 'TR jet velocity') AS trjet ON img.ImagingKey = trjet.ImagingKey

--TR grad
LEFT JOIN (SELECT *
           FROM #img
           WHERE FindingName = 'TR gradient') AS trgrad ON img.ImagingKey = trgrad.ImagingKey

--E/E' results 
LEFT JOIN (SELECT *
           FROM #img
           WHERE FindingName LIKE '%E/E%') AS eeratio ON img.ImagingKey = eeratio.ImagingKey

-- E/A ratio 
LEFT JOIN (SELECT *
           FROM #img
           WHERE FindingName = 'E/A ratio') AS earatio ON img.ImagingKey = earatio.ImagingKey

-- LVEF
LEFT JOIN (SELECT *
           FROM #img
           WHERE FindingName = 'ejection fraction'
		   AND FindingType =  'Left Ventricle') AS lvef ON img.ImagingKey = lvef.ImagingKey
-- BP  up to 24 hours prior to exam
LEFT JOIN #bpprior AS bp ON  img.ImagingOrderEpicId = bp.ImagingOrderEpicId

-- pulse up to 24 hours prior to exam
LEFT JOIN #pulseprior AS pul ON img.ImagingOrderEpicId = pul.ImagingOrderEpicId

-- weight prior to exam
LEFT JOIN #weightprior AS wght ON img.ImagingOrderEpicId = wght.ImagingOrderEpicId


-- CRP  up to 24 hours prior to exam
LEFT JOIN #crpprior  AS crp  ON img.ImagingOrderEpicId = crp.ImagingOrderEpicId

--positve urine cultre 
LEFT JOIN (SELECT EncounterEpicCsn
                 , 1 AS 'Positive_urine_culture'
           FROM #urine
		   GROUP BY EncounterEpicCsn)  AS urnflag ON img.EncounterEpicCsn = urnflag.EncounterEpicCsn

--positve urine cultre prior to exam
LEFT JOIN #urnprior AS urn ON img.ImagingOrderEpicId = urn.ImagingOrderEpicId

--On anitbiotics at time of scan 
LEFT JOIN #antibacscan AS antibac  ON   img.ImagingOrderEpicId = antibac.ImagingOrderEpicId

--last dose of noradrenaline prior to scan
LEFT JOIN #noradrenalinescan AS nora ON img.ImagingOrderEpicId = nora.ImagingOrderEpicId 

-- last dose of metaraminol prior to scan
LEFT JOIN #metaraminolprior AS met ON img.ImagingOrderEpicId= met.ImagingOrderEpicId

--Patient details
LEFT JOIN PatientDim AS pd ON img.PatientDurableKey = pd.DurableKey
                           AND pd.IsCurrent = 1

-- sepsis diagnosis prior to scan order 
LEFT JOIN #HaemOncPrior AS hop ON  img.ImagingOrderEpicId = hop.ImagingOrderEpicId

-- problem list?
--left join #HaemOncProbList hpl ON img.PatientDurableKey = hpl.PatientDurableKey

--number of echos during spell
LEFT JOIN #echocount AS echoc ON co.EncounterEpicCsn = echoc.EncounterEpicCsn

WHERE img.ImagingKey IS NOT NULL 
      AND
	  echoc.number_echos_during_spell >=1
ORDER BY co.TransferEndDate DESC






SELECT DISTINCT *
FROM #echoimg AS img
WHERE 1=1
AND (img.haemOnc_diagnosis_prior_to_scan = 1
	OR img.latest_haemOnc_diagnosis IS NOT NULL
	OR img.HaemOnc_ProbList IS NOT NULL
	)
ORDER BY img.EncounterEpicCsn
         ,img.echo_number





-- culture working 
/*
SELECT ef.EncounterEpicCsn
       ,pd.ProcedureKey
       ,pd.Name AS 'procedure'
       ,dim.Name AS 'result_name'
       ,lab.CollectionInstant
	   ,lab.ResultInstant
	   ,lab.Value

FROM LabComponentResultFact AS lab
LEFT JOIN LabComponentResultTextFact AS txt ON lab.LabComponentResultKey = txt.LabComponentResultKey
LEFT JOIN  LabComponentDim AS dim ON lab.LabComponentKey = dim.LabComponentKey
LEFT JOIN ProcedureDim AS pd ON lab.ProcedureKey = pd.ProcedureKey
LEFT JOIN EncounterFact AS ef ON lab.EncounterKey = ef.EncounterKey
WHERE dim.Name = 'Culture'
      AND pd.ProcedureKey IN ('4054','4142')
	  AND lab.Value <> ''

SELECT DISTINCT lab.Value
FROM LabComponentResultFact AS lab
LEFT JOIN LabComponentResultTextFact AS txt ON lab.LabComponentResultKey = txt.LabComponentResultKey
LEFT JOIN  LabComponentDim AS dim ON lab.LabComponentKey = dim.LabComponentKey
LEFT JOIN ProcedureDim AS pd ON lab.ProcedureKey = pd.ProcedureKey
LEFT JOIN EncounterFact AS ef ON lab.EncounterKey = ef.EncounterKey
WHERE dim.Name = 'Culture'
      AND pd.ProcedureKey IN ('4142')
	  AND lab.Value NOT LIKE '%no growth%'
	  AND (
	      lab.Value LIKE '%mixed%'
		  OR
		  lab.Value LIKE '%cfu%'
		  OR
		  lab.Value LIKE '%coliform%'
	      )
*/

-- 120
