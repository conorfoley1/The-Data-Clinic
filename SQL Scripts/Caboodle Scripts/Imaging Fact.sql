----------------------------------------
--Name:Imageing Results 
--Description: general data on imaging such as key time stams and times beteew this events
--Data Source: CABOODLE_REPORT
---------------------------------------

--Filtering to find a specific type of imageing 
-- Imageing data is mainly filtered in one of two ways, first is by the scan type i.e. xray, ct, etc. and for a specific imaging test such as chest xray 
-- The below table can also be used to filter the imaging data 
-- DROP TABLE IF EXISTS #proc
SELECT pro.ProcedureKey
--INTO #proc
FROM ProcedureDim AS pro		
WHERE pro.ProcedureKey IN (SELECT FirstProcedureKey FROM ImagingFact)
-- The below can be used to be filtered be based on scan type , for example if you were only intrested in MRI's then you woul set pro.Category = 'IMG MRI PROCEDURES'
--   AND pro.Category = ''
-- The code below can be used to filter for a specific scan, for example if you were 
-- AND pro.Name LIKE '%XR Chest%'

-- Imageing temp table
DROP TABLE IF EXISTS #img
SELECT  img.OrderingEncounterKey
        ,img.ImagingOrderEpicId
        ,pat.PrimaryMrn
        ,ef.EncounterEpicCsn
		,img.OrderingInstant
		,img.ProtocolledInstant
		,img.ScheduledExamInstant
		,img.ExamStartInstant
		,img.ExamEndInstant
		,img.PreliminaryInstant
		,img.FinalizingInstant
	    ,imgt.Narrative
		,pd.Name
		,pd.Category
		,DATEDIFF(MINUTE,img.OrderingInstant,img.ProtocolledInstant) AS 'meas_order_to_protcol'
		,DATEDIFF(MINUTE,img.ProtocolledInstant,img.ScheduledExamInstant) AS 'meas_protocol_to_scheduled'
		,DATEDIFF(MINUTE,img.ScheduledExamInstant,img.ExamStartInstant) AS 'meas_scheduled_to_exam_start'
		,DATEDIFF(MINUTE,img.ExamStartInstant,img.ExamEndInstant) AS 'meas_exam_start_to_exam_end'
		,CASE 
		WHEN img.PreliminaryInstant IS NOT NULL THEN DATEDIFF(MINUTE,img.ExamEndInstant,img.PreliminaryInstant)
		ELSE DATEDIFF(MINUTE,img.ExamEndInstant,img.FinalizingInstant)
		END AS 'meas_exam_end_to_result' 
		,CASE 
		 WHEN img.PreliminaryInstant IS NOT NULL THEN DATEDIFF(MINUTE,img.PreliminaryInstant,img.FinalizingInstant)
		 ELSE NULL
		 END AS  'meas_result_to_finalized'
		,CASE 
		WHEN img.PreliminaryInstant IS NOT NULL THEN DATEDIFF(MINUTE,img.OrderingInstant,img.PreliminaryInstant)
		ELSE DATEDIFF(MINUTE,img.OrderingInstant,img.FinalizingInstant)
		END AS 'meas_order_to_result' 
		,CASE 
		 WHEN CAST(img.ExamEndInstant AS DATE) >= CAST('2021-11-07' AS DATE) AND  CAST(img.ExamEndInstant AS TIME) >= CAST('08:00' AS TIME) AND CAST(img.ExamEndInstant AS TIME) <= CAST('17:00' AS TIME) THEN 'UCLH'  
		 WHEN CAST(img.ExamEndInstant AS DATE) < CAST('2021-11-07' AS DATE) AND  CAST(img.ExamEndInstant AS TIME) >= CAST('08:00' AS TIME) AND CAST(img.ExamEndInstant AS TIME) <= CAST('20:00' AS TIME) THEN 'UCLH'   
		 ELSE 'ELR'
		 END AS 'Reporting services'
INTO #img
FROM ImagingTextFact AS imgt
LEFT JOIN ImagingFact AS img ON imgt.ImagingKey = img.ImagingKey
LEFT JOIN dbo.ProcedureDim AS pd ON img.FirstProcedureKey = pd.ProcedureKey
LEFT JOIN EncounterFact AS ef ON img.OrderingEncounterKey = ef.EncounterKey
LEFT JOIN (SELECT *
           FROM PatientDim )AS pat ON ef.PatientKey = pat.PatientKey 

WHERE img.CancelingInstant IS NULL
 -- To filter for the scan type found using the table above then use the code above 
--    AND  img.FirstProcedureKey IN (SELECT ProcedureKey FROM #proc )

SELECT *
FROM #img