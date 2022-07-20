----------------------------------------
--Name: Flowsheet Data
--Description: This will provide you with the relavent flowsheet row and template information. 
--Data Source: CABOODLE_REPORT
---------------------------------------

--Searching for flowsheets 
-- The most effective way for search for flowsheets in Caboodle is using the Epic ID
-- There are two common methods for getting the Epic ID's for flowsheets, the first is use the session report info feature within epic .
-- The other is to search for the name of the flowsheet based of what it is called on Epic, the below script is designed to facilitate this.  

SELECT *
FROM FlowsheetRowDim
-- put the name of the flowsheetyou looking for in the middle of the two percentage symbol below e.g. '%Pulse%' 
WHERE DisplayName LIKE '%%'
-- The below ensures you are only showed flowsheets which have been ued atleat once.
      AND
	  FlowsheetRowEpicId IN (SELECT DISTINCT FlowsheetRowEpicId FROM FlowsheetValueFact )



-- SQL code for pulling flowsheet data and storeing it in temporary table 
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
	   ,flow.NumericValue
	   ,ROW_NUMBER() OVER (PARTITION BY ef.EncounterEpicCsn , flowdim.FlowsheetRowKey ORDER BY TakenInstant ) AS 'testnum'
INTO #flow 
FROM FlowsheetValueFact AS flow
LEFT JOIN FlowsheetRowDim AS flowdim ON flow.FlowsheetRowKey = flowdim.FlowsheetRowKey
LEFT JOIN FlowsheetTemplateDim AS flowtemp ON flowtemp.FlowsheetTemplateKey = flow.FlowsheetTemplateKey
LEFT JOIN EncounterFact AS ef ON flow.EncounterKey = ef.EncounterKey
LEFT JOIN PatientDim AS pd ON ef.PatientDurableKey = pd.DurableKey AND pd.IsCurrent = 1
-- code for filtering based off of flowsheet row Epic ID
--WHERE flowdim.FlowsheetRowEpicId IN ()

SELECT *
FROM #flow