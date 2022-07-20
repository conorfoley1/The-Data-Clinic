----------------------------------------
--Name: Lab results
--Description: General script for pulling lab results 
--Data Source: CABOODLE_REPORT
---------------------------------------

-- There are two differant ways you may wish to search for labs; via a broad catogory of type e.g.virology or for a specific test e.g.hepatits B monitoring
-- In either case you can use the code below to find the name of a catogory of spacific test in epic so that it can be used to filter the subsequent script 
-- If there is a large list of labs you wished to search for you may whish to save the results of your search, this can be done by includeing the DROP TABLE.. and INTO.. sections of the code below
--DROP TABLE IF EXISTS #test
SELECT DISTINCT  ProcedureKey
                ,Category
                ,NAME
--INTO #test
FROM ProcedureDim
WHERE ProcedureKey IN (SELECT ProcedureKey FROM LabComponentDim)
-- you can use the the code below to help find a specific Category by typing what you are looking for between %% e.g.'%Heam%'  
--    AND Category LIKE '%%'
-- Or the below th help find a specific test by typing what you are looking for between %% e.g.'%ALLERGEN%'  
--    AND Category NAME- '%%'

--SQL code for pulling labs data and storeing it in temporary table 
DROP TABLE IF EXISTS #lab
SELECT 
		 ef.EncounterEpicCsn
		,dim.LabComponentKey
		,pd.Category 'procedure_category'
		,pd.Name AS 'procedure'		
		,dim.Name AS 'result_name'
		,lab.Value
		,lab.NumericValue
		,lab.Unit
		,lab.CollectionInstant
		,lab.ResultInstant
INTO #lab
FROM dbo.LabComponentResultFact AS lab

LEFT JOIN LabComponentDim dim ON lab.LabComponentKey = dim.LabComponentKey

LEFT JOIN LabComponentResultTextFact AS txt ON lab.LabComponentResultKey = txt.LabComponentResultKey

LEFT JOIN ProcedureDim AS pd ON lab.ProcedureKey = pd.ProcedureKey

LEFT JOIN EncounterFact AS ef ON lab.EncounterKey = ef.EncounterKey

WHERE lab.LabComponentKey NOT IN (-3,-2,-1)
      AND 
	  dim.Name <> 'HISTORIC'
-- If you use the above script to find the labs and saved it as a tempory table to filter on then you can use the below code to do so
--	  AND ProcedureKey IN (SELECT ProcedureKey FROM #test)

SELECT *
FROM #lab