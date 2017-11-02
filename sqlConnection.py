# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 19:13:18 2017

@author: mohini
"""
import pandas as pd
import csv
import pyodbc
import os
import datetime
import sys

def main(arg1):
	folderName = 'C:\\TaskFolder\\ProductivityReportBMS\\dist'
	fileName = folderName + '\\config' + '\\ResourceConfig.txt'

	Config = {}
	with open(fileName) as f:
		for line in f:
		   (key, val) = line.split()
		   Config[key] = val
	
	LiveConnect = 'DRIVER={SQL Server};' + Config['LiveConnection']
	
	cnxn = pyodbc.connect(LiveConnect)
	crsr = cnxn.cursor()
    
    
	#sql = "EXEC [BMS].[GetAgentProductivityReport]@productId = 117, @FromDate='2017-06-14', @ToDate='2017-06-15'"
	#sql = "Select top 10 * from crm.Leaddetails where createdOn>getdate()-1"
	#sql2 = 'EXEC [MTX].[Get_LeadHistory] @leadId=25618724'
	#sql = 'EXEC [dbo].[GetGroupType]'
	productid=arg1
	sql = "DECLARE @ProductID TINYINT = " + str(productid) + ","
	sql +="""\ 	  @FromDate DATE = null,
            @ToDate DATE = null
    
    BEGIN
    	SET NOCOUNT ON;
    	BEGIN 
    		DECLARE @Product VARCHAR(50)
    		SELECT @Product = P.ProductName FROM dbo.Products P WHERE P.ID = @ProductID
    		
    		IF(@FromDate IS NULL)
    		BEGIN
    			SET @FromDate = CAST(GETDATE()-1 AS DATE)
    		END
    		IF(@ToDate IS NULL)
    		BEGIN
    			SET @ToDate = CAST(GETDATE() AS DATE)
    		END
    		IF(OBJECT_ID('tempdb..#CustData') IS NOT NULL)
    		BEGIN
    			DROP TABLE #CustData
    		END
    		CREATE TABLE #CustData (RecId smallint Identity(1,1) Primary Key,
    								ProductivityDate DATE, GroupName VARCHAR(200), AssignedTo VARCHAR(255), CustIdentityID BIGINT, ProductName VARCHAR(50),
    								PreviousCallback DATETIME, PreviousCallbackType VARCHAR(100),Callback DATETIME, CallbackType VARCHAR(100), 
    								PulledOn DATETIME, PulledOff DATETIME, PulledBy SMALLINT, PulledOffActual BIT)
    								
    		IF(OBJECT_ID('tempdb..#CallData') IS NOT NULL)
    		BEGIN
    			DROP TABLE #CallData
    		END
    		IF(OBJECT_ID('tempdb..#StatusChangeData') IS NOT NULL)
    		BEGIN
    			DROP TABLE #StatusChangeData
    		END
    		IF(OBJECT_ID('tempdb..#EmailSMSData') IS NOT NULL)
    		BEGIN
    			DROP TABLE #EmailSMSData
    		END
    		IF(OBJECT_ID('tempdb..#CallbackMaster') IS NOT NULL)
    		BEGIN
    			DROP TABLE #CallbackMaster
    		END
    		CREATE TABLE #CallData				(LeadID BIGINT, CustomerIdentityId BIGINT, UserID SMALLINT, CallDate DATETIME, TalkTime SMALLINT, Disposition varchar(200), PulledOn DATETIME, PulledOff DATETIME)
    		CREATE TABLE #EmailSMSData			(LeadID BIGINT,PulledOn DATETIME,PulledOff DATETIME)
    		CREATE TABLE #CallbackMaster		(CallBackType VARCHAR(20), CallBackTypeID TINYINT)
    		CREATE TABLE #StatusChangeData		(LeadID BIGINT, CustomerIdentityId BIGINT, UserID SMALLINT, PulledOn DATETIME, PulledOff DATETIME, Status VARCHAR(255))		
    	END
    
    	BEGIN
    		INSERT INTO #CallbackMaster(CallBackType, CallBackTypeID)
    		select ctm.CallBackType, ctm.CallBackTypeID from BMS.CallBackTypeMaster ctm 
    		
                  
    		BEGIN
    			INSERT INTO #CustData(ProductivityDate, GroupName, AssignedTo, CustIdentityID, ProductName,
    								 PreviousCallback, PreviousCallbackType, Callback, CallbackType, 
    								 PulledOn, PulledOff, PulledBy, PulledOffActual)
    			SELECT	CAST(CPD.PulledOn AS DATE), GM.UserGroupName, UD.UserName + '(' + UD.EmployeeId + ')' AS AssignedTo, CPD.CustIdentityID, @Product,
    				CPD.PreviousCallBackOn, (SELECT CallBackType FROM #CallbackMaster WHERE CallBackTypeID = CPD.PreviousCallBackTypeID) AS PreviousCallBackType,
    				CPD.CallBackOn, (SELECT CallBackType FROM #CallbackMaster WHERE CallBackTypeID = CPD.CallBackTypeID) AS CallBackType,
    				CPD.PulledOn, CPD.PulledOff, CPD.SearchBy, CASE WHEN PulledOff IS NOT NULL THEN 1 ELSE 0 END AS PulledoffActual
    				
    				FROM	BMS.CustomerPulledDetails	AS CPD	WITH(NOLOCK) 
    				INNER JOIN (SELECT DISTINCT CustIdentityID FROM BMS.CustomerIdentityDetails WHERE ParentProductId = @ProductID AND CreatedON>GETDATE()-90) AS CID ON CID.CustIdentityID = CPD.CustIdentityID
    				LEFT JOIN BMS.CustomerAssignmentDetails   AD	WITH(NOLOCK) ON CPD.CustIdentityID=AD.CustIdentityID AND AD.IsActive = 1
    				LEFT JOIN CRM.UserDetails				AS UD	WITH(NOLOCK) ON UD.UserID = AD.AssignedToUserID 
    				LEFT JOIN  CRM.UserGroupMaster			AS GM	WITH(NOLOCK) ON GM.UserGroupID=CPD.GroupID AND GM.IsBMSGroup=1 AND GM.UserGroupID = AD.AssignToGroupId
    				WHERE CPD.PulledOn > @FromDate AND CPD.PulledOn <= @ToDate AND SearchBy IS NOT NULL 
                       ORDER BY CPD.SearchBy, CPD.PulledOn
    
    
    			DECLARE @TotalLeadCount smallint = 0;
    			
    			Select @TotalLeadCount = count(1) from #CustData
    			DECLARE @Counter smallint = @TotalLeadCount;
    			
    			DECLARE @FromDateTime DATETIME
    			DECLARE @ToDateTime DATETIME
    			DECLARE @AgentId SMALLINT
    			
    			WHILE @Counter>0
    			BEGIN
    				SELECT @FromDateTime = PulledOn, @ToDateTime = PulledOff, @AgentId = PulledBy from #CustData where RecId = @Counter
    				
    				IF(@ToDateTime IS NULL AND @Counter!=@TotalLeadCount)
    				BEGIN
    					SELECT @ToDateTime = PulledOn from #CustData where RecId = @Counter+1 and PulledBy = @AgentId
    				END
    				IF(@ToDateTime IS NULL)
    				BEGIN
    					SELECT @ToDateTime = DATEADD(MINUTE, 5, @FromDateTime)
    				END
    				
    				UPDATE #CustData SET PulledOff = @ToDateTime where RecId = @Counter
    				
    				SET @Counter = @Counter-1
    			END
    		
    		END
    		
      
    		BEGIN
    
    			
    			INSERT INTO #CallData (CallDate, Disposition, LeadID, CustomerIdentityId, TalkTime, UserID, PulledOn, PulledOff)
    			SELECT CDH.CallDate, CDH.Disposition, CDH.LeadID, CUST.CustIdentityID, CDH.talktime, CDH.UserID, CUST.PulledOn, CUST.PulledOff
    			FROM		#CustData	AS CUST
    			INNER JOIN BMS.CustomerIdentityDetails	AS CID WITH(NOLOCK) ON CID.CustIdentityID = CUST.CustIdentityID
    					AND CID.ParentProductId = @ProductID AND CID.CreatedON>GETDATE()-180
    			INNER JOIN MTX.CallDataHistory			AS CDH WITH(NOLOCK) ON CID.LeadID = CDH.LeadID 
    					AND CDH.CallDate BETWEEN CUST.PulledOn AND DATEADD(MINUTE,2,CUST.PulledOff) AND CDH.IsBMS = 1 AND CDH.UserID = CUST.PulledBy
    					AND CDH.talktime>0 AND Disposition IS NOT NULL
    		END 
            BEGIN
                
    			INSERT INTO #EmailSMSData (LeadID, PulledOn, PulledOff)
    			SELECT DISTINCT CML.LeadID, CUST.PulledOn, PulledOff
    				FROM MTX.CommunicationLog				AS CML	WITH(NOLOCK)
    				INNER JOIN BMS.CustomerIdentityDetails	AS CID	WITH(NOLOCK) ON CML.LeadID = CID.LeadID AND CML.IsBooking = 1 
    				INNER JOIN #CustData AS CUST				 ON CID.CustIdentityID = CUST.CustIdentityID
    					AND CML.CreatedOn BETWEEN CUST.PulledOn AND DATEADD(MINUTE,2,CUST.PulledOff) AND CID.CreatedON>GETDATE()-180
    		END	
    		
    		BEGIN
    			INSERT INTO #StatusChangeData(CustomerIdentityId, LeadID, PulledOn, PulledOff, UserID, Status)
    			SELECT CUST.CustIdentityID, LS.LeadID, CUST.PulledOn, CUST.PulledOff, CUST.PulledBy
    			, SM.StatusName + CASE WHEN SSM.SubStatusID>0 THEN '(' + SSM.SubStatusName + ')' ELSE '' END AS STATUS
    			FROM		#CustData	AS CUST
    			INNER JOIN BMS.CustomerIdentityDetails	AS CID	WITH(NOLOCK) ON CID.CustIdentityID = CUST.CustIdentityID
    					AND CID.ParentProductId = @ProductID AND CID.CreatedON>GETDATE()-180
    			INNER JOIN CRM.LeadStatus				AS LS	WITH(NOLOCK) ON LS.LeadID = CID.LeadID AND LS.StatusID >= 13
    					AND LS.CreatedOn BETWEEN CUST.PulledOn AND CUST.PulledOff
    			INNER JOIN CRM.StatusMaster				AS SM	WITH(NOLOCK) ON SM.StatusId = LS.StatusID 
    			LEFT JOIN  CRM.SubstatusMaster			AS SSM	WITH(NOLOCK) ON SSM.SubStatusID = LS.SubStatusID
    		END
    	END
     
    
    	SELECT * FROM 
    	(SELECT	CUST.ProductivityDate, CUST.ProductName, CUST.GroupName, CUST.AssignedTo, UD.UserName + '(' + UD.EmployeeId + ')' AS PulledBy, CUST.CustIdentityID
    			, ID.CustomerID, ID.LeadID, SM.StatusName + CASE WHEN SSM.SubStatusID>0 THEN '(' + SSM.SubStatusName + ')' ELSE '' END AS STATUS
    			, CAST(CUST.PreviousCallback AS DATETIME) AS PreviousCallback, CUST.PreviousCallbackType, CAST(CUST.Callback AS DATETIME) AS Callback, CUST.CallbackType
                 , CAST(CUST.PulledOn AS DATETIME) AS PulledOn, CAST(CUST.PulledOff AS DATETIME) AS PulledOff, CAST(DATEDIFF(SECOND,CUST.pulledon,CUST.pulledoff) AS VARCHAR(10)) AS TimeSpent, CUST.PulledOffActual
    			, ISNULL(CommDet.AutoSMS,0) AS AutoSMS, ISNULL(CommDet.ManualSMS,0) AS ManualSMS, ISNULL(CommDet.AutoEmail,0) AS AutoEmail
    			, ISNULL(CommDet.ManualEmail,0) AS ManualEmail, CommDet.talktime, CommDet.Disposition, CommDet.Status AS StatusChange
    			
    	FROM		#CustData					AS CUST 			
    	
    	INNER JOIN	CRM.UserDetails				AS UD	WITH(NOLOCK) ON UD.UserID = CUST.PulledBy
    
    	INNER JOIN	BMS.CustomerIdentityDetails	AS ID	WITH(NOLOCK) ON ID.CustIdentityID = CUST.CustIdentityID 
    				AND ID.ParentProductId = @ProductID AND ID.CreatedON > GETDATE()-180
    				
    	INNER JOIN	CRM.BookingDetails			AS BD	WITH(NOLOCK) ON BD.LEADID = ID.LEADID
    				AND BD.IsBooked = 1 AND BD.PaymentSTATUS IN (300,3002,4002,5002,6002) AND BD.CreatedOn>GETDATE()-180
    	
    
    	INNER JOIN	CRM.LeadStatus				AS LS	WITH(NOLOCK) ON LS.LeadID = BD.LeadID AND LS.StatusID >=13
    				AND LS.IsLastStatus = 1
    	INNER JOIN	CRM.StatusMaster			AS SM	WITH(NOLOCK) ON SM.StatusID = LS.StatusID
    	LEFT JOIN	CRM.SubstatusMaster			AS SSM	WITH(NOLOCK) ON SSM.SubStatusID = LS.SubStatusID AND SSM.IsActive = 1 AND SSM.StatusId = SM.StatusId
    	
    
    	INNER JOIN	
    	(SELECT IDS.LeadID, IDS.PulledOff, IDS.PulledOn, CAL.UserID, CAL.TalkTime, CAL.Disposition, ESD.AutoEmail, ESD.AutoSMS, ESD.ManualEmail, ESD.ManualSMS
    			, CASE WHEN SCD.Status IS NOT NULL THEN 'Status changed to ' + SCD.Status ELSE NULL END AS Status
    	FROM(SELECT DISTINCT LeadID, PulledOn, PulledOff FROM #CallData
    		UNION
    		 SELECT DISTINCT LeadID, PulledOn, PulledOff  FROM #EmailSMSData
    		UNION
    		 SELECT DISTINCT LeadID, PulledOn, PulledOff FROM #StatusChangeData) AS IDS
    	LEFT JOIN #CallData			AS CAL ON CAL.LeadID = IDS.LeadID AND CAL.PulledOn = IDS.PulledOn AND CAL.PulledOff = IDS.PulledOff
    	LEFT JOIN (SELECT LeadID, AutoSMS, ManualSMS, AutoEmail, ManualEmail, PulledOn, PulledOff FROM(
    				SELECT CML.LeadID, CUST.PulledOn, PulledOff, 
    				COUNT(CASE WHEN CommType = 'SMS' and  IsAuto = 1 THEN CML.LeadID ELSE NULL END) AS AutoSMS,
    				COUNT(CASE WHEN CommType = 'SMS' and  IsAuto = 0 THEN CML.LeadID ELSE NULL END) AS ManualSMS,
    				COUNT(CASE WHEN CommType = 'EMAIL' and  IsAuto = 1 THEN CML.LeadID ELSE NULL END) AS AutoEmail,
    				COUNT(CASE WHEN CommType = 'EMAIL' and  IsAuto = 0 THEN CML.LeadID ELSE NULL END) AS ManualEmail
    				FROM MTX.CommunicationLog				AS CML	WITH(NOLOCK)
    				INNER JOIN BMS.CustomerIdentityDetails	AS CID	WITH(NOLOCK) ON CML.LeadID = CID.LeadID AND CML.IsBooking = 1 AND CID.ParentProductId = @ProductID
    				INNER JOIN #CustData AS CUST				 ON CID.CustIdentityID = CUST.CustIdentityID and CUST.PulledoffActual=1
    					AND CML.CreatedOn BETWEEN CUST.PulledOn AND DATEADD(MINUTE,2,CUST.PulledOff) AND CID.CreatedON>GETDATE()-180
    				GROUP BY CML.LeadID, CUST.PulledOn, CUST.PulledOff) AS T
    				WHERE (T.AutoEmail+T.AutoSMS+T.ManualEmail+T.ManualSMS)>0)		AS ESD ON ESD.LeadID = IDS.LeadID AND ESD.PulledOn = IDS.PulledOn AND ESD.PulledOff = IDS.PulledOff 
    	LEFT JOIN #StatusChangeData	AS SCD ON SCD.LeadID = IDS.LeadID AND SCD.PulledOn = IDS.PulledOn AND SCD.PulledOff = IDS.PulledOff 
    	) AS CommDet ON CommDet.LeadID = ID.LeadID AND CommDet.PulledOn = CUST.PulledOn AND CommDet.PulledOff = CUST.PulledOff
    	
    	UNION
    	
    	SELECT	CUST.ProductivityDate, CUST.ProductName, CUST.GroupName, CUST.AssignedTo, UD.UserName + '(' + UD.EmployeeId + ')' AS PulledBy, CUST.CustIdentityID
    			, ID.CustomerID, 0, NULL AS STATUS
    			, CUST.PreviousCallback, CUST.PreviousCallbackType, CUST.Callback, CUST.CallbackType, CUST.PulledOn
    			, CUST.PulledOff, CAST(DATEDIFF(SECOND,CUST.pulledon,CUST.pulledoff) AS VARCHAR(10)) AS TimeSpent, CUST.PulledOffActual
    			, 0 AS AutoSMS, 0 AS ManualSMS, 0 AS AutoEmail,0 AS ManualEmail, 0 AS Talktime, NULL AS Disposition, NULL AS StatusChange
    	FROM #CustData AS CUST
    	INNER JOIN	CRM.UserDetails				AS UD	WITH(NOLOCK) ON UD.UserID = CUST.PulledBy
    
    	INNER JOIN	BMS.CustomerIdentityDetails	AS ID	WITH(NOLOCK) ON ID.CustIdentityID = CUST.CustIdentityID 
    				AND ID.ParentProductId = @ProductID AND ID.CreatedON > GETDATE()-180
    	WHERE CUST.PreviousCallback IS NOT NULL AND CUST.Callback IS NOT NULL ) AS FINAL
    	ORDER BY PulledBy, PulledOn
    	
    END
    """
	sql = sql.replace("\\", "")
	rows = crsr.execute(sql)
	#rows2 = crsr.execute(sql2)
	#print(rows2)
	print(productid)
	if (productid == 2):
		product = '\\health'
	elif(productid == 7):
		product = '\\term'
	elif(productid == 115):
		product = '\\invest'
	elif(productid == 117):
		product = '\\motor'
	folderName = 'C:\\TaskFolder\\ProductivityReportBMS\\dist'
	folderName += '\\' + datetime.date.today().strftime("%B %d, %Y") 
	if not os.path.exists(folderName):
		os.makedirs(folderName)
	folderName += product
	with open(folderName + 'Data.csv', 'w',newline='') as csvfile:
		writer = csv.writer(csvfile)
		writer.writerow([x[0] for x in crsr.description])  # column headers
		for row in rows:
			#print(row)
			writer.writerow(row)
            
    ##########################################################################################################################
	df=pd.read_csv(folderName + 'Data.csv',encoding = "ISO-8859-1")
	df.PreviousCallback=pd.to_datetime(df.PreviousCallback)
	df.Callback=pd.to_datetime(df.Callback)
	df.PulledOn=pd.to_datetime(df.PulledOn)
	df.PulledOff=pd.to_datetime(df.PulledOff)
	
	df1=df.copy()
	for index1, x in df[(df1.LeadID==0)].iterrows():
		count = df1[(df1.CustIdentityID == x.CustIdentityID ) & (df1.PulledBy == x.PulledBy) & (df1.PulledOn == x.PulledOn) & (df1.LeadID > 0)].LeadID.count()
		#print(count)
		if (count>0):
			#print(df1[(df1.CustIdentityID == x.CustIdentityID ) & (df1.PulledBy == x.PulledBy) & (df1.PulledOn == x.PulledOn) & (df1.LeadID == 0)])
			df1.drop(df1.index[(df1.CustIdentityID == x.CustIdentityID ) & (df1.PulledBy == x.PulledBy) & (df1.PulledOn == x.PulledOn) & (df1.LeadID == 0)], inplace=True)
    
	df1.drop_duplicates(subset=['PulledBy','CustIdentityID','LeadID','PulledOn','talktime'],keep='last', inplace=True)
	
	def f1(s):
		#print(type(s), s)
		return max(s, key=len)
	def f(x):
		return pd.Series(dict(talktime = x['talktime'].sum(), Disposition = x['Disposition'].f1()))
                            
	df2 = df1.groupby(['ProductivityDate', 'ProductName', 'GroupName', 'PulledBy', 'AssignedTo',
           'CustIdentityID', 'CustomerID', 'LeadID',
           'PreviousCallback', 'PreviousCallbackType', 'Callback',
           'CallbackType', 'PulledOn', 'PulledOff', 'TimeSpent',
           'PulledOffActual', 'AutoSMS', 'ManualSMS', 'AutoEmail',
           'ManualEmail']).agg({'talktime':'sum','Disposition':'max','StatusChange':'last','STATUS':'last'}).reset_index()
    
	df2.drop_duplicates(subset=['PulledBy','CustIdentityID','LeadID','PulledOn'],keep='last', inplace=True)
           
	df2.to_csv(folderName + 'DataFinal.csv')


if __name__=='__main__':    
	sys.exit(main(2))