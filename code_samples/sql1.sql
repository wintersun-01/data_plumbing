USE [UDAReporting]
GO
/****** Object:  StoredProcedure [mkt].[CLC_ApplicationSummarySP]    Script Date: 11/11/2023 10:59:09 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ==========================================================================================
-- Author:		Allie Rhodes
-- Create date: 17/08/2024
-- Description:	Daily update from CloudCase into mkt.CLC_AppSummary. Data modified in last 24 hours from CloudCase via CloudCase api.
-- ==========================================================================================
ALTER PROCEDURE [mkt].[CLC_ApplicationSummarySP]
AS
BEGIN

	/*What this script does. 
	1. Connects to Cloudcase via api to find new or modified loan applications.
	2. Copies an individual file, new or modified in the last 24 hours, to the working directory.
	3. Loops through the copied file, extracting new or changed xml.
	4. Data is placed in the current application table. (Further processing follows)	
	5. Deletes the local copy of the file.
	6. The loop moves to the next file.*/

	 /*Declarations*/
	Declare @path varchar (500)	=  '\\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\';
	Declare @nameOfFile as varchar (200);  
	Declare @SourceFile AS VARCHAR(500);  
	Declare @cmd as varchar(1000); 
	Declare @i as int ; 
	Declare @a as int ;
	Declare @cmd2 as varchar(1000);
	Declare @Destination  varchar(1000);  
	Declare @DestinationFile  VARCHAR(500);

	--Select * from ##filesToUse

	Select @a =  count (*) from ##filesToUse 
	Set @i=0 ;

    /*Loop through each file*/
  	While @a > @i   
		 Begin
			Set @i 					= @i+1 
			Set @path 				= '\\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\'
			Set @Destination 		= '\\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\WorkingFiles\'
			Select @nameOfFile 		= NameOfFile from  ##filesToUse where num = @i
			Set	@SourceFile  		= @path + @nameOfFile;
			Set	@DestinationFile	= @Destination + '1.xml'
			Set	@cmd 				= 'copy '+ @SourceFile +' ' + @Destination + '1.xml'
			Exec master.dbo.xp_cmdshell @cmd; 

	/*Extract xml convert to tabular*/
	SET IDENTITY_INSERT mkt.CLC_AppSummary OFF
					
	--Cloudcase API
	;WITH XMLNAMESPACES(DEFAULT'http://cloudcase.net/api/OrchestrationService-v15-0')
		
	Insert into mkt.CLC_AppSummary
   	SELECT
		t0.value('(id/text())[1]', 'varchar(200)') 												as CaseID,
		DATEADD(hour, 11,t0.value('(receivedDate/text())[1]',  'varchar(200)')) 				as ReceivedDate,
		t0.value('(submissionStatus/text())[1]',  'varchar(200)') 								as submissionStatus,
		t0.value('(stageText/text())[1]', 'varchar(200)') 										as stageText,
		t0.value('(status/text())[1]', 'varchar(200)') 											as [status],
		t1.value('(answer/text())[1]', 'varchar(100)') 											as OfficeUseNumberOfLoans,
		t2.value('(answer/text())[1]', 'varchar(200)') 											as NumOfBorrowers,
		t0.value('(firstName/text())[1]', 'varchar(200)') 										as B1FirstName,
		t0.value('(lastName/text())[1]', 'varchar(200)') 										as B1LastName,
		t3.value('(answer/text())[1]', 'varchar(8)') 											as allBorrowersAreMembers,
		t10.value('(answer/text())[1]', 'varchar(8)') 											as B1MemberNumber,
		t4.value('(answer/text())[1]', 'varchar(200)') 											as memberNumberPending,
		DATEADD(hour, 11,t0.value('(lastModifiedDate/text())[1]', 'varchar(200)')) 				as lastModifiedDate,
		DATEADD(hour, 11,t0.value('(assignmentDate/text())[1]', 'varchar(200)')) 				as assignmentDate,
		t0.value('(assignedUsername/text())[1]', 'varchar(200)') 								as assignedUserName,
		t0.value('(assignment/text())[1]', 'varchar(200)') 										as assignment,
		t0.value('(priority/text())[1]', 'varchar(200)') 										as priority,
		t5.value('(answer/text())[1]',  'varchar(200)') 										as appTotalAmt,
		t8.value('(answer/text())[1]',  'varchar(200)') 										as BrokerID,
		t0.value('(rulebookVersion/text())[1]', 'varchar(200)') 								as rulebookVersion,
		DATEADD(hour, 11,t0.value('(lastIdleResetActivityDate/text())[1]', 'varchar(200)')) 	as LastIdleReset,
		t6.value('(answer/text())[1]', 'varchar(200)') 											as initOperator,
		t7.value('(answer/text())[1]', 'varchar(200)') 											as initOpEmail,
		t9.value('(answer/text())[1]', 'varchar(200)') 											as Origination,
		GetDate() 																				as DataInsertDate,
		t.MY_XML 																				as RawData
					
	FROM (SELECT CAST(MY_XML AS xml)
	FROM 
		OPENROWSET(BULK '\\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\WorkingFiles\1.xml', SINGLE_BLOB) AS T(MY_XML) 
		)AS T (MY_XML)
		--OPENROWSET(BULK '\\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\KEE2I20IHRYS5D8D.xml', SINGLE_BLOB) AS T(MY_XML) 
	
	Outer Apply MY_XML.nodes('//selectedApplication/summary') 																	as MY_XML(t0)
	Outer apply MY_XML.nodes('//selectedApplication/summary/questionSummaries[id/text()	="OfficeUseNumberOfLoans"]') 			as NumofLoans (t1)
	Outer apply MY_XML.nodes('//selectedApplication/summary/questionSummaries[id/text()	="NumberOfBorrowers"]') 				as NumofBorrowers (t2)
	Outer apply MY_XML.nodes('//selectedApplication/summary/questionSummaries[id/text()	="iAllBorrowersAreMembers"]') 			as AllBAreMembers(t3)
	Outer apply MY_XML.nodes('//selectedApplication/summary/questionSummaries[id/text() ="iMemberNumberPending"]') 				as MemberNumPending(t4)
	Outer apply MY_XML.nodes('//selectedApplication/summary/questionSummaries[id/text() ="OfficeUseApplicationTotalAmount"]') 	as AppTotalAmt (t5) 
	Outer apply MY_XML.nodes('//selectedApplication/summary/questionSummaries[id/text() ="OfficeUseInitialOperatorUsername"]') 	as initOperator (t6) 
	Outer apply MY_XML.nodes('//selectedApplication/summary/questionSummaries[id/text() ="OfficeUseInitialOperatorEmail"]') 	as initOperatorEmail (t7) 	
	Outer apply MY_XML.nodes('//selectedApplication/summary/questionSummaries[id/text() ="OfficeUseBrokerID"]') 				as BrokerID (t8)
	Outer apply MY_XML.nodes('//selectedApplication/summary/questionSummaries[id/text() ="OfficeUserOriginator"]') 				as OffUseOrigination (t9)
	Outer apply MY_XML.nodes('//selectedApplication/summary/questionSummaries[id/text() = "OfficeUseBorrower1MemberNumber"]') 	as OffUseB1MemberNumber (t10)

	/*Remove current file, ready for next file to process */
	Set @cmd2 = 'del '+ @DestinationFile
	
	/*Clean up*/
	Exec master.dbo.xp_cmdshell @cmd2; 
       
	End



END
