USE [UDAReporting]
GO
/****** Object:  StoredProcedure [mkt].[aaAgentJobTest]    Script Date: 11/11/2023 10:39:27 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:		Allie Rhodes
-- Create date: 24/08/2023
-- Description:	Executes AgentJob_CloudCase_ETL that sets up,  executes CloudCase scripts, then cleans up.
-- =============================================
ALTER PROCEDURE [mkt].[AgentJob_CloudCase_ETL]
AS
BEGIN
/* Steps
			1. Create temp table listing files arrived from cloudcase in \\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\
			2. Delete from all tables, existing data updated in these new files.
			3. Loop through each table importing from new and updated files.
			4. Archives newly arrived files. 
			5. Purge archive older than 7 days. 
*/	
					
	
	
/*Cloud case scripts that will be executed in AgentJob_CloudCaseETL*/
			--Exec  [mkt].[CLC_ApplicationSummarySP]	 
			--Exec  [mkt].[CLC_LoanSummarySP]
			--Exec  [mkt].[CLC_LoanDetailSP]
			--Exec  [mkt].[CLC_ApplicantSP]
			--Exec  [mkt].[CLC_ActivityLogSP]	
			--Select * from mkt.CLC_AppSummary
			--Select * from mkt.CLC_LoanSummary
			--Select * from mkt.CLC_LoanDetail
			--Select * from mkt.CLC_Applicants
			--Select * from mkt.CLC_ActivityLog

/* 1. Create global list of files to move*/

		If object_ID(N'tempdb..##FilesToUse') IS NOT NULL
		Begin
			Drop table ##FilesToUse
		End


		Create table ##filesToUse
				(ID int Identity
				,NameOfFile varchar(100)
				,Depth int
				,Num int)

		Insert into  ##filesToUse
				Exec master.sys.xp_dirtree '\\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\',0,1; 
				Delete from ##filesToUse where (Depth >1 or Num = 0 or nameOfFile like 'App%')

		Update f
				Set f.Num = f.newNum 
				From (SELECT Num, ROW_NUMBER() over (order by  num desc) AS newNum
							FROM ##filesToUse ) f

/* 2.Delete values from all tables that will be updated today*/
			--Select * from ##filesToUse f
							
			Delete mkt.CLC_AppSummary 
			from mkt.CLC_AppSummary aps
			inner join 		##filesToUse f
			on Left(f.NameOfFile,CharIndex('.',f.NameOfFile)-1) COLLATE DATABASE_DEFAULT =  aps.CaseID 

			Delete mkt.CLC_LoanSummary
			from mkt.CLC_LoanSummary ls
			inner join 		##filesToUse f
			on Left(f.NameOfFile,CharIndex('.',f.NameOfFile)-1) COLLATE DATABASE_DEFAULT =  ls.CaseID 
				
			Delete  mkt.CLC_LoanDetail 
			From  mkt.CLC_LoanDetail ld
			inner join 		##filesToUse f
			on Left(f.NameOfFile,CharIndex('.',f.NameOfFile)-1) COLLATE DATABASE_DEFAULT =  ld.CaseID 

			Delete  mkt.CLC_Applicants  
			From  mkt.CLC_Applicants a
			inner join 		##filesToUse f
			on Left(f.NameOfFile,CharIndex('.',f.NameOfFile)-1) COLLATE DATABASE_DEFAULT =  a.CaseID

			Delete mkt.CLC_ActivityLog
			From mkt.CLC_ActivityLog al
			inner join 	##filesToUse f
			on Left(f.NameOfFile,CharIndex('.',f.NameOfFile)-1) COLLATE DATABASE_DEFAULT = al.CaseID COLLATE  DATABASE_DEFAULT



/* Create working folder to be used by all scripts*/ 
		Declare @cmd varchar(2000)
		Declare @folderName varchar(100)  = 'WorkingFiles'
		Declare @path varchar (500)		  = '\\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\';

		Set @cmd = 'mkdir ' + @path +   @folderName
		Exec master.dbo.xp_cmdshell @cmd; 


/*3. Agent job loops through tables updating from files*/ 
		
		Exec msdb.dbo.sp_start_job [AgentJob_CloudCase_ETL_]
		
	/*  
		Exec  [mkt].[CLC_ApplicationSummarySP]	 
		Exec  [mkt].[CLC_LoanSummarySP]
		Exec  [mkt].[CLC_LoanDetailSP]
		Exec  [mkt].[CLC_ApplicantSP]
		Exec  [mkt].[CLC_ActivityLogSP]
	*/	


/*Clean up */ 
		Set @path					= '\\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\';
		Set @folderName 			= 'WorkingFiles'
		Set @cmd 					= 'rmdir ' + @path +   @folderName  + '/S /Q'
		Exec master.dbo.xp_cmdshell @cmd; 
		
		Drop table	##filesToUse


/*Set up new archive folder for today's files*/
		Declare @cmd0 varchar(2000)
		Declare @date date					= 	SYSDATETIME( )
		Declare @folderName0 varchar(100)  	=  FORMAT(@date, 'ddMMMyyyy')
		Declare @path0 varchar (500)		=  '\\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\Archive\';
		Set @cmd0 							= 'mkdir ' + @path0 +   @folderName0
		Exec master.dbo.xp_cmdshell @cmd0; 

					 
/*Move files to archive folder*/
	  
		Set @cmd0 				= 'move /Y \\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\*.xml \\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\Archive\'
	    Exec master.dbo.xp_cmdshell @cmd0; 



/*Purge 8th day prior archive folder*/
	    Declare @date8 date	=  DateAdd(DAY,-8, GetDate( ))
		Declare @folderName8 varchar(100)  =  FORMAT(@date8, 'ddMMMyyyy')
		Set @cmd0 = 'rmdir '+ @path + @folderName8  + '/S ' + '/Q' 
		Exec master.dbo.xp_cmdshell @cmd0


Exec msdb.dbo.sp_stop_job  [AgentJob_CloudCase_ETL_] 


END 













/*
Testing only.

/*Set @cmd0 			= 'move /Y \\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\*.xml \\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\Archive\'
Exec master.dbo.xp_cmdshell @cmd0; */

/*Set @cmd 				= ' ren \\ha-prd-rdb02\RBDEXSAM\CloudCaseFiles\WorkingFiles_AppSummary\123.xml  1.xml'
Exec master.dbo.xp_cmdshell @Cmd; 
*/
/*Declare @path0 varchar (500)	
Set @path 				=  '\\ha-prd-RDB02\RBDEXSAM\CloudCaseFiles\'
Set @cmd0 				= 'move /Y' + @path  +'*.xml ' +  @path0 --+ @folderName0  */
*/
source dir: dest dir
EXEC xp_cmdshell 
'copy C:\NPE C:\backups';
*/