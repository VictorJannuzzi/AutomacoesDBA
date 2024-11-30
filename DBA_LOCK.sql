-- desenvolvida pelos dba victor hugo version 1.0 - 29/08/2024
-- time está em ms, 5 segundos = 5000, 1 minuto = 60000
-- job serve para apenas registrar os resultados em uma tabela específica chamada dba_block.
-- requisito: alterar a base DBA para a base que deseja criar a procedure- > dê CTrl + F neste trecho para alterar: DBA..
create or alter  procedure sp_dba_lock (  @time int = 300000, @job bit = 0 ) 
as              
IF OBJECT_ID('TEMPDB..#CTE') IS NOT NULL DROP TABLE #CTE              
IF OBJECT_ID('TEMPDB..#CTE2') IS NOT NULL DROP TABLE #CTE2              
IF OBJECT_ID('TEMPDB..#RESULT') IS NOT NULL DROP TABLE #RESULT              
IF OBJECT_ID('TEMPDB..#INSERT') IS NOT NULL DROP TABLE #INSERT              
              
SELECT GETDATE() AS DATA_EVENTO, ES.SESSION_ID,               
  CASE WHEN ES.STATUS != 'SLEEPING'               
  THEN               
   DB_NAME(ER.DATABASE_ID)              
  ELSE               
   DB_NAME(ES.DATABASE_ID)               
  END AS [DATABASE],              
  ER.BLOCKING_SESSION_ID,              
  ES.STATUS,              
  ES.OPEN_TRANSACTION_COUNT,              
  CASE WHEN ES.TRANSACTION_ISOLATION_LEVEL = 0              
   THEN    'UNSPECIFIED'              
  WHEN ES.TRANSACTION_ISOLATION_LEVEL = 1               
   THEN    'READUNCOMMITTED'              
  WHEN ES.TRANSACTION_ISOLATION_LEVEL = 2               
   THEN    'READCOMMITTED'              
  WHEN ES.TRANSACTION_ISOLATION_LEVEL = 3               
   THEN    'REPEATABLEREAD'              
  WHEN ES.TRANSACTION_ISOLATION_LEVEL = 4               
   THEN    'SERIALIZABLE'              
  ELSE               
   'SNAPSHOT'               
  END AS ISOLATION_LEVEL,              
  ER.WAIT_TYPE,              
  ER.WAIT_RESOURCE,              
  ER.WAIT_TIME,              
  EST.TEXT AS [QUERY_ATUAL],              
  CASE WHEN OBJECTID IS NOT NULL               
   THEN               
    OBJECT_NAME(OBJECTID)              
   ELSE               
    'AD-HOC'              
  END AS [OBJETO],              
  ES.HOST_NAME,              
  ES.PROGRAM_NAME,              
  ES.LOGIN_NAME,              
  ER.QUERY_HASH,              
  EIB.EVENT_INFO AS [QUERY_RAIZ]              
  INTO #CTE              
  FROM SYS.DM_EXEC_SESSIONS ES               
  LEFT JOIN SYS.DM_EXEC_REQUESTS ER ON ER.SESSION_ID = ES.SESSION_ID              
  OUTER APPLY SYS.DM_EXEC_SQL_TEXT(ER.SQL_HANDLE) AS EST               
  OUTER APPLY SYS.DM_EXEC_INPUT_BUFFER (ES.SESSION_ID, NULL) AS EIB              
  WHERE 1=1              
  AND EXISTS (SELECT 1 FROM SYS.DM_EXEC_REQUESTS R2 WHERE R2.BLOCKING_SESSION_ID = ES.SESSION_ID)              
  AND ISNULL(ER.BLOCKING_SESSION_ID, 0) IN (0, -2,-3,-4,-5)              
              
SELECT GETDATE() AS DATA_EVENTO, ES.SESSION_ID,               
    CASE WHEN ES.STATUS != 'SLEEPING'               
    THEN               
        DB_NAME(ER.DATABASE_ID)              
    ELSE               
        DB_NAME(ES.DATABASE_ID)               
    END AS [DATABASE],              
    ER.BLOCKING_SESSION_ID,              
    ES.STATUS,              
    ES.OPEN_TRANSACTION_COUNT,              
    CASE WHEN ES.TRANSACTION_ISOLATION_LEVEL = 0              
        THEN    'UNSPECIFIED'              
    WHEN ES.TRANSACTION_ISOLATION_LEVEL = 1               
        THEN    'READUNCOMMITTED'              
    WHEN ES.TRANSACTION_ISOLATION_LEVEL = 2               
        THEN    'READCOMMITTED'              
    WHEN ES.TRANSACTION_ISOLATION_LEVEL = 3               
        THEN    'REPEATABLEREAD'              
    WHEN ES.TRANSACTION_ISOLATION_LEVEL = 4               
        THEN    'SERIALIZABLE'              
    ELSE               
  'SNAPSHOT'               
    END AS ISOLATION_LEVEL,              
 ER.WAIT_TYPE,              
    ER.WAIT_RESOURCE,              
 ER.WAIT_TIME,              
    EST.TEXT AS [QUERY_ATUAL],              
    CASE WHEN OBJECTID IS NOT NULL               
        THEN               
            OBJECT_NAME(OBJECTID)              
        ELSE               
            'AD-HOC'              
    END AS [OBJETO],              
    ES.HOST_NAME,              
    ES.PROGRAM_NAME,              
    ES.LOGIN_NAME,              
    ER.QUERY_HASH,              
    EIB.EVENT_INFO AS [QUERY_RAIZ]              
 INTO #CTE2              
    FROM SYS.DM_EXEC_SESSIONS ES               
    LEFT JOIN SYS.DM_EXEC_REQUESTS ER ON ER.SESSION_ID = ES.SESSION_ID              
    OUTER APPLY SYS.DM_EXEC_SQL_TEXT(ER.SQL_HANDLE) AS EST               
    OUTER APPLY SYS.DM_EXEC_INPUT_BUFFER (ES.SESSION_ID, NULL) AS EIB              
 WHERE               
 1=1              
 AND ISNULL(ER.BLOCKING_SESSION_ID, 0) != 0              
               
                
 -- TRATAMENTO DE DADOS PARA A SAÍDA FINAL              
 SELECT RANK() OVER (PARTITION BY ISNULL(B.SESSION_ID,X.BLOCKING_SESSION_ID) ORDER BY ORDEM DESC) RANK, X.ORDEM,ISNULL(B.SESSION_ID,0) SESSION_RAIZ,X.BLOCKING_SESSION_ID ,X.SESSION_ID               
 INTO #RESULT FROM (SELECT DENSE_RANK() OVER (ORDER BY WAIT_TIME DESC) AS ORDEM, * FROM #CTE2) AS X              
 LEFT JOIN #CTE B ON B.SESSION_ID = X.BLOCKING_SESSION_ID              
 ORDER BY B.BLOCKING_SESSION_ID, ORDEM               
              
 SELECT              
 R.SESSION_RAIZ, R.SESSION_ID              
 INTO #INSERT              
 FROM #RESULT R              
 WHERE R.SESSION_ID IN (SELECT BLOCKING_SESSION_ID FROM #RESULT WHERE SESSION_RAIZ = 0)              
              
  
UPDATE #RESULT SET SESSION_RAIZ = ISNULL((SELECT SESSION_RAIZ FROM #INSERT WHERE #RESULT.BLOCKING_SESSION_ID = #INSERT.SESSION_ID),0) WHERE SESSION_RAIZ = 0              

DECLARE @ID INT = 1    
WHILE 1=1    
BEGIN    
	IF EXISTS (SELECT DISTINCT blocking_session_id FROM #RESULT WHERE SESSION_RAIZ = 0 AND session_id = @ID)    
	BEGIN  
		UPDATE #RESULT SET SESSION_RAIZ = ISNULL((SELECT SESSION_RAIZ FROM #RESULT WHERE SESSION_ID = (SELECT DISTINCT blocking_session_id FROM #RESULT WHERE SESSION_RAIZ = 0 AND session_id = @ID)),0) WHERE SESSION_RAIZ = 0  
		IF EXISTS(SELECT * FROM #RESULT WHERE SESSION_RAIZ = 0 AND SESSION_ID = @ID AND (blocking_session_id NOT IN(select session_id from #CTE) AND blocking_session_id in (SELECT BLOCKING_SESSION_ID FROM #CTE2)))
		BEGIN
			UPDATE #RESULT SET SESSION_RAIZ = blocking_session_id WHERE session_id = @ID 
		END
	END    
	IF NOT EXISTS (SELECT SESSION_RAIZ FROM #RESULT WHERE SESSION_RAIZ = 0) BREAK    
	ELSE    
	SET @ID = @ID + 1    
END    
    
IF (SELECT OBJECT_ID('DBA..DBA_BLOCK')) IS NULL            
BEGIN            
 WITH CTE AS( SELECT * FROM #CTE UNION ALL SELECT * FROM #CTE2)              
  , RAIZ AS(              
   SELECT ROW_NUMBER() OVER(PARTITION BY SESSION_RAIZ ORDER BY ORDEM ASC) AS ORDEM_BLOQUEIO,SESSION_ID, BLOCKING_SESSION_ID,SESSION_RAIZ FROM #RESULT)              
  , CTE2 AS(              
  SELECT CASE WHEN RAIZ.SESSION_RAIZ IS NULL THEN CTE.SESSION_ID ELSE RAIZ.SESSION_RAIZ END AS RAIZ,CTE.*               
  FROM CTE              
  LEFT JOIN RAIZ ON RAIZ.SESSION_ID = CTE.SESSION_ID)              
  SELECT * into DBA..DBA_BLOCK  FROM CTE2 WHERE WAIT_TIME > @time OR ISNULL(BLOCKING_SESSION_ID,0) = 0 ORDER BY RAIZ, WAIT_TIME -- coleta bloqueios acima de X minuto              
END            
IF @JOB = 1            
BEGIN            
  WITH CTE AS( SELECT * FROM #CTE UNION ALL SELECT * FROM #CTE2)              
  , RAIZ AS(              
   SELECT ROW_NUMBER() OVER(PARTITION BY SESSION_RAIZ ORDER BY ORDEM ASC) AS ORDEM_BLOQUEIO,SESSION_ID, BLOCKING_SESSION_ID,SESSION_RAIZ FROM #RESULT)              
  , CTE2 AS(              
  SELECT CASE WHEN RAIZ.SESSION_RAIZ IS NULL THEN CTE.SESSION_ID ELSE RAIZ.SESSION_RAIZ END AS RAIZ,CTE.*               
  FROM CTE              
  LEFT JOIN RAIZ ON RAIZ.SESSION_ID = CTE.SESSION_ID)              
  INSERT INTO DBA..DBA_BLOCK SELECT * FROM CTE2 WHERE WAIT_TIME >= @time OR ISNULL(BLOCKING_SESSION_ID,0) = 0 ORDER BY RAIZ, WAIT_TIME -- coleta bloqueios acima de X minuto              
END            
ELSE            
  WITH CTE AS( SELECT * FROM #CTE UNION ALL SELECT * FROM #CTE2)              
  , RAIZ AS(              
   SELECT ROW_NUMBER() OVER(PARTITION BY SESSION_RAIZ ORDER BY ORDEM ASC) AS ORDEM_BLOQUEIO,SESSION_ID, BLOCKING_SESSION_ID,SESSION_RAIZ FROM #RESULT)              
  , CTE2 AS(              
  SELECT CASE WHEN RAIZ.SESSION_RAIZ IS NULL THEN CTE.SESSION_ID ELSE RAIZ.SESSION_RAIZ END AS RAIZ,CTE.*               
  FROM CTE              
  LEFT JOIN RAIZ ON RAIZ.SESSION_ID = CTE.SESSION_ID)              
 SELECT * FROM CTE2 WHERE WAIT_TIME >= @TIME OR ISNULL(BLOCKING_SESSION_ID,0) = 0 ORDER BY RAIZ, WAIT_TIME -- retorna bloqueios acima de x minuto 
