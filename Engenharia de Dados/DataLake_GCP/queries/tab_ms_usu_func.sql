--QUERY COM DADOS DE FUNCOES DOS USUARIOS

SET NOCOUNT ON;
SELECT 
	id,
	funcao,
	flag_gerente,
	(CASE WHEN funcao LIKE 'Diret%' THEN 1 ELSE 0 END) 		AS flag_diretoria,
	CAST(CONVERT(VARCHAR(19),
			DATEADD(HOUR,-3,GETDATE()),120) AS DATETIME)	AS loaded
FROM kurole_usuarios_funcao