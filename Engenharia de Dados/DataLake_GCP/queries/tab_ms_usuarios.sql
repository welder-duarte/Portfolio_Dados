--QUERY COM DADOS DE USUARIOS

SET NOCOUNT ON;
SELECT
	u.UsuId 												AS id_usuario,
	u.UsuFlgAtivo 											AS ativo,
	u.UsuNic 												AS nome,
	u.creci													AS creci,
	CONVERT(DATE,ISNULL(u.data_admissao,'1900-01-01'), 103) AS data_admissao,
	CONVERT(DATE,ISNULL(u.data_demissao,'1900-01-01'), 103)	AS data_demissao,
	u.UsuEmp												AS id_empresa,
	u.id_departamento										AS id_departamento,
	u.funcao												AS id_funcao,
	CAST(CONVERT(VARCHAR(19),
			DATEADD(HOUR,-3,GETDATE()),120) AS DATETIME)	AS loaded
FROM [dbo].[CADUSU] AS u
WHERE u.UsuId > 0