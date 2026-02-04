--QUERY COM DADOS DAS EMPRESAS

SELECT
	emp.id,
	emp.empresa,
	emp.locacao_venda,
	emp_m.EmpNom,
	DATE_SUB(NOW(), INTERVAL 3 HOUR) 	AS loaded
FROM ksi_lago.chave_empresa AS emp
LEFT JOIN ksi_lago.chave_empresa_mae AS emp_m
	ON emp.empresa = emp_m.EmpCod
WHERE emp.flag_ativo = 1