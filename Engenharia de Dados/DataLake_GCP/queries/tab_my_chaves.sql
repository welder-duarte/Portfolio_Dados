-- QUERY COM DADOS DE CHAVES DOS IMOVEIS

SELECT
	ch_im.id,
	ch_im.id_imovel,
	ch_im.`data`,
	ch_cad.chave_nome,
	ch_emp.locacao_venda,
	ch_emp_m.EmpNom 						AS empresa,
	DATE_SUB(NOW(), INTERVAL 3 HOUR) 		AS loaded
FROM ksi_lago.chave_imoveis AS ch_im
LEFT JOIN ksi_lago.chave_cadastro AS ch_cad
	ON ch_im.id_chave = ch_cad.id
LEFT JOIN ksi_lago.chave_empresa AS ch_emp
	ON ch_im.id_empresa = ch_emp.id
LEFT JOIN ksi_lago.chave_empresa_mae AS ch_emp_m
	ON ch_emp.empresa = ch_emp_m.EmpCod