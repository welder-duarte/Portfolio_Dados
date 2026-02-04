-- QUERY DEPARTAMENTOS DOS USUARIOS

SELECT
	id,
	nome 								AS departamento,
	locacao_venda 						AS locacao_venda,
	flag_locacao 						AS flag_locacao,
	flag_venda							AS flag_venda,
	flag_empreendimento 				AS flag_empreend,
	flag_transfere_departamento 		AS flag_transf_depto,
	DATE_SUB(NOW(), INTERVAL 3 HOUR) 	AS loaded
FROM ksi_lago.departamentos