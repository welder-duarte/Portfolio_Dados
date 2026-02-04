-- QUERY COM DADOS DE PLACAS

SELECT
	pi.id,
	pi.id_imovel,
	pt.nome 															AS tipo,
	ps.nome 															AS status,
	pa.nome 															AS autorizacao,
	pi.solicitacao_data_mao 											AS data_solicit,
	pi.locacao_venda,
	pi.id_usuario_criou,
	pi.id_usuario_para,
	pi.id_departamento,
	DATE_SUB(NOW(), INTERVAL 3 HOUR) 									AS loaded
FROM ksi_lago.placas_imoveis AS pi
LEFT JOIN ksi_lago.placas_tipo AS pt
	ON pi.id_tipo = pt.id
LEFT JOIN ksi_lago.placas_status AS ps
	ON pi.id_status = ps.id
LEFT JOIN ksi_lago.placas_autorizacao AS pa
	ON pi.id_autorizacao = pa.id
