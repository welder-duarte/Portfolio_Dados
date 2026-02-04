-- QUERY COM DADOS DOS CADASTRADORES/CAPTADORES DE IMOVEIS

SELECT
	im_cad.*,
	DATE_SUB(NOW(), INTERVAL 3 HOUR) 									AS loaded
FROM ksi_lago.imoveis_cadastrador AS im_cad