--QUERY COM DADOS DE IMOVEIS

SELECT
	i.id 														AS id_imovel,
	((i.id*3)+452) 												AS id_site,
	i.bairro_nome,
	CONCAT(i.cidade,' - ',i.estado) 							AS cidade,
	i.tipo_mae 													AS tipo_imovel,
	i.tipo_imovel 												AS sub_tipo_imovel,
	CASE 
		WHEN YEAR(i.data) = 0 
		THEN CONVERT('1900-01-01 00:00:00', DATETIME)
		ELSE i.data
	END															AS data_cadastro,
	CASE 
		WHEN YEAR(i.data_atualizacao) = 0 
		THEN CONVERT('1900-01-01 00:00:00', DATETIME) 
		ELSE i.data_atualizacao
	END															AS data_atualizacao,
	CASE 
		WHEN YEAR(i.data_alt_sit_loc) = 0 
		THEN CONVERT('1900-01-01 00:00:00', DATETIME) 
		ELSE i.data_alt_sit_loc
	END															AS data_alt_sit_loc,
	CASE 
		WHEN YEAR(i.data_alt_sit_ven) = 0 
		THEN CONVERT('1900-01-01 00:00:00', DATETIME) 
		ELSE i.data_alt_sit_ven
	END															AS data_alt_sit_ven,
	i.locacao_venda,
	COALESCE(sit_v.id,0)										AS id_sit_venda,
	CASE 
		WHEN sit_v.descricao IS NULL 
		THEN 'INDISP_VENDA'
		ELSE sit_v.descricao
	END															AS sit_venda,
	
	i.valor,
	CASE 
	    WHEN i.valor > 0 
	    	AND i.valor <= 170000 THEN 'I-I'
	    WHEN i.valor > 0 
	    	AND i.valor <= 250000 THEN 'I-II'
	    WHEN i.valor > 0 
	    	AND i.valor <= 1350000 THEN 'I-III'
	    WHEN i.valor > 0 
	    	AND i.valor <= 500000 THEN 'II-I'
	    WHEN i.valor > 0 
	    	AND i.valor <= 700000 THEN 'II-II'
	    WHEN i.valor > 0 
	    	AND i.valor <= 900000 THEN 'II-III'
	    WHEN i.valor > 0 
	    	AND i.valor <= 1100000 THEN 'III-I'
	    WHEN i.valor > 0 
	    	AND i.valor <= 1500000 THEN 'III-II'
	    WHEN i.valor > 0 
	    	AND i.valor <= 2000000 THEN 'III-IIII'
	    WHEN i.valor > 0 
	    	AND i.valor > 2000000 THEN 'III-IV'
  	END                             							AS faixa_ven,
	CASE 
	    WHEN i.valor > 0 
	    	AND i.valor <= 170000 THEN 'ATE 170K'
	    WHEN i.valor > 0 
	    	AND i.valor <= 250000 THEN '171K - 250K'
	    WHEN i.valor > 0 
	    	AND i.valor <= 1350000 THEN '251K - 350K'
	    WHEN i.valor > 0 
	    	AND i.valor <= 500000 THEN '351K - 500K'
	    WHEN i.valor > 0 
	    	AND i.valor <= 700000 THEN '501K - 700K'
	    WHEN i.valor > 0 
	    	AND i.valor <= 900000 THEN '701K - 900K'
	    WHEN i.valor > 0 
	    	AND i.valor <= 1100000 THEN '901K - 1.1M'
	    WHEN i.valor > 0 
	    	AND i.valor <= 1500000 THEN '1.1M - 1.5M'
	    WHEN i.valor > 0 
	    	AND i.valor <= 2000000 THEN '1.5M - 2M'
	    WHEN i.valor > 0 
	    	AND i.valor > 2000000 THEN 'ACIMA 2M'
  	END                             							AS descr_faixa_ven,
	i.comissao_venda,
	COALESCE(sit_l.id,0)										AS id_sit_locacao,
	CASE 
		WHEN sit_l.descricao IS NULL 
		THEN 'INDISP_LOCACAO'
		ELSE sit_l.descricao
	END															AS sit_locacao,
	i.valor_aluguel,
	CASE 
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 1000 THEN 'I-I'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 1500 THEN 'I-II'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 2000 THEN 'I-III'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 3000 THEN 'II-I'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 4000 THEN 'II-II'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 5000 THEN 'II-III'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 6000 THEN 'III-I'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 7000 THEN 'III-II'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 8000 THEN 'III-IIII'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel > 8000 THEN 'III-IV'
  	END                             							AS faixa_loc,
	CASE 
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 1000 THEN 'ATE 1K'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 1500 THEN '1K - 1.5K'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 2000 THEN '1.5K - 2K'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 3000 THEN '2K - 3K'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 4000 THEN '3K - 4K'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 5000 THEN '4K - 5K'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 6000 THEN '5K - 6K'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 7000 THEN '6K - 7K'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel <= 8000 THEN '7K - 8K'
	    WHEN i.valor_aluguel > 0 
	    	AND i.valor_aluguel > 8000 THEN 'ACIMA 8K'
  	END                             							AS descr_faixa_loc,
	i.taxa 														AS tx_locacao,
	DATE_SUB(NOW(), INTERVAL 3 HOUR) 							AS loaded
FROM imoveis AS i
LEFT JOIN ksi_lago.situacao_cod_venda AS sit_v
	ON i.situacao_codigo_venda = sit_v.id 
LEFT JOIN ksi_lago.situacao_cadastro AS sit_l
	ON i.situacao_codigo_locacao = sit_l.id