MERGE `funds_explorer.funds_ranking_silver` AS tgt
USING (
  SELECT
  fundos,
  setor,
  SAFE_CAST(REPLACE(REPLACE(preco_atual, 'R$', ''), ',', '.') AS FLOAT64) AS preco_atual,
  SAFE_CAST(REPLACE(REPLACE(liquidez_diaria, 'R$', ''), ',', '.') AS FLOAT64) AS liquidez_diaria,
  SAFE_CAST(REPLACE(p_vp, ',', '.') AS FLOAT64) AS p_vp,
  SAFE_CAST(REPLACE(REPLACE(ultimo_dividendo, 'R$', ''), ',', '.') AS FLOAT64) AS ultimo_dividendo,
  SAFE_CAST(REPLACE(REPLACE(dividend_yield, '%', ''), ',', '.') AS FLOAT64) AS dividend_yield,
  SAFE_CAST(REPLACE(REPLACE(dy_3m_acumulado, '%', ''), ',', '.') AS FLOAT64) AS dy_3m_acumulado,
  SAFE_CAST(REPLACE(REPLACE(dy_6m_acumulado, '%', ''), ',', '.') AS FLOAT64) AS dy_6m_acumulado,
  SAFE_CAST(REPLACE(REPLACE(dy_12m_acumulado, '%', ''), ',', '.') AS FLOAT64) AS dy_12m_acumulado,
  SAFE_CAST(REPLACE(REPLACE(dy_3m_media, '%', ''), ',', '.') AS FLOAT64) AS dy_3m_media,
  SAFE_CAST(REPLACE(REPLACE(dy_6m_media, '%', ''), ',', '.') AS FLOAT64) AS dy_6m_media,
  SAFE_CAST(REPLACE(REPLACE(dy_12m_media, '%', ''), ',', '.') AS FLOAT64) AS dy_12m_media,
  SAFE_CAST(REPLACE(REPLACE(dy_ano, '%', ''), ',', '.') AS FLOAT64) AS dy_ano,
  SAFE_CAST(REPLACE(REPLACE(variacao_preco, '%', ''), ',', '.') AS FLOAT64) AS variacao_preco,
  SAFE_CAST(REPLACE(REPLACE(rentab_periodo, '%', ''), ',', '.') AS FLOAT64) AS rentab_periodo,
  SAFE_CAST(REPLACE(REPLACE(rentab_acumulada, '%', ''), ',', '.') AS FLOAT64) AS rentab_acumulada,
  SAFE_CAST(REPLACE(REPLACE(patrimonio_liquido, 'R$', ''), ',', '.') AS FLOAT64) AS patrimonio_liquido,
  SAFE_CAST(REPLACE(REPLACE(vpa, 'R$', ''), ',', '.') AS FLOAT64) AS vpa,
  SAFE_CAST(REPLACE(p_vpa, ',', '.') AS FLOAT64) AS p_vpa,
  SAFE_CAST(REPLACE(REPLACE(dy_patrimonial, '%', ''), ',', '.') AS FLOAT64) AS dy_patrimonial,
  SAFE_CAST(REPLACE(REPLACE(variacao_patrimonial, '%', ''), ',', '.') AS FLOAT64) AS variacao_patrimonial,
  SAFE_CAST(REPLACE(REPLACE(rentab_patr_periodo, '%', ''), ',', '.') AS FLOAT64) AS rentab_patr_periodo,
  SAFE_CAST(REPLACE(REPLACE(rentab_patr_acumulada, '%', ''), ',', '.') AS FLOAT64) AS rentab_patr_acumulada,
  SAFE_CAST(REPLACE(quant_ativos, ',', '.') AS INT64) AS quant_ativos,
  SAFE_CAST(REPLACE(REPLACE(volatilidade, '%', ''), ',', '.') AS FLOAT64) AS volatilidade,
  SAFE_CAST(REPLACE(num_cotistas, '.', '') AS INT64) AS num_cotistas,
  SAFE_CAST(REPLACE(REPLACE(taxa_gestao, '%', ''), ',', '.') AS FLOAT64) AS taxa_gestao,
  SAFE_CAST(REPLACE(REPLACE(taxa_performance, '%', ''), ',', '.') AS FLOAT64) AS taxa_performance,
  SAFE_CAST(REPLACE(REPLACE(taxa_administracao, '%', ''), ',', '.') AS FLOAT64) AS taxa_administracao,
  scraping_date,
  CURRENT_TIMESTAMP() AS ingestion_timestamp  FROM `funds_explorer.funds_ranking_raw`
  WHERE scraping_date = @scraping_date
) AS src
ON tgt.fundos = src.fundos
AND tgt.scraping_date = src.scraping_date

WHEN NOT MATCHED THEN
  INSERT ROW;
