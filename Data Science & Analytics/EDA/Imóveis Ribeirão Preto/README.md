### **Anúncios de imóveis em Ribeirão Preto (Web Scraping + Paralelismo)**
Esse projeto foi desenvolvido para explorar aplicações de EDA (Análise Exploratória de Dados).

#### *O Problema:*
Com objetivo aplicar conceitos de EDA em uma base de dados real, coletamos dados de anúncios de vendas de imóveis em uma página 
de uma imobiliária com web scraping e paralelismo, uma vez que essa tarefa tende a ser demorada. Após a coleta, limpeza e estruturação dos dados, partimos para a analise exploratória e visual com Tableau, buscando entender:
- Distribuição de preços dos imóveis por tipo.
- Distribuição geográfica dos imóveis na cidade.
- Relação da região do imóvel com os preços.
- Identificação dos bairros mais caros e mais acessíveis.
- Relação entre o número de quartos com os preços.
- Valores dos imóveis pelo tipo e quantidade de itens (do imóvel, de lazer, de segurança e condomínio).

#### *Técnicas e tecnologias utilizadas:*
- ``Jupter Notebook (Anaconda)``
- ``Python (Pandas, Numpy, Seaborn, Urllib.Request)``
- ``BeautifulSoup (Scraping)``
- ``Concurrent.futures (Paralelismo)``
- ``Modularização das funções (functions_proj_scraping)``
- ``Dtype-diet (Otimização de tipagem)``
- ``Api Google Maps (Definição de Lat e Long dos bairros)``
- ``Tableau Public (Dataviz)``

#### *Análise:*
A amostra de dados apresenta 7.885 registros pré-processados, porém, alguns anúncios ainda apresentavam informações incoerentes a respeito
da localização, de valores ou metragens, sendo necessário a exclusão diretamente no dashboard, ficando a base então com 7.839 registros.

- **``Apartamentos``**
  - Representam a maior parte dos anúncios: 70,5% (5.503)
  - Distribuição de preço m² fica entre R$ 3.315 (inf) e R$ 6.219 (sup) com mediana de R$ 4.475
  - Na zona norte (- valorizada) os valores estão 39,7% abaixo em relação à zona sul (+ valorizada). Obs: comparação entre medianas
  - Apartamentos com 1 quarto apresentam maiores valores, chegando a +26,5% em relação aos com 2/3 quartos e +20% em relação aos com 4/5 quartos
  - Distribuição de metragens fica entre 72m² (inf) e 175m² (sup) com mediana de 122,5m²
  - Top 5 bairros mais caros: todos estão na região sul da cidade (+ valorizada)
  - Top 5 bairros mais acessíveis: Distribuição entre Norte, Oeste e Leste. Região Central não aparece nos TOPs
  - Itens mais comuns ofertados nos apartamentos são: Sala de estar, box, sacada, armario, ventilador de teto, iluminação e ar condicionado
  - Foi possível observar que a quantidade de itens de lazer e segurança não afetam os valores dos anúncios. O mesmo não ocorre em relação a itens do imóvel, onde quanto mais itens o disponíveis, maior o preço do m² dos anúncios

- **``Casas``**
  - Representam 24,3% dos anúncios (1.904)
  - Distribuição de preço m² fica entre R$ 1.431 (inf) e R$ 2.463 (sup) com mediana de R$ 1.895
  - Na zona norte (- valorizada) os valores estão 31,8% abaixo em relação à zona sul (+ valorizada). Obs: comparação entre medianas
  - Casas com 3 quartos são mais valorizados, com o preço do m² em média de +16% em relação às casas com 1,2,4 ou 5 quartos
  - Distribuição de metragens fica entre 166m² (inf) e 390m² (sup) com mediana de 268m²
  - Top 5 bairros mais caros: 3 bairros situados na região leste e 2 na região sul
  - Top 5 bairros mais acessíveis: Distribuição entre Norte, Oeste e Leste. Região Central não aparece nos TOPs
  - Itens mais comuns ofertados são: Armário, box, churrasqueira, piscina, ventilador, ar condicionado e alarme
  - A quantidade de itens do imóvel afetam muito pouco os valores ofertados nos anúncios. O mesmo não ocorre em relação a itens de segurança e codomínio ou itens de lazer, onde quanto mais itens disponíveis, maior o preço do m² dos anúncios

- **``Casas em condomínios``**
  - Representam 5,5% dos anúncios (432)
  - Distribuição de preço m² fica entre R$ 2.166 (inf) e R$ 4.589 (sup) com mediana de R$ 3.710
  - Na zona sul (+ valorizada) os valores são 200% acima em relação à zona norte (- valorizada) e aproximadamente 100% acima das demais regiões. Não há casas em condomínios fechados na região central da cidade. Obs: comparação entre medianas
  - Casas com 3 quartos são mais valorizados, com o preço do m² em média de +95% em relação às casas com 2,4 ou 5 quartos. Não há anuncios de casas em condomínio com apenas 1 quarto.
  - Distribuição de metragens fica entre 251,5m² (inf) e 385,5m² (sup) com mediana de 306m²
  - Top 5 bairros mais caros: Todos localizados na região sul da cidade
  - Top 5 bairros mais acessíveis: Distribuição entre Sul e Leste
  - Itens mais comuns ofertados são (em orderm): churrasqueira, armário, piscina, ventilador, ar condicionado e portaria 24h
  - Todas as categorias de itens (do imóvel, de lazer ou de segurança) afetam os valores dos anúncios. Quanto mais itens disponíveis, maior o preço do m² dos anúncios

#### *Link para Dashboard:*
https://public.tableau.com/views/EDAImoveisemRibeiraoPreto/EDAimoveisRibeiroPreto?:language=pt-BR&publish=yes&:sid=&:display_count=n&:origin=viz_share_link

![EDA imoveis Ribeirão Preto](https://github.com/welder-duarte/Portfolio_DataScience/assets/85957982/d8fe7981-534c-42f1-88c0-51264fe380ee)
