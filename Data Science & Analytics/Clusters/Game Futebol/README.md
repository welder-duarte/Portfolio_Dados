### Campeonato futebol (game)
Esse projeto foi desenvolvido em uma etapa de avaliação técnica de um processo seletivo para uma vaga de analista de dados.

#### *O Problema*
Uma plataforma global de games quer montar torneios em um de seus games de futebol para seus usuários, mas como o jogo tem diferentes niveis
de jogadores, precisa buscar uma maneira de equalizar os competidores, de modo que usuários experts dispute torneios contra outros usuários 
do mesmo nível, e usuários iniciantes dispute contra outros usuários iniciantes, e assim por diante.

Você como analista de dados dessa plataforma, foi encarregado de entender quais são os perfis de jogadores existentes e a partir dessas 
informações, sugerir como elaborar campeonatos de modo a engajar os usuários no game.

#### *Dados*
O teste possui 2 bases de dados:
- ``Jogadores:`` Base com 10k de usuários distintos com: player_id, player_nome, country, last_login
- ``Partidas:`` Base com 270,3k de partidas distintas com: partida_id, player_id, tempo_partida, gols_marcados, gols_recebidos e status (**V**itoria, **E**mpate, **D**errota)
- Existe um gerador de dados aleatórios para a base de partidas, caso queiram aplicar esse teste mas com dados diferentes.

#### *Técnicas e tecnologias utilizadas:*
- ``Jupter Notebook (Anaconda)``
- ``Python (Pandas, Numpy)``
- ``Seaborn``
- ``Plotly``
- ``Sklearn (K-means, StandardScaler, PCA)``
- ``Yellowbrick (KElbowVisualizer)``

#### *Detalhamento e solução*

##### *Gerando novas variáveis*
A partir dos dados disponíveis, agrupando-os por jogador, podemos gerar novas variáveis como total de partidas por usuário, média de gols por partida,
contagens de vitórias, empates e derrotas, tempo total jogado e taxa de vitórias (o racional de todas essas variáveis estão disponíveis no notebook).

![Sample da base jogadores](https://github.com/welder-duarte/Portfolio_DataScience/assets/85957982/408eea3b-945f-4e50-a21a-e7e18503001d)

##### *Pré-Processamento e redução de dimensionalidade com PCA[^1]*
Antes de realizarmos a clusterização, precisamos normalizar os dados, de modo a reduzir a escala entre as variáveis, uma vez que temos dados 
em decimais (tx_vitoria) e dados inteiros com altos valores (como gols_marcados e tempo_total).

Após normalizar os dados seguimos com o PCA, onde transformamos o conjunto de dados com 8 variáveis em um novo conjunto com apenas 2. Essa transformação
nos permitirá analisar os dados de forma visual em um gráfico 2D (grafico abaixo):

![Grafico PCA](https://github.com/welder-duarte/Portfolio_DataScience/assets/85957982/0a9e6db4-791f-4b50-a749-eb5ed7e43777)


##### *Clusterização*
Com os dados normalizados e transformados, podemos seguir para a clusterização com K-means. Nesta etapa precisamos descobrir a quantidade
ideal de clusters, uma vez que o problema não nos deu um valor base. Para isso usamos KElbowVisualizer (biblioteca Yellowbrick) que processa 
nossos dados e nos retorna o "cotovelo" do método Elbow. Uma das vantagens dessa biblioteca é que o número ideal de clusters é retornado pela função,
então podemos usá-lo diretamente na etapa do k-means (k = visualizador.elbow_value_), sem precisar imputar manualmente o valor para k.

![Elbow](https://github.com/welder-duarte/Portfolio_DataScience/assets/85957982/5783ac9c-1c2a-4544-9054-2b5d02b995c1)

Com o número de clusters definidos (k = 3), podemos rodar o k-means, que nesse caso, gerou o resultado abaixo:

![clusters1](https://github.com/welder-duarte/Portfolio_DataScience/assets/85957982/e545ee92-21f6-481c-a042-cb689569418c)

##### *Analisando os resultados*
Agrupando a base de dados pelos clusters e analisando as médias de cada variável temos:

![Resultados clusters](https://github.com/welder-duarte/Portfolio_DataScience/assets/85957982/d8e714d0-afd1-4ffa-b0c5-1df346e667e2)

Com base na tabela acima podemos obervar que as variáveis partidas, gols e taxa de vitórias são as mais significativas quando se trata 
de separar os grupos, dessa forma, podemos plotar um gráfico para nos ajudar a analisar os grupos de forma visual 
(taxa vitórias x partidas disputadas, com tamanho das bolhas = gols marcados):

![Clusters 2D](https://github.com/welder-duarte/Portfolio_DataScience/assets/85957982/6f547863-201a-41d9-8e2f-8ea1c1e74c06)

``Azul - Cluster 0: OS LIDERES``
Menor grupo com 26% dos jogadores (2599 de 10k). Os jogadores desse grupo sabem o que estão fazendo.
Seus números de gols marcados, tempo jogado e partidas não destoam dos demais grupos, mas apresentam a maior taxa de vitórias e 
gols por partidas, mostrando que embora não joguem tanto, não estão para brincadeira e são eficientes na busca pela vitória.

SUGESTÃO: Campeonatos curtos, modalidade mata-mata pode ser interessante para esse grupo.

``Rosa - Cluster 1: OS EXPERIENTES``
Maior grupo de jogadores com 47,5% do total (4750 de 10k). 
Esse grupo apresenta a maior media de gols marcados, entretanto não possuem a melhor taxa de vitória, ficando em 2º nesse quesito.
Apresentam também a maior média de partidas disputadas, mostrando serem jogadores frequentes e familizarizados com o game.

SUGESTÃO: Campeonatos longos com pontos corridos para esse grupo, onde o uso de estratégias possam ser usadas para garantir as vitórias.

``Amarelo - Cluster 2: OS INICIANTES``
Grupo com 26,5% dos jogadores (2651 de 10k).
Apresentam os piores números de gols marcados e vitórias, embora tenham disputado quase a mesma quantidade de partidas do cluster 0.

SUGESTÃO: Por se tratar de um grupo de jogares iniciantes, campeonatos rápidos e partidas livres (amistosos) podem ser interessante 
para adquirirem experiência no game.


[^1]: PCA = Principal Component Analysis. Método que permite transformar um conjunto de dados de alta dimensionalidade (muitas variáveis) 
em um novo conjunto, fornecendo uma visualização em dimensões mais baixas dos mesmos dados, utilizando a informação contida na matriz de covariância dos dados.
