### **Criação de um DataLake com Google Cloud Plataform**
Esse projeto foi desenvolvido para atender às necessidades de uma empresa real.

#### *O Problema:*
Empresa do ramo imobiliário não possui uma estrutura de dados. Tem como principal ferramenta operacional um SaaS [^1], no qual disponibiliza os dados das vendas e 
operações através de arquivos de backups dos 2 bancos de dados utilizados: 1 backup SQLSERVER com dados financeiros e de contratos, e outro MYSQL como dados 
de CRM. 
Por não ter acesso aos próprios dados de forma prática, o gerenciamento dos resultados é ineficiente, com controles feitos em planilhas e alimentados manualmente, 
dedicando-se muito tempo em tarefas que não agregam valor ao negócio (como preencher planilhas com dados do site por exemplo). 
Esse projeto teve como principal objetivo criar um ETL [^2] automatizado dos dados, criando e disponibilizando uma camada de dados que possa ser utilizada para 
análises e acompanhamento das operações, via Power Bi, gerando valor ao negócio.

#### *Técnicas e tecnologias utilizadas:*
- ``Google Cloud Plataform (Cloud Engine + Cloud Storage + Cloud BigQuery)``
- ``Linux Ubuntu (VM)``
- ``SqlServer e MySql``
- ``Power Bi (Dataviz)``

#### *Arquitetura:*
Projeto seguiu a arquitetura abaixo. 
Dentro das infitas opções de stacks [^3], optou-se pelo uso de uma VM [^4] pela facilidade em concentrar 2 bancos de dados em um mesmo ambiente com a maioria dos 
comandos sendo executados via bash na inicialização do linux, o que mostrou ser mais simples de ser implementado. 

![Proj Cloud](https://github.com/welder-duarte/Portfolio_DataScience/assets/85957982/af730ae3-75ef-4932-8eed-78644e2ada4e)

[^1]: Software as service. Permite aos usuários se conectar e usar aplicativos baseados em nuvem pela Internet, pagando para ter acesso aos serviços.
[^2]: Extração, transformação e carregamento. Processo de combinação de dados de várias fontes em um grande repositório central (data warehouse).
[^3]: Em programação, uma stack (pilha) é um conjunto de tecnologias que são utilizadas na criação de aplicações.
[^4]: Uma máquina virtual é um ambiente virtual que funciona como um sistema de computação com sua própria CPU, memória, interface de rede e armazenamento. 
