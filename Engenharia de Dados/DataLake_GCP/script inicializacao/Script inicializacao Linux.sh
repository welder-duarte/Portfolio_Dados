#!/bin/bash

# OBSERVACOES:
# 1 - Scripts e queries foram previamente carregados para Google Storage
# 2 - Alguns dados foram omitidos neste documento por seguranca, mas os comandos seguem as documentacoes
# 3 - Links dos backups no Saas foram coletados via web scraping com Selenium. Omitidos por seguranca.
# 4 - Esse script pode ser transcrito no campo de automacao ou carregado como metadados para ser executado na inicializacao da VM. 
# Doc: https://cloud.google.com/compute/docs/instances/startup-scripts/linux?hl=pt-br

# Carregando scripts para VM
gcloud storage cp gs://gs_user/scripts/* /home/user/scripts

# Executando downloads dos arquivos bkp (pasta backups criada previamente na VM. Links omitidos por seguranca)
wget -O /home/user/backups/ARQ_BKP_1.bak 'https://link_para_o_arquivo1'
wget -O /home/user/backups/ARQ_BKP_2.sql.gz 'https://link_para_o_arquivo2'

# Carregando copias para o storage (pasta backups criada previamente na VM)
gcloud storage cp /home/user/backups/ARQ_BKP_1.bak gs://gs_user/backups
gcloud storage cp /home/user/backups/ARQ_BKP_2.sql.gz gs://gs_user/backups

# Restaurando bd SqlServer, executando e gravando consultas (pasta outputs criada previamente na VM)
sqlcmd -S localhost -U sa -P 'senha_bd' -d master -Q "RESTORE DATABASE nome_bd FROM DISK = '/home/user/backups/ARQ_BKP_1.bak' WITH REPLACE"
sqlcmd -S localhost -U sa -P 'senha_bd' -d nome_bd -i "/home/user/scripts/tab_ms_usu_func.sql" -o "/home/user/outputs/tab_ms_usu_func.csv" -s "," -W -f 65001 && sed -i '2d' "/home/user/outputs/tab_ms_usu_func.csv"
sqlcmd -S localhost -U sa -P 'senha_bd' -d nome_bd -i "/home/user/scripts/tab_ms_usuarios.sql" -o "/home/user/outputs/tab_ms_usuarios.csv" -s "," -W -f 65001 && sed -i '2d' "/home/user/outputs/tab_ms_usuarios.csv"

# Descompactando, restaurando bd Mysql, executando e gravando consultas
gunzip -f /home/user/backups/ARQ_BKP_2.sql.gz
mysql -u root -p'senha_bd' -e "CREATE DATABASE nome_bd;"
mysql -u root -p'senha_bd' nome_bd < /home/user/backups/ARQ_BKP_2.sql
mysql -u root -p'senha_bd' -D nome_bd -e "source /home/user/scripts/tab_my_chaves.sql;" | tr '\t' ',' > /home/user/outputs/tab_my_chaves.csv
mysql -u root -p'senha_bd' -D nome_bd -e "source /home/user/scripts/tab_my_deptos.sql;" | tr '\t' ',' > /home/user/outputs/tab_my_deptos.csv
mysql -u root -p'senha_bd' -D nome_bd -e "source /home/user/scripts/tab_my_empresa.sql;" | tr '\t' ',' > /home/user/outputs/tab_my_empresa.csv
mysql -u root -p'senha_bd' -D nome_bd -e "source /home/user/scripts/tab_my_imov_cad.sql;" | tr '\t' ',' > /home/user/outputs/tab_my_imov_cad.csv
mysql -u root -p'senha_bd' -D nome_bd -e "source /home/user/scripts/tab_my_imoveis.sql;" | tr '\t' ',' > /home/user/outputs/tab_my_imoveis.csv
mysql -u root -p'senha_bd' -D nome_bd -e "source /home/user/scripts/tab_my_placas.sql;" | tr '\t' ',' > /home/user/outputs/tab_my_placas.csv

# Carregando resultados para Storage
gcloud storage cp /home/user/outputs/*.csv gs://gs_user/outputs

# Carregando dados do Storage para BigQuery
bq load --source_format=CSV --autodetect --replace schema_bigquery.tab_my_chaves gs://gs_user/outputs/tab_my_chaves.csv
bq load --source_format=CSV --autodetect --replace schema_bigquery.tab_my_deptos gs://gs_user/outputs/tab_my_deptos.csv
bq load --source_format=CSV --autodetect --replace schema_bigquery.tab_my_empresa gs://gs_user/outputs/tab_my_empresa.csv
bq load --source_format=CSV --autodetect --replace schema_bigquery.tab_my_imov_cad gs://gs_user/outputs/tab_my_imov_cad.csv
bq load --source_format=CSV --autodetect --replace schema_bigquery.tab_my_imoveis gs://gs_user/outputs/tab_my_imoveis.csv
bq load --source_format=CSV --autodetect --replace schema_bigquery.tab_my_placas gs://gs_user/outputs/tab_my_placas.csv
bq load --source_format=CSV --autodetect --replace schema_bigquery.tab_ms_usu_func gs://gs_user/outputs/tab_ms_usu_func.csv
bq load --source_format=CSV --autodetect --replace schema_bigquery.tab_ms_usuarios gs://gs_user/outputs/tab_ms_usuarios.csv

# Dropando bancos de dados da VM
sqlcmd -S localhost -U sa -P 'senha_bd' -Q "DROP DATABASE nome_bd;"
mysql -u root -p'senha_bd' -e "DROP DATABASE nome_bd;"

# Removendo arquivos da VM
sudo rm -rf /home/user/backups/*
sudo rm -rf /home/user/outputs/*

# Desligando a VM
sudo shutdown now
gcloud compute instances stop nome_vm --zone=us-central1-a