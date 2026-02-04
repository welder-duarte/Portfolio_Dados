#!/usr/bin/env python
# coding: utf-8

import re
import numpy as np
import pandas as pd
import warnings

# Ignorar Warnings
warnings.filterwarnings('ignore')

from pandas import DataFrame
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from unidecode import unidecode
from typing import *
from dtype_diet import report_on_dataframe, optimize_dtypes


##### GERACAO DE OBJETOS SOUP

# FUNCAO PARA GERAR NRO TOTAL DE PAGINAS DO SITE
def total_paginas_site(url:str):
    """
    Retorna o total de páginas de um site.

    Parameters:
    - url (str): URL base.

    Returns:
    - Paginas (int): Quantidade de páginas de anúncios (macro) encontradas no site.
    """
    soup = gerando_soup_principal(url, 1000000)
    texto = soup.find('div', {'class':'page_indicator'}).ul.findAll('li')
    texto = texto[2:-1]
    paginas = int(texto[-1].get_text())
    
    return paginas

# FUNCAO PARA GERAR OBJETO SOUP DAS PAGINAS PRINCIPAIS (MACRO)
def gerando_soup_principal(url: str, num_pag: int):
    """
    Gera um objeto BeautifulSoup para uma página específica de uma URL paginada.

    Parameters:
    - url (str): URL base.
    - num_pag (int): Número da página.

    Returns:
    - Retorna um objeto BeautifulSoup se a solicitação for bem-sucedida, caso contrário, retorna 0.
    """

    # Url paginada
    url = url + f'&&pag={num_pag}'
    
    # Headers - Informações fake de navegador
    header = {'User-Agent':'Mozilla/5.0'}

    try:
        resp = urlopen(Request(url, headers=header))  # Requisitando dados da página
        resp = resp.read()  # Lendo dados
        soup = BeautifulSoup(resp, 'html.parser')  # Carregando em objeto BeautifulSoup
        return soup

    except:
        return 0

    
# FUNCAO PARA GERAR OBJETO SOUP DO ANUNCIO ESPECIFICO
def gerando_soup_anuncio(url: str):
    """
    Gera um objeto BeautifulSoup para uma página de um anúncio específico.

    Parameters:
    - url (str): URL base.

    Returns:
    - Retorna um objeto BeautifulSoup se a solicitação for bem-sucedida, caso contrário, retorna 0.
    """
    # Headers - Informações para fingir ser um navegador
    header = {'User-Agent':'Mozilla/5.0'}

    try:
        resp = urlopen(Request(url, headers=header)) # Requisitando dados da página
        resp = resp.read() # Lendo dados
        soup = BeautifulSoup(resp, 'html.parser') # Carregando em objeto BeautifulSoup
        return soup

    except:
        return 0


##### TRATAMENTOS DOS DADOS

# FUNCAO PARA SEPARAR ITENS DO IMOVEL E DO CONDOMINIO EM NOVOS DFS
def separa_itens(base: DataFrame, coluna: str):
    """
    Gera novos dataframes a partir de uma lista de itens.

    Parameters:
    - base (DataFrame): Base com os dados.
    - coluna (str): Nome da coluna para ser dividida.

    Returns:
    - Retorna um dataframe com os itens listados em linhas.
    """
    texto = base[coluna].astype(str).str.lower().apply(unidecode).str.replace('[','').str.replace(']','').str.strip()
    df = texto.str.split(',', expand=True)
    df['id'] = base['id']
    df = df.melt(id_vars=['id'], value_name='itens')
    df.dropna(subset=['itens'], inplace=True)
    excluir = df[(df['itens'] == '') | (df['itens'] == '\t')]
    df.drop(excluir.index, inplace=True)
    df.drop(columns='variable', inplace=True)
    df.sort_values(by='id', inplace=True)
    df.reset_index(inplace=True, drop=True)
    
    return df

# FUNCAO PARA REMOVER OUTLIERS EM VARIAVEIS NUMERICAS
def remove_outliers(base: DataFrame, threshold=3):
    """
    Remove outliers em variaveis numericas (int,float) considerando 3 desvios padrão como default.

    Parameters:
    - base (DataFrame): O DataFrame a ser modificado.

    Returns:
    - base: O DataFrame modificado.
    """
    dados = base.dtypes
    typ = ['int8','int16','int32','int64','float8','float16','float32','float64']
    colunas = []

    for coluna, tipos in dados.items():
        if tipos in typ and coluna != 'registros':
            z_scores = np.abs((base[coluna] - base[coluna].mean()) / base[coluna].std())
            base = base[z_scores < threshold]

    return base


# FUNCAO PARA CONVERTER TIPAGEM DOS DADOS DO DF
def tipagem(base: DataFrame):
    """
    Converte a tipagem dos dados em colunas numericas (int,float).

    Parameters:
    - base (DataFrame): O DataFrame a ser modificado.

    Returns:
    - base: O DataFrame modificado.
    """
    dados = base.dtypes
    typ = ['int8','int16','int32','int64','float8','float16','float32','float64']
    colunas = []

    for coluna, tipos in dados.items():
        if tipos in typ:
            colunas.append(coluna)

    df_temp = base[colunas]
    df_temp2 = report_on_dataframe(df_temp, unit="MB")
    df_temp = optimize_dtypes(df_temp, df_temp2)

    base[colunas] = df_temp[colunas]
    
    return base
    
# FUNCAO QUE CORRIGE LINK EM CASO DE ERRO DE UNIDECODE
def safeStr(obj: object) -> str:
    """
    Converte um link para uma string segura (sem erros de encode).

    Parameters:
    - obj: Objeto a ser convertido.

    Returns:
    - str: Representação em string do objeto, com caracteres não ASCII removidos.
    """
    try: 
        return str(obj).encode('ascii', 'ignore').decode('ascii')
    except: 
        return ""
    
    
# FUNCAO PARA FORMATAR TEXTOS (LABELS DE COLUNAS)
def formata_textos(texto: str, func: str):
    """
    Transforma e formata uma string, removendo acentuações e convertendo para minúsculas, de acordo com a função informada.

    Parameters:
    - texto: Texto a ser tratado.
    - func: 
        header = Texto compoe um cabecalho.
        descricao = Texto descritivo, podendo conter telefones
        (vazio '') = Nenhuma funcionalidade especifica, formatando apenas o basico.

    Returns:
    - str: String formatada.
    - List: Lista de telefones identificados
    """
    texto = unidecode(texto).lower()
    texto = re.sub(r'\s$','', texto)
    texto = re.sub(r'\s\s+',' ', texto)
    list_tel = []
    
    if func == 'header':
        texto = re.sub('dormitorio[s]?','quartos',texto)
        texto = re.sub('banheiro[s]?','banheiros',texto)
        texto = re.sub('garage[mn][s]?','vagas',texto)
        texto = re.sub(' ','_',texto)
    elif func == 'descricao':
        texto = re.sub(r'[^\w\,]',' ', texto)
        texto = re.sub(r'(?<=\d)\s{1,4}(?=\d)','',texto)
        list_tel = re.findall(r'(?:(\d{2}\s+?\d{9}|\d{10,11}|\d{2}\s+?\d{8}|\d{2}\s+?\d{4,5}\s?\d{4}|\d{6,7}\s+?\d{4,5}))', texto)
        list_tel = [re.sub(r'\s','', tel) for tel in list_tel]
        
    return texto, list_tel

# FUNCAO PARA AJUSTAR VALORES NUMERICOS, REMOVENDO STRINGS E CONVERTENDO FLOAT/INT
def ajusta_valores(chave: str, valor: str):
    """
    Ajusta valores de acordo com a chave fornecida.

    Parameters:
    - chave (str): A chave indicando o tipo de ajuste a ser aplicado.
    - valor (str): O valor a ser ajustado.

    Returns:
    - float or int: O valor ajustado.
    """
    chave = chave.split('_')[0]
    
    try:
        if chave == 'area':
            valor = re.sub(r'[^0-9\.]','',valor)
            valor = float(valor)
        elif chave == 'valor':
            valor = re.sub(r'[^0-9\,]','',valor)
            valor = valor.replace(',','.')
            valor = float(valor)
        elif chave == 'suites':
            if 'sendo' in valor:
                valor = re.sub(r'[^0-9\,]','',valor).split(',') 
                valor = int(valor[-1])
            else:
                valor = 0
    except:
        valor = 0

    return valor


##### SCRAPING DOS DADOS

# FUNCAO PARA PEGAR OS DADOS DAS PAGINAS PRINCIPAIS
def dados_pag_principal(anuncio: str, pag: int):
    """
    Extrai dados relevantes de um anúncio na página principal (macro).

    Parameters:
    - anuncio (str): O conteúdo HTML do anúncio.
    - pag (int): O número da página que está sendo analisada.

    Returns:
    - dict: Um dicionário contendo os dados extraídos.
    """
    dados = {}
    
    # Valor
    texto = anuncio.find('div', {'class':'property_details'}).find('h4').get_text().strip()
    dados['valor'] = ajusta_valores('valor', texto)

    # Bairro e Cidade
    #texto = anuncio.find('div', {'class':'property_details'}).find('span').get_text().replace('\t','').strip().split('\n')
    #dados['bairro'] = formata_textos(texto[0],'')[0]
    #dados['cidade'] = formata_textos(texto[1],'')[0]

    # Dormitorios, banheiros e garagem
    texto = anuncio.findAll('li', {'class':'tooltip'})
    itens_de_interesse = ['quartos','banheiros','vagas']
    for item in texto:
        texto = item.get('title')[:-4].strip()
        texto = formata_textos(texto,'header')[0]
        if texto in itens_de_interesse:
            dados[texto] = int(item.get_text())

    # Descricao
    texto = anuncio.find('div', {'class':'property_details_desc'}).find('p').get_text()
    x = formata_textos(texto,'descricao')
    dados['descricao'] = x[0]
    dados['telefones'] = x[1]

    # link da pagina e foto da capa
    texto = anuncio.find('div', {'class':'img_holder'})('a')
    link = 'https://www.lagoimobiliaria.com.br/' + texto[0].get('href')
    foto = 'https:' + texto[0].img.get('src')

    dados['link'] = link
    dados['link_foto_capa'] = foto
    
    dados['pagina'] = pag
    
    return dados

# FUNCAO PARA PEGAR OS DADOS COMPLEMENTARES DENTRO DA PAGINA DE CADA ANUNCIO INDIVIDUALMENTE
def dados_pag_anuncio(anuncio: str, index: int):
    """
    Extrai dados complementares dos anúncios macros, retirando dados do anúncio fornecido específicamente.

    Parameters:
    - anuncio (str): O conteúdo HTML do anúncio.
    - index (int): Indice do anúncio no dataframe principal. Usado para join entre dfs.

    Returns:
    - dict: Um dicionário contendo os dados extraídos.
    """
    dados = {}
    dados['indice'] = index
    try:
        texto = anuncio.findAll('div', {'class':'single_input'})
        for item in texto:
            chave = item.find('span').get_text()
            chave = formata_textos(chave,'header')[0]

            if chave == 'quartos':
                chave = 'suites'

            chaves_de_interesse = ['valor_venda','valor_condominio','valor_iptu','classificacao','suites',
                                   'area_terreno','area_construida','valor_aluguel','area_util','area_total']

            if chave in chaves_de_interesse:
                dados[chave] = ajusta_valores(chave,item.find('div', {'class':'item-detalhe'}).get_text().strip())
        try:
            # Pegando dados dos imoveis se disponivel
            texto = anuncio.findAll('div', {'class':'public_facilities'})
            itens_imovel = texto[0].findAll('li')
            list_itens_imovel = []
            for item_imovel in itens_imovel:
                list_itens_imovel.extend(item_imovel.get_text().strip().split(','))
                
            dados['itens_do_imovel'] = list_itens_imovel
        except:
            dados['itens_do_imovel'] = []

        try:
            # Pegando dados do condominio se disponivel
            itens_cond = texto[1].findAll('li')
            list_itens_cond = []
            for item_cond in itens_cond:
                list_itens_cond.extend(item_cond.get_text().strip().split('\n '))
            dados['itens_do_condominio'] = list_itens_cond
        except:
            dados['itens_do_condominio'] = []
            
            
        try:
            # Pegando dados de bairro, zona e cidade
            texto = formata_textos(anuncio.findAll('ul', {'class':'nova-desc'})[0].get_text(), '')[0]
            texto = texto.replace('\nlocalizacao: ', '').split(',')
            dados['bairro'] = texto[0].strip()
            
            texto2 = texto[-1].strip()
            texto2 = texto2.split(' em ')
            dados['zona'] = texto2[0].strip()
            dados['cidade'] = texto2[-1].replace('/', '-').strip()
        except:
            pass
            
    except:
        return 'erro'
    
    return dados