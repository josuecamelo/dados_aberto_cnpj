# -*- encoding: utf-8 -*-
import os
import glob
import sys
import csv
import datetime
import psycopg2

import json
import zipfile
import re
from urllib.parse import urlsplit

from pandas import pandas as pd

from cfwf import read_cfwf

csvFilePath = ''
jsonFilePath = ''
data = {}

def cnpj_full(input_list, tipo_output, output_path):
    # Update connection string information
    host = "localhost"
    dbname = "dados_cnpj"
    user = "postgres"
    password = "p0h7n5x2"

    # Construct connection string
    conn_string = "host={0} user={1} dbname={2} password={3}".format(host, user, dbname, password)
    conexaoDB = psycopg2.connect(conn_string)

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Itera sobre sequencia de arquivos (p/ suportar arquivo dividido pela RF)

    cursor = conexaoDB.cursor()

    for i_arq, arquivo in enumerate(input_list):
        print('Processando arquivo: {}'.format(arquivo))

        #customizacoes josue
        with zipfile.ZipFile(arquivo) as z:
            parts = urlsplit(arquivo)
            paths = parts.path.split('\\')
            csvFile = paths[-1].rsplit(".", 1)[0]

            with z.open(csvFile) as f:
                df = pd.read_csv(f, sep=';', header=None, encoding='ISO-8859-1', na_filter=False, low_memory=False)
                for row in df.iterrows():
                    print(row)

                    sql = "INSERT INTO socios(cnpj_basico, identificador_socio, nome_socio, cpf_cnpj, qualificacoes_socio, data_entrada_sociedade, pais, cpf_representante_legal, nome_representante_legal, qualificacao_representante_legal, faixa_etaria) " \
                        "VALUES ('" + str(row[1][0]).zfill(8) + "', '" + str(row[1][1]) + "', '" + escapeSpecialCharacters(str(row[1][2])) + "'," \
                        "'" + str(row[1][3]) + "', '" + escapeSpecialCharacters(str(row[1][4])) + "', '" + str(row[1][5]) + "'," \
                        "'" + str(row[1][6]) + "', '" + str(row[1][7]) + "', '" + escapeSpecialCharacters(str(row[1][8])) + "'" \
                        ", '" + str(row[1][9]) + "', '" + str(row[1][10]) + "');"

                    cursor.execute(sql)
                    conexaoDB.commit()

        #final customizaçoes josue
    cursor.close()
    conexaoDB.close()

    print("########")
    print("Por Hoje é Só!")
    print("########")

def escapeSpecialCharacters (text):
    return text.replace("'", "''")

def main():

    num_argv = len(sys.argv)
    if num_argv < 4:
        help()
        sys.exit(-1)
    else:
        input_path = sys.argv[1]
        tipo_output = sys.argv[2]
        output_path = sys.argv[3]
        input_list = [input_path]

        if num_argv > 4:
            for opcional in sys.argv[4:num_argv]:
                if (opcional == '--noindex'):
                    gera_index = False

                elif (opcional == '--dir'):
                    input_list = glob.glob(os.path.join(input_path,'*.zip'))

                    if not input_list:
                        # caso nao ache zip, procura arquivos descompactados.
                        input_list = glob.glob(os.path.join(input_path,'*.L*'))

                    if not input_list:
                        # caso nem assim ache, indica erro.
                        print(u'ERRO: Nenhum arquivo válido encontrado no diretório!')
                        sys.exit(-1)

                    input_list.sort()

                else:
                    print(u'ERRO: Argumento opcional inválido.')
                    help()
                    sys.exit(-1)

        if tipo_output not in ['csv','sqlite']:
            print('''
ERRO: tipo de output inválido. 
Escolha um dos seguintes tipos de output: csv ou sqlite.
            ''')
            help()

        else:
            print('Iniciando processamento em {}'.format(datetime.datetime.now()))

            cnpj_full(input_list, tipo_output, output_path)

            print('Processamento concluido em {}'.format(datetime.datetime.now()))

if __name__ == "__main__":
    main()