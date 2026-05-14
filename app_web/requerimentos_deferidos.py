"""
Projeto         : requerimentos deferidos
Solicitante     : Marilene
Autor           : Rubens Lima
Criado em       : 2025-10-10
Última alteração: 2025-10-10
Versão          : 1.0.1.c
Descrição       : Geração de planilha para conferência de requerimentos deferidos
Tipo            : ETL
Módulo          : Conferência CCB
Tags            : requerimentos deferidos
ID              : GBE.CCB.20251010.002.WEB
"""

import re
import streamlit as st
import pandas as pd
import os
import io
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# Carrega as variáveis do arquivo .env
load_dotenv()

# ------------------------------
# Funções de Configuração e Dados
# ------------------------------


def get_engine():
    """Busca as credenciais do arquivo .env via os.getenv"""
    try:
        # Extração das variáveis
        server = os.getenv("SERVER")
        user = os.getenv("USER")
        password = os.getenv("PASSWORD")
        database = os.getenv("DATABASE")
        driver = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
        extra = os.getenv("ODBC_EXTRA", "")

        if not all([server, user, password, database]):
            st.error("Erro: Variáveis de ambiente incompletas no arquivo .env")
            return None

        params = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password}"
        )
        if extra:
            params += ";" + (extra if extra.endswith(";") else extra + ";")

        url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(params)}"

        return create_engine(
            url,
            pool_pre_ping=True,
            fast_executemany=True,  # Otimiza inserções/leituras se o driver suportar
        )
    except Exception as e:
        st.error(f"Erro ao configurar a engine: {e}")
        return None


# ... (restante das funções sanitize_columns e to_excel permanecem iguais)


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"[\\/*?:\[\]]", "", str(col)).strip() for col in df.columns]
    return df


def to_excel(df_dados, df_sem_num, df_estat):
    """Gera o arquivo Excel em memória para download"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # Aba Dados
        df_dados.to_excel(writer, sheet_name="Dados", index=False)

        # Aba Sem Número Benefício
        if not df_sem_num.empty:
            df_sem_num.to_excel(writer, sheet_name="Sem_Numero_Beneficio", index=False)

        # Aba Estatística
        if not df_estat.empty:
            df_estat.to_excel(writer, sheet_name="Estatistica", index=False)

    return output.getvalue()


# ------------------------------
# Interface Streamlit
# ------------------------------

st.title("📊 Gestão de Requerimentos Devolvidos")
st.markdown("Filtre o período desejado para processar os dados do banco.")

# Sidebar para Filtros
with st.sidebar:
    st.header("Filtros de Data")
    # Streamlit já retorna objeto datetime.date
    data_inicio_ref = st.date_input("Data Inicial", datetime.now())
    data_fim_ref = st.date_input("Data Final", datetime.now())

    # Formatação para a Query (mm-dd-aaaa conforme original)
    data_inicio = data_inicio_ref.strftime("%m-%d-%Y")
    data_fim = data_fim_ref.strftime("%m-%d-%Y")

    btn_processar = st.button("Executar Consulta", type="primary")

if btn_processar:
    engine = get_engine()

    if engine:
        with st.spinner("Conectando e executando queries..."):
            try:
                # Queries (Mantendo sua lógica original de filtros)
                query_principal = f"""
                set nocount on;
                IF OBJECT_ID('tempdb..#Identificador') IS NOT NULL DROP TABLE #Identificador;
                SELECT rtrim(fun.NUM_MATRICULA)+'@'+rtrim(req.NU_BENEFICIO_INSS) identificador
                into #Identificador
                FROM dbo.WEB_GBE_REQUERIMENTO AS req
                INNER JOIN dbo.CS_FUNCIONARIO AS fun ON req.CD_FUNDACAO = fun.CD_FUNDACAO AND req.CD_INSCRICAO = fun.NUM_INSCRICAO
                INNER JOIN dbo.CS_PLANOS_VINC AS plv ON fun.CD_FUNDACAO = plv.CD_FUNDACAO AND fun.NUM_INSCRICAO = plv.NUM_INSCRICAO
                INNER JOIN web.HistoricoRequerimento AS his ON req.SQ_REQUERIMENTO = his.SequencialRequerimento
                INNER JOIN dbo.TB_PLANOS AS pln ON req.CD_PLANO = pln.CD_PLANO AND req.CD_FUNDACAO = pln.CD_FUNDACAO
                INNER JOIN dbo.WEB_GPA_SIT_INSCRICOES AS sit ON req.NS_SIT_REQUERIMENTO = sit.NS_SIT_INSCRICAO
                INNER JOIN web.TipoRequerimentoBeneficio AS tip ON req.TP_PROCESSO = tip.Id
                WHERE req.DT_REQUERIMENTO >= '{data_inicio}' AND req.DT_REQUERIMENTO <= '{data_fim}'
                AND sit.NS_SIT_INSCRICAO = 6 AND tip.Id = '1' AND len(req.NU_BENEFICIO_INSS)>1
                group by fun.NUM_MATRICULA, req.NU_BENEFICIO_INSS;

                SELECT
                    fun.NUM_MATRICULA AS Matricula, ent.NOME_ENTID AS NomeParticipante,
                    FORMAT(req.DT_REQUERIMENTO, 'dd/MM/yyyy HH:mm') AS DataRequerimento,
                    FORMAT(req.DT_DEFERIMENTO, 'dd/MM/yyyy') AS DataDeferimento,
                    req.NU_BENEFICIO_INSS AS NumeroBeneficioINSS, esp.Especie, tip.Descricao AS Tipo,
                    sit.DS_SIT_INSCRICAO AS [Status], funent.NOME_ENTID AS Responsavel,
                    pln.DS_PLANO AS Plano, format(dad.DT_OBITO, 'dd/MM/yyyy') AS DataObito
                into #passo01
                FROM dbo.WEB_GBE_REQUERIMENTO AS req
                LEFT OUTER JOIN dbo.vwEspecieBeneficio AS esp ON req.CD_ESPECIE = esp.Codigo
                INNER JOIN dbo.CS_FUNCIONARIO AS fun ON req.CD_FUNDACAO = fun.CD_FUNDACAO AND req.CD_INSCRICAO = fun.NUM_INSCRICAO
                INNER JOIN dbo.EE_ENTIDADE AS ent ON fun.COD_ENTID = ent.COD_ENTID
                INNER JOIN dbo.CS_DADOS_PESSOAIS AS dad ON ent.COD_ENTID = dad.COD_ENTID
                INNER JOIN dbo.CS_PLANOS_VINC AS plv ON fun.CD_FUNDACAO = plv.CD_FUNDACAO AND fun.NUM_INSCRICAO = plv.NUM_INSCRICAO
                INNER JOIN web.HistoricoRequerimento AS his ON req.SQ_REQUERIMENTO = his.SequencialRequerimento
                LEFT OUTER JOIN dbo.CS_FUNCIONARIO AS hisfun ON his.MatriculaAtendimento = hisfun.NUM_MATRICULA
                LEFT OUTER JOIN dbo.EE_ENTIDADE AS funent ON hisfun.COD_ENTID = funent.COD_ENTID
                INNER JOIN dbo.TB_PLANOS AS pln ON req.CD_PLANO = pln.CD_PLANO AND req.CD_FUNDACAO = pln.CD_FUNDACAO
                INNER JOIN dbo.WEB_GPA_SIT_INSCRICOES AS sit ON req.NS_SIT_REQUERIMENTO = sit.NS_SIT_INSCRICAO
                INNER JOIN web.TipoRequerimentoBeneficio AS tip ON req.TP_PROCESSO = tip.Id
                WHERE his.Data = (SELECT MAX(sub.Data) FROM web.HistoricoRequerimento AS sub WHERE sub.SequencialRequerimento = req.SQ_REQUERIMENTO)
                AND tip.Id = '1' AND rtrim(fun.NUM_MATRICULA)+'@'+rtrim(req.NU_BENEFICIO_INSS) in(select identificador from #Identificador);

                delete #passo01 where rtrim(MATRICULA)+'@'+rtrim(NumeroBeneficioINSS) in (select rtrim(MATRICULA)+'@'+rtrim(NumeroBeneficioINSS) from #passo01 where [status] ='INDEFERIDO');
                select * from #passo01 ORDER BY NomeParticipante, NumeroBeneficioINSS, DataRequerimento;
                """

                # Execução
                with engine.connect() as conn:
                    df_principal = pd.read_sql(text(query_principal), conn)
                    # (Aqui você pode adicionar a execução da query_sem_numero_beneficio se desejar)
                    df_sem_beneficio = pd.DataFrame()

                # Processamento
                df_principal = sanitize_columns(df_principal)

                # Estatísticas
                df_estat = pd.DataFrame()
                if not df_principal.empty and "Status" in df_principal.columns:
                    df_estat = df_principal["Status"].value_counts().reset_index()
                    df_estat.columns = ["Status", "Total"]

                # Exibição na Tela
                st.success(
                    f"Consulta finalizada! {len(df_principal)} registros encontrados."
                )

                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Resumo por Status")
                    st.table(df_estat)

                st.subheader("Visualização dos Dados (Top 5)")
                st.dataframe(df_principal.head(5), use_container_width=True)

                # Botão de Download
                excel_data = to_excel(df_principal, df_sem_beneficio, df_estat)
                st.download_button(
                    label="📥 Baixar Relatório Excel",
                    data=excel_data,
                    file_name=f"Relatorio_Requerimentos_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            except Exception as e:
                st.error(f"Erro durante a execução: {e}")

# ------------------------------
# Configuração de Segurança (Secrets)
# ------------------------------
# No Streamlit Cloud ou Local, crie um arquivo .streamlit/secrets.toml com:
# [database]
# SERVER = "seu_servidor"
# USER = "seu_user"
# PASSWORD = "sua_password"
# DATABASE = "seu_db"
# ODBC_DRIVER = "ODBC Driver 17 for SQL Server"
