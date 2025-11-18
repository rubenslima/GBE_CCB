import streamlit as st
import pandas as pd
from io import BytesIO
import os
import uuid
import urllib.parse
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ================== Config da página ==================
st.set_page_config(page_title="Extrair Matrícula de Excel", page_icon="📄", layout="centered")
st.title(" Extração de coluna de Matrícula a partir de planilha")
st.caption("Arraste e solte sua planilha Excel/CSV. O app localizará a coluna de matrícula e permitirá exportar com as colunas 'Origem' e 'Responsavel'.")

# Reserva espaço no topo para os botões de exportação
top_actions = st.container()

# ================== Conexão com o banco ==================
load_dotenv()

SERVER   = (os.getenv("SERVER") or "").strip()
USER     = (os.getenv("USER") or "").strip()
PASSWORD = (os.getenv("PASSWORD") or "").strip()
DATABASE = (os.getenv("DATABASE") or "").strip()
AMBIENTE = (os.getenv("AMBIENTE") or "DEV").upper().strip()
ODBC_DRIVER  = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server").strip()

if not all([SERVER, USER, PASSWORD, DATABASE]):
    st.error("Defina as variáveis: SERVER, USER, PASSWORD, DATABASE (opcional: ODBC_DRIVER, AMBIENTE).")
    st.stop()

# ================== Sidebar: informações de ambiente/servidor ==================
ENV_COLORS = {"DEV": "#2563eb", "HOMOLOGAÇÃO": "#059669", "PROD": "#dc2626"}
env_color = ENV_COLORS.get(AMBIENTE, "#2563eb")

with st.sidebar:
    st.header("Conexão")
    st.markdown(
        f"""
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;">
          <span style="font-size:16px;">Conectado a</span>
          <span style="background:{env_color};color:white;padding:2px 10px;border-radius:999px;font-weight:600;">
            {AMBIENTE}
          </span>
        </div>
        """,
        unsafe_allow_html=True
    )
    st.caption(f"**Server**: {SERVER}")
    st.caption(f"**Database**: {DATABASE}")
    st.markdown("---")
    st.header("Resumo")
    st.caption("Carregue um arquivo para ver os números.")

# ================== String de conexão ==================
server_encoded = urllib.parse.quote(SERVER, safe="")
user_encoded   = urllib.parse.quote(USER,  safe="")
pass_encoded   = urllib.parse.quote(PASSWORD, safe="")
driver_qs      = ODBC_DRIVER.replace(" ", "+")

extra_params = []
if "ODBC Driver 18" in ODBC_DRIVER:
    extra_params += ["Encrypt=yes", "TrustServerCertificate=yes"]
else:
    extra_params += ["TrustServerCertificate=yes"]

qs = "&".join(["driver=" + driver_qs] + extra_params)
conn_url = f"mssql+pyodbc://{user_encoded}:{pass_encoded}@{server_encoded}/{DATABASE}?{qs}"

@st.cache_resource(show_spinner=False)
def get_engine():
    return create_engine(conn_url, future=True)

engine = get_engine()

# ================== Upload do arquivo ==================
uploaded_file_placeholder = st.empty()
uploaded_file = uploaded_file_placeholder.file_uploader(
    "Arraste e solte um arquivo aqui ou clique para selecionar",
    type=["xlsx", "xls", "csv"],
    accept_multiple_files=False,
    help="Formatos suportados: .xlsx, .xls, .csv",
    key="uploader_principal"
)

# ================== Funções utilitárias ==================
@st.cache_data(show_spinner=False)
def load_dataframe(file_bytes: bytes, file_name: str, sheet: str | None) -> pd.DataFrame:
    name = file_name.lower()
    bio = BytesIO(file_bytes)

    if name.endswith((".xlsx", ".xls")):
        if sheet is None:
            raise ValueError("Para arquivos Excel, a aba (sheet) deve ser informada.")
        xls = pd.ExcelFile(bio)
        df = pd.read_excel(xls, sheet_name=sheet, dtype=object)
        return df

    elif name.endswith(".csv"):
        try:
            df = pd.read_csv(bio, dtype=object)
        except Exception:
            df = pd.read_csv(BytesIO(file_bytes), sep=";", dtype=object)
        return df

    else:
        raise ValueError("Formato de arquivo não suportado.")

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = (
        pd.Series(df.columns)
        .astype(str)
        .str.strip()
        .str.lower()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    df2 = df.copy()
    df2.columns = cols
    return df2

def extract_matricula(df: pd.DataFrame) -> pd.DataFrame:
    df_norm = normalize_columns(df)

    target_col = None
    if "matricula" in df_norm.columns:
        target_col = "matricula"
    if target_col is None:
        candidates = [c for c in df_norm.columns if c.replace(" ", "") == "matricula"]
        if candidates:
            target_col = candidates[0]
    if target_col is None:
        raise KeyError("Não foi encontrada a coluna 'Matricula' ou 'Matrícula' na planilha.")

    serie = df_norm[target_col].astype(str).str.strip()
    serie = serie.replace({"nan": None, "None": None, "": None}).dropna()

    return pd.DataFrame({"Matricula": serie})

def find_original_matricula_column(df: pd.DataFrame) -> str:
    for col in df.columns:
        norm = (
            pd.Series([col])
            .astype(str)
            .str.strip()
            .str.lower()
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        ).iloc[0]
        if norm == "matricula" or norm.replace(" ", "") == "matricula":
            return col
    raise KeyError("Não foi possível identificar a coluna original de matrícula no arquivo.")

# ================== SQL (ajustada para 2012 e para embed) ==================
# Sem ORDER BY e sem ; no final — para embutir como subquery sem erros
SQL_PLANOS = """
SELECT
    b.NUM_MATRICULA AS Matricula,
    b.COD_ENTID,
    STUFF((
        SELECT '/' + s.RESP_LIBERACAO
        FROM (
            SELECT DISTINCT BO2.RESP_LIBERACAO
            FROM CS_BLOQUEIO AS BO2
            WHERE BO2.CD_INSCRICAO = b.NUM_INSCRICAO
              AND BO2.DT_LIBERACAO IS NULL
        ) AS s
        ORDER BY s.RESP_LIBERACAO
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS Responsavel
FROM (
    SELECT
        FU.NUM_MATRICULA,
        BO.COD_ENTID,
        MAX(FU.NUM_INSCRICAO) AS NUM_INSCRICAO
    FROM CS_PLANOS_VINC AS PV
    INNER JOIN CS_FUNCIONARIO AS FU
        ON FU.CD_FUNDACAO   = PV.CD_FUNDACAO
       AND FU.NUM_INSCRICAO = PV.NUM_INSCRICAO
    INNER JOIN CS_BLOQUEIO AS BO
        ON BO.CD_INSCRICAO = FU.NUM_INSCRICAO
    WHERE BO.DT_LIBERACAO IS NULL
    GROUP BY FU.NUM_MATRICULA, BO.COD_ENTID
) AS b
"""

SQL_REQUER = """
SELECT
       PB.CD_EMPRESA
     , PB.CD_PLANO
     , PB.CD_ESPECIE
     , EB.CD_GRUPO_ESPECIE
     , GE.DS_GRUPO_ESPECIE
     , SUBSTRING(ds_dr, 4, 3)  AS SIGLA_ENTID
     , F.NUM_MATRICULA         AS Matricula
     , DS_NOME
     , PB.NUM_PROCESSO
     , PB.ANO_PROCESSO
     , RB.SEQ_RECEBEDOR
     , DT_CONCESSAO
     , HP.DT_INICIO_FUND
     , PB.DT_TERMINO
     , W.DS_SINDICATO
     , W.DT_INCLUSAO
FROM dbo.WEB_GPA_INSCRICAO_SALDADO_LIMINAR AS W
INNER JOIN dbo.CS_FUNCIONARIO         AS F   ON W.CD_MATRICULA   = F.NUM_MATRICULA
INNER JOIN dbo.CS_DADOS_PESSOAIS      AS DP  ON F.COD_ENTID      = DP.COD_ENTID
INNER JOIN dbo.TB_LOCALIDADE          AS L   ON L.CD_LOCALIDADE  = F.CD_LOCALIDADE
INNER JOIN dbo.GB_PROCESSOS_BENEFICIO AS PB  ON F.NUM_INSCRICAO  = PB.NUM_INSCRICAO
INNER JOIN dbo.GB_ESPECIE_BENEFICIO   AS EB  ON PB.CD_ESPECIE    = EB.CD_ESPECIE
INNER JOIN dbo.GB_HIST_PROCESSOS      AS HP  ON HP.NUM_PROCESSO  = PB.NUM_PROCESSO
                                            AND HP.ANO_PROCESSO  = PB.ANO_PROCESSO
                                            AND HP.CD_ESPECIE    = PB.CD_ESPECIE
                                            AND HP.CD_PLANO      = PB.CD_PLANO
                                            AND HP.CD_EMPRESA    = PB.CD_EMPRESA
                                            AND HP.VERSAO        = PB.VERSAO
INNER JOIN dbo.GB_SITUACAO            AS S   ON PB.CD_SITUACAO   = S.CD_SITUACAO
INNER JOIN dbo.GB_RECEBEDOR_BENEFICIO AS RB  ON PB.CD_FUNDACAO   = RB.CD_FUNDACAO
                                            AND PB.CD_EMPRESA    = RB.CD_EMPRESA
                                            AND PB.NUM_INSCRICAO = RB.NUM_INSCRICAO
INNER JOIN dbo.GB_GRUPO_ESPECIE       AS GE  ON EB.CD_GRUPO_ESPECIE = GE.CD_GRUPO_ESPECIE
WHERE
      HP.DT_INICIO_FUND >= '20080316'
  AND EB.CD_GRUPO_ESPECIE <> 4
  AND F.NUM_MATRICULA NOT IN ('080024874','080816452','080815685')
  AND NOT EXISTS (
        SELECT 1
        FROM dbo.GB_PROCESSOS_BENEFICIO AS BANT
        INNER JOIN dbo.GB_HIST_PROCESSOS AS PANT
                ON  BANT.NUM_PROCESSO = PANT.NUM_PROCESSO
                AND BANT.ANO_PROCESSO = PANT.ANO_PROCESSO
                AND BANT.CD_ESPECIE   = PANT.CD_ESPECIE
                AND BANT.CD_PLANO     = PANT.CD_PLANO
                AND BANT.CD_EMPRESA   = PANT.CD_EMPRESA
                AND BANT.VERSAO       = PANT.VERSAO
        WHERE BANT.NUM_INSCRICAO   = PB.NUM_INSCRICAO
          AND BANT.DT_TERMINO      = DATEADD(DAY, -1, HP.DT_INICIO_FUND)
          AND PANT.DT_INICIO_FUND <= W.DT_INCLUSAO
  )
GROUP BY
       PB.CD_EMPRESA,
       PB.CD_PLANO,
       PB.CD_ESPECIE,
       EB.CD_GRUPO_ESPECIE,
       GE.DS_GRUPO_ESPECIE,
       SUBSTRING(ds_dr, 4, 3),
       F.NUM_MATRICULA,
       F.NUM_INSCRICAO,
       DS_NOME,
       PB.NUM_PROCESSO,
       PB.ANO_PROCESSO,
       RB.SEQ_RECEBEDOR,
       DT_CONCESSAO,
       HP.DT_INICIO_FUND,
       PB.DT_TERMINO,
       W.DS_SINDICATO,
       W.DT_INCLUSAO
"""

def fetch_presence(engine, df_mats: pd.DataFrame):
    """
    Insere matrículas em temp table única e verifica presença nas duas consultas.
    Retorna também a coluna 'Responsavel' (proveniente de SQL_PLANOS).
    """
    mats = df_mats["Matricula"].astype(str).tolist()
    temp_name = f"#Matriculas_{uuid.uuid4().hex}"

    with engine.begin() as conn:
        # Cria e popula #temp
        conn.exec_driver_sql(f"CREATE TABLE {temp_name} (Matricula NVARCHAR(64) NOT NULL)")
        cursor = conn.connection.cursor()
        try:
            cursor.fast_executemany = True
        except Exception:
            pass
        cursor.executemany(f"INSERT INTO {temp_name} (Matricula) VALUES (?)", [(m,) for m in mats])

        # Consulta 1 (Planos) - traz também Responsavel
        q1_sql = f"""
        SELECT DISTINCT P.Matricula, P.Responsavel
        FROM (
            {SQL_PLANOS}
        ) AS P
        INNER JOIN {temp_name} AS M
                ON M.Matricula = P.Matricula
        ORDER BY P.Matricula;
        """
        df1 = pd.read_sql_query(q1_sql, conn)

        # Consulta 2 (Requer) - apenas Matricula
        q2_sql = f"""
        SELECT DISTINCT R.Matricula
        FROM (
            {SQL_REQUER}
        ) AS R
        INNER JOIN {temp_name} AS M
                ON M.Matricula = R.Matricula
        """
        df2 = pd.read_sql_query(q2_sql, conn)

        # Limpeza
        conn.exec_driver_sql(f"DROP TABLE {temp_name}")

    return df1, df2

def build_result(df_mats: pd.DataFrame, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Monta o resultado consolidado com Origem e Responsavel.
    """
    df = df_mats.copy()
    df["Matricula"] = df["Matricula"].astype(str)

    s1 = set(df1["Matricula"].astype(str)) if "Matricula" in df1.columns else set()
    s2 = set(df2["Matricula"].astype(str)) if "Matricula" in df2.columns else set()

    df["_in_planos"] = df["Matricula"].isin(s1)
    df["_in_requer"] = df["Matricula"].isin(s2)

    def label(row):
        if row["_in_planos"] and row["_in_requer"]:
            return "Ambas"
        if row["_in_planos"]:
            return "Bloqueios"
        if row["_in_requer"]:
            return "Liminar"
        return None

    df["Origem"] = df.apply(label, axis=1)

    # Responsável (somente quando há no df1/Planos)
    if "Responsavel" in df1.columns:
        responsaveis = df1.drop_duplicates(subset=["Matricula"]).set_index("Matricula")["Responsavel"]
        df["Responsavel"] = df["Matricula"].map(responsaveis)
    else:
        df["Responsavel"] = None

    return df[["Matricula", "Origem", "Responsavel"]]

# ================== Fluxo principal ==================
if uploaded_file is None:
    st.info("Carregue uma planilha para iniciar.")
else:
    try:
        name = uploaded_file.name
        data = uploaded_file.getbuffer().tobytes()

        sheet_selected = None
        if name.lower().endswith((".xlsx", ".xls")):
            try:
                xls_preview = pd.ExcelFile(BytesIO(data))
                sheet_selected = st.selectbox("Selecione a aba (sheet)", xls_preview.sheet_names, index=0)
            except Exception as e:
                st.error(f"Erro ao ler abas do Excel: {e}")
                st.stop()

        df_raw = load_dataframe(data, name, sheet_selected)

        # Prévia da planilha importada
        st.subheader("Prévia do arquivo carregado")
        st.dataframe(df_raw.head(20), use_container_width=True)

        # Processamento oculto
        df_matricula = extract_matricula(df_raw)
        if df_matricula.empty:
            st.warning("Nenhuma matrícula válida encontrada após a limpeza da coluna.")
            st.stop()

        with st.spinner("Consultando banco de dados…"):
            df1, df2 = fetch_presence(engine, df_matricula)

        result = build_result(df_matricula, df1, df2)

        # Mapeamento de Origem e Responsavel por linha da planilha importada
        original_col = find_original_matricula_column(df_raw)
        mapa_origem = dict(zip(result["Matricula"].astype(str).str.strip(), result["Origem"]))
        mapa_resp   = dict(zip(result["Matricula"].astype(str).str.strip(), result["Responsavel"]))

        origem_series = (
            df_raw[original_col]
            .astype(str).str.strip()
            .map(mapa_origem)        # sem correspondência -> NaN
        )
        resp_series = (
            df_raw[original_col]
            .astype(str).str.strip()
            .map(mapa_resp)          # sem correspondência -> NaN
        )

        # Função para inserir colunas no lugar correto
        def insert_cols(df_base: pd.DataFrame, origem_series: pd.Series, resp_series: pd.Series) -> pd.DataFrame:
            df_out = df_base.copy()
            origem_alinhada = origem_series.reindex(df_out.index)
            resp_alinhada   = resp_series.reindex(df_out.index)
            cols = list(df_out.columns)
            if "Situação do Participante" in cols:
                idx = cols.index("Situação do Participante")
                df_out.insert(idx + 1, "Origem", origem_alinhada)
                df_out.insert(idx + 2, "Responsavel", resp_alinhada)
            else:
                df_out["Origem"] = origem_alinhada
                df_out["Responsavel"] = resp_alinhada
            return df_out.reset_index(drop=True)

        xlsx_sheet = (sheet_selected or "Planilha")[:31]

        # Geração dos arquivos antes de desenhar os botões
        # Exportar completo
        df_export_completo = insert_cols(df_raw, origem_series, resp_series)
        buf_full = BytesIO()
        with pd.ExcelWriter(buf_full, engine="openpyxl") as writer:
            df_export_completo.to_excel(writer, index=False, sheet_name=xlsx_sheet)
        buf_full.seek(0)

        # Exportar correspondências: apenas linhas com Origem preenchida
        df_export_matches = df_export_completo.loc[df_export_completo["Origem"].notna()].copy()
        buf_matches = None
        if not df_export_matches.empty:
            buf_matches = BytesIO()
            with pd.ExcelWriter(buf_matches, engine="openpyxl") as writer:
                df_export_matches.to_excel(writer, index=False, sheet_name=xlsx_sheet)
            buf_matches.seek(0)

        # ======= TOPO: Botões lado a lado =======
        with top_actions:
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="Exportar toda a planilha",
                    data=buf_full.getvalue(),
                    file_name="planilha_completa_com_origem_responsavel.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="btn_export_full_top",
                )
            with col2:
                if buf_matches is None:
                    st.button("Exportar correspondências (sem dados)", disabled=True, key="btn_export_matches_top_disabled")
                else:
                    st.download_button(
                        label="Exportar correspondências",
                        data=buf_matches.getvalue(),
                        file_name="planilha_somente_correspondencias.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="btn_export_matches_top",
                    )

        # ================== Sidebar: resumo dinâmico ==================
        linhas_importadas = len(df_raw)
        matriculas_analisadas = len(df_matricula)
        correspondencias_encontradas = origem_series.notna().sum()

        with st.sidebar:
            st.markdown("### Resumo")
            st.metric("Linhas importadas", f"{linhas_importadas:,}".replace(",", "."))
            st.metric("Matrículas analisadas", f"{matriculas_analisadas:,}".replace(",", "."))
            st.metric("Correspondências encontradas", f"{correspondencias_encontradas:,}".replace(",", "."))

        st.success("Processo concluído.")

    except Exception as e:
        st.error(f"Ocorreu um erro ao processar o arquivo: {e}")