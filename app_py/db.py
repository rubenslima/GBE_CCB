"""
Projeto         : conecta DB
Autor           : Rubens Lima
Criado em       : 2026-04-16
Última alteração: 2026-04-26
Versão          : 1.0.0.a
Descrição       : conexão ao banco de dados
Tipo            : ETL
Módulo          : utils
ID              : GBE.DBA.20260410.001.APP
"""

import os
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv


def carregar_cfg():
    load_dotenv()

    cfg = {
        "AMBIENTE": (os.getenv("AMBIENTE") or "").strip(),
        "SERVER": (os.getenv("SERVER") or "").strip(),
        "USER": (os.getenv("USER") or "").strip(),
        "PASSWORD": (os.getenv("PASSWORD") or "").strip(),
        "DATABASE": (os.getenv("DATABASE") or "").strip(),
        "ODBC_DRIVER": (
            os.getenv("ODBC_DRIVER") or "ODBC Driver 17 for SQL Server"
        ).strip(),
        "ODBC_EXTRA": (os.getenv("ODBC_EXTRA") or "").strip(),
    }

    faltando = [k for k in ("SERVER", "USER", "PASSWORD", "DATABASE") if not cfg[k]]
    if faltando:
        raise RuntimeError(f"Variáveis ausentes no .env: {', '.join(faltando)}")

    return cfg


def build_connection_url(cfg) -> str:
    params = (
        f"DRIVER={{{cfg['ODBC_DRIVER']}}};"
        f"SERVER={cfg['SERVER']};"
        f"DATABASE={cfg['DATABASE']};"
        f"UID={cfg['USER']};"
        f"PWD={cfg['PASSWORD']}"
    )

    if cfg["ODBC_EXTRA"]:
        extra = cfg["ODBC_EXTRA"]
        if not extra.endswith(";"):
            extra += ";"
        params += ";" + extra

    return f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(params)}"


def get_engine() -> Engine:
    cfg = carregar_cfg()
    url = build_connection_url(cfg)

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=5,
    )


# =========================
# INFO DE AMBIENTE
# =========================

RESET = "\033[0m"
AZUL = "\033[94m"
VERMELHO = "\033[91m"
VERDE = "\033[92m"


def obter_info_ambiente(cfg: dict) -> dict:
    ambiente = (cfg.get("AMBIENTE") or "").upper()

    if ambiente == "HOMOLOGAÇÃO":
        cor = AZUL
    elif ambiente == "PRODUÇÃO":
        cor = VERMELHO
    elif ambiente == "DESENVOLVIMENTO":
        cor = VERDE
    else:
        cor = RESET

    return {
        "ambiente": cfg.get("AMBIENTE") or "(não informado)",
        "server": cfg.get("SERVER"),
        "database": cfg.get("DATABASE"),
        "cor": cor,
    }


def exibir_info_ambiente_console():
    cfg = carregar_cfg()
    info = obter_info_ambiente(cfg)

    print(info["cor"] + "=" * 60)
    print(f"Ambiente: {info['ambiente']}")
    print(f"Origem:   {info['server']} / {info['database']}")
    print("=" * 60 + RESET)
