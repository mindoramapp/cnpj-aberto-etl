#!/usr/bin/env python3
"""ETL dos dados abertos do CNPJ (Receita Federal) para um SQLite enxuto, por município.

A Receita publica todo mês ~7 GB de CSVs compactados com todas as empresas do Brasil.
Este script baixa os arquivos (com retomada), lê os .zip em fluxo, sem descompactar nem
carregar tudo na memória, e grava só os estabelecimentos ATIVOS, com telefone ou e-mail,
dos municípios escolhidos. Do MEI, o CPF que vem no fim da razão social é removido.

Uso:
    python3 cnpj_etl.py listar
    python3 cnpj_etl.py baixar [AAAA-MM]
    python3 cnpj_etl.py processar AAAA-MM "Salvador" BA ["Feira de Santana" BA ...]
    python3 cnpj_etl.py resumo [--top 15]
    python3 cnpj_etl.py exportar --cnae 5611 --saida restaurantes.csv

A pasta de dados é ./dados por padrão; mude com --pasta ou a variável CNPJ_DADOS.
"""
from __future__ import annotations

import argparse
import base64
import csv
import os
import re
import sqlite3
import sys
import time
import unicodedata
import urllib.request
import zipfile
from collections.abc import Iterable, Iterator
from pathlib import Path

# Pasta pública da Receita (Nextcloud). O "token" é o identificador público do compartilhamento,
# o mesmo do link aberto no navegador: o acesso é anônimo e somente leitura.
TOKEN_PUBLICO = "gn672Ad4CF8N6TK"
WEBDAV = "https://arquivos.receitafederal.gov.br/public.php/webdav/Dados/Cadastros/CNPJ/"
ARQUIVOS = (
    [f"Estabelecimentos{i}.zip" for i in range(10)]
    + [f"Empresas{i}.zip" for i in range(10)]
    + ["Municipios.zip"]
)
CABECALHOS = {
    "Authorization": "Basic " + base64.b64encode(f"{TOKEN_PUBLICO}:".encode()).decode(),
    "User-Agent": "cnpj-aberto-etl/1.0",
}
PROPFIND = b'<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop><d:getcontentlength/></d:prop></d:propfind>'

SITUACAO_ATIVA = "02"
NATUREZA_EMPRESARIO_INDIVIDUAL = "2135"
CPF_NO_FIM = re.compile(r"\s*\d{11}\s*$")

ESQUEMA = """
CREATE TABLE estab (
    cnpj TEXT PRIMARY KEY, basico TEXT, fantasia TEXT, razao TEXT, natureza TEXT, mei INTEGER,
    cnae TEXT, cnaes_sec TEXT, endereco TEXT, bairro TEXT, cep TEXT, municipio_nome TEXT, uf TEXT,
    tel1 TEXT, tel2 TEXT, email TEXT, inicio TEXT
);
CREATE TABLE info (chave TEXT PRIMARY KEY, valor TEXT);
"""


# ---------------------------------------------------------------- transformação (funções puras)

def normalizar(texto: str | None) -> str:
    """Maiúsculas, sem acento e sem hífen: 'Feira-de-Santana' -> 'FEIRA DE SANTANA'."""
    t = unicodedata.normalize("NFKD", (texto or "").upper())
    t = "".join(c for c in t if not unicodedata.combining(c)).replace("-", " ")
    return " ".join(t.split())


def chave_municipio(uf: str, codigo: str) -> bytes:
    """Trecho fixo da linha do CSV de estabelecimentos ("UF";"código";), usado como pré-filtro em bytes."""
    return f'"{uf.upper()}";"{codigo}";'.encode()


def transformar_estabelecimento(campos: list[str], municipio: str) -> tuple | None:
    """Converte uma linha de Estabelecimentos*.csv em registro, ou None se não interessa.

    Regras: só situação cadastral ativa e só quem tem telefone ou e-mail válido.
    """
    if len(campos) < 28 or campos[5] != SITUACAO_ATIVA:
        return None
    tel1 = campos[21] + campos[22] if campos[22].strip() else ""
    tel2 = campos[23] + campos[24] if campos[24].strip() else ""
    email = campos[27].strip().lower()
    email = email if "@" in email else ""
    if not (tel1 or tel2 or email):
        return None
    endereco = " ".join(x.strip() for x in (campos[13], campos[14]) if x.strip())
    if campos[15].strip():
        endereco += f", {campos[15].strip()}"
    return (
        campos[0] + campos[1] + campos[2],  # CNPJ completo = básico + ordem + DV
        campos[0], campos[4].strip(), "", "", 0,
        campos[11], campos[12], endereco, campos[17].strip(), campos[18],
        municipio, campos[19], tel1, tel2, email, campos[10],
    )


def limpar_razao(razao: str, natureza: str) -> tuple[str, int]:
    """Remove o CPF do fim da razão social e diz se é MEI (empresário individual com CPF no nome)."""
    mei = int(natureza == NATUREZA_EMPRESARIO_INDIVIDUAL and bool(CPF_NO_FIM.search(razao)))
    return CPF_NO_FIM.sub("", razao).strip(), mei


def ler_csv(linha: bytes) -> list[str]:
    return next(csv.reader([linha.decode("latin-1")], delimiter=";"))


# ---------------------------------------------------------------- extração

def _pedido(url: str, metodo: str = "GET", extra: dict | None = None, corpo: bytes | None = None):
    return urllib.request.Request(url, data=corpo, method=metodo, headers={**CABECALHOS, **(extra or {})})


def _propfind(url: str) -> str:
    with urllib.request.urlopen(_pedido(url, "PROPFIND", {"Depth": "1"}, PROPFIND), timeout=60) as r:
        return r.read().decode()


def listar_meses() -> list[str]:
    return sorted(set(re.findall(r"CNPJ/(\d{4}-\d{2})/", _propfind(WEBDAV))))


def tamanhos(mes: str) -> dict[str, int]:
    saida = {}
    for bloco in _propfind(WEBDAV + mes + "/").split("<d:response>")[1:]:
        nome = re.search(r"<d:href>[^<]*/([^/<]+)</d:href>", bloco)
        tam = re.search(r"<d:getcontentlength>(\d+)", bloco)
        if nome and tam:
            saida[nome.group(1)] = int(tam.group(1))
    return saida


def baixar(pasta: Path, mes: str | None = None) -> str:
    """Baixa os 21 arquivos do mês com retomada (HTTP Range) e até 5 tentativas por arquivo."""
    mes = mes or listar_meses()[-1]
    destino = pasta / mes
    destino.mkdir(parents=True, exist_ok=True)
    tam = tamanhos(mes)
    print(f"Mês {mes}: {sum(tam.get(a, 0) for a in ARQUIVOS) / 1e9:.1f} GB em {destino}", flush=True)
    for nome in ARQUIVOS:
        arq, esperado = destino / nome, tam.get(nome)
        if esperado and arq.exists() and arq.stat().st_size == esperado:
            continue
        for _ in range(5):
            feito = arq.stat().st_size if arq.exists() else 0
            try:
                extra = {"Range": f"bytes={feito}-"} if feito else None
                with urllib.request.urlopen(_pedido(WEBDAV + f"{mes}/{nome}", extra=extra), timeout=120) as r, \
                        arq.open("ab" if feito else "wb") as f:
                    if feito and r.status != 206:  # o servidor ignorou o Range: recomeça
                        f.seek(0)
                        f.truncate()
                    while bloco := r.read(1 << 20):
                        f.write(bloco)
                break
            except OSError as e:
                print(f"  {nome}: {e}; nova tentativa em 20 s", flush=True)
                time.sleep(20)
        print(f"  {nome}: {arq.stat().st_size / 1e6:.0f} MB", flush=True)
    return mes


def linhas_zip(arq: Path) -> Iterator[bytes]:
    """Lê o CSV de dentro do .zip linha a linha, em fluxo."""
    with zipfile.ZipFile(arq) as z, z.open(z.namelist()[0]) as f:
        yield from f


def municipios(pasta_mes: Path) -> dict[str, str]:
    saida = {}
    for linha in linhas_zip(pasta_mes / "Municipios.zip"):
        codigo, nome = ler_csv(linha)[:2]
        saida[normalizar(nome)] = codigo
    return saida


# ---------------------------------------------------------------- carga

def processar(pasta: Path, mes: str, cidades_uf: Iterable[tuple[str, str]]) -> int:
    """Monta <pasta>/cnpj.db só com os municípios pedidos. Grava num arquivo temporário e troca no fim."""
    pasta_mes = pasta / mes
    mun = municipios(pasta_mes)
    alvos: dict[bytes, str] = {}
    for cidade, uf in cidades_uf:
        codigo = mun.get(normalizar(cidade))
        if not codigo:
            raise SystemExit(f"Município não encontrado na tabela da Receita: {cidade}")
        alvos[chave_municipio(uf, codigo)] = normalizar(cidade)
    print("Municípios:", ", ".join(alvos.values()), flush=True)

    banco = pasta / "cnpj.db"
    tmp = banco.with_suffix(".novo")
    tmp.unlink(missing_ok=True)
    con = sqlite3.connect(tmp)
    con.executescript(ESQUEMA)
    lidas = guardadas = 0
    inicio = time.time()

    # 1) Estabelecimentos: o pré-filtro em bytes descarta ~99% das linhas antes de decodificar o CSV.
    for i in range(10):
        arq = pasta_mes / f"Estabelecimentos{i}.zip"
        lote = []
        for linha in linhas_zip(arq):
            lidas += 1
            municipio = next((nome for chave, nome in alvos.items() if chave in linha), None)
            if municipio and (reg := transformar_estabelecimento(ler_csv(linha), municipio)):
                lote.append(reg)
        con.executemany(f"INSERT OR REPLACE INTO estab VALUES ({','.join('?' * 17)})", lote)
        con.commit()
        guardadas += len(lote)
        print(f"  {arq.name}: {lidas:,} lidas, {guardadas:,} guardadas ({time.time() - inicio:.0f} s)", flush=True)

    # 2) Empresas: completa razão social, natureza jurídica e o selo MEI só para os CNPJs guardados.
    basicos = {b for (b,) in con.execute("SELECT DISTINCT basico FROM estab")}
    for i in range(10):
        atualizacoes = []
        for linha in linhas_zip(pasta_mes / f"Empresas{i}.zip"):
            if linha[1:9].decode() not in basicos:
                continue
            c = ler_csv(linha)
            razao, mei = limpar_razao(c[1], c[2])
            atualizacoes.append((razao, c[2], mei, c[0]))
        con.executemany("UPDATE estab SET razao=?, natureza=?, mei=? WHERE basico=?", atualizacoes)
        con.commit()
        print(f"  Empresas{i}.zip: {len(atualizacoes):,} razões sociais ({time.time() - inicio:.0f} s)", flush=True)

    con.execute("CREATE INDEX i_cnae ON estab(municipio_nome, cnae)")
    con.executemany("INSERT INTO info VALUES (?,?)", [
        ("mes", mes),
        ("municipios", ", ".join(alvos.values())),
        ("processado_em", time.strftime("%Y-%m-%dT%H:%M:%S")),
    ])
    con.commit()
    con.close()
    os.replace(tmp, banco)
    print(f"Pronto: {guardadas:,} estabelecimentos ativos com contato em {banco}", flush=True)
    return guardadas


# ---------------------------------------------------------------- consulta

def resumo(banco: Path, top: int = 15) -> list[tuple]:
    con = sqlite3.connect(banco)
    linhas = con.execute(
        """SELECT municipio_nome, cnae, COUNT(*) AS n, SUM(mei) AS meis
           FROM estab GROUP BY municipio_nome, cnae ORDER BY n DESC LIMIT ?""",
        (top,),
    ).fetchall()
    info = dict(con.execute("SELECT chave, valor FROM info"))
    total = con.execute("SELECT COUNT(*) FROM estab").fetchone()[0]
    con.close()
    print(f"Base de {info.get('mes')} ({info.get('municipios')}): {total:,} estabelecimentos")
    print(f"{'município':<22}{'CNAE':<10}{'total':>8}{'MEI':>8}")
    for m, cnae, n, meis in linhas:
        print(f"{m:<22}{cnae:<10}{n:>8,}{meis:>8,}")
    return linhas


def exportar(banco: Path, prefixos_cnae: list[str], saida: Path) -> int:
    """Exporta para CSV os estabelecimentos cujo CNAE principal começa com algum dos prefixos."""
    filtro = " OR ".join("cnae LIKE ?" for _ in prefixos_cnae)
    con = sqlite3.connect(banco)
    cur = con.execute(f"SELECT * FROM estab WHERE {filtro} ORDER BY municipio_nome, cnae",
                      [p + "%" for p in prefixos_cnae])
    colunas = [d[0] for d in cur.description]
    n = 0
    with saida.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(colunas)
        for linha in cur:
            w.writerow(linha)
            n += 1
    con.close()
    print(f"{n:,} linhas em {saida}")
    return n


# ---------------------------------------------------------------- CLI

def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--pasta", type=Path, default=Path(os.environ.get("CNPJ_DADOS", "dados")),
                   help="pasta dos .zip e do cnpj.db (padrão: ./dados ou $CNPJ_DADOS)")
    sub = p.add_subparsers(dest="comando", required=True)
    sub.add_parser("listar", help="meses disponíveis na Receita")
    b = sub.add_parser("baixar", help="baixa os arquivos de um mês (padrão: o mais recente)")
    b.add_argument("mes", nargs="?")
    pr = sub.add_parser("processar", help='monta o banco: processar 2026-10 "Salvador" BA')
    pr.add_argument("mes")
    pr.add_argument("cidade_uf", nargs="+", help="pares cidade UF")
    r = sub.add_parser("resumo", help="CNAEs com mais estabelecimentos")
    r.add_argument("--top", type=int, default=15)
    e = sub.add_parser("exportar", help="exporta um recorte por CNAE para CSV")
    e.add_argument("--cnae", action="append", required=True, help="prefixo do CNAE (repetível)")
    e.add_argument("--saida", type=Path, required=True)
    a = p.parse_args(argv)

    if a.comando == "listar":
        meses = listar_meses()
        tam = tamanhos(meses[-1])
        print("Meses:", ", ".join(meses[-6:]))
        print(f"{meses[-1]}: {sum(tam.get(x, 0) for x in ARQUIVOS) / 1e9:.1f} GB nos arquivos usados")
    elif a.comando == "baixar":
        baixar(a.pasta, a.mes)
    elif a.comando == "processar":
        if len(a.cidade_uf) % 2:
            p.error('informe pares cidade UF: processar 2026-10 "Salvador" BA')
        try:
            os.nice(15)  # processo longo: cede a CPU para o resto da máquina
        except OSError:
            pass
        processar(a.pasta, a.mes, list(zip(a.cidade_uf[::2], a.cidade_uf[1::2])))
    elif a.comando == "resumo":
        resumo(a.pasta / "cnpj.db", a.top)
    else:
        exportar(a.pasta / "cnpj.db", a.cnae, a.saida)


if __name__ == "__main__":
    main(sys.argv[1:])
