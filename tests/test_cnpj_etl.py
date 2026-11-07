"""Testes com arquivos sintéticos no mesmo formato da Receita (dados inventados)."""
import csv
import io
import sqlite3
import zipfile

import pytest

import cnpj_etl as etl


def _zip(caminho, linhas):
    buf = io.StringIO()
    csv.writer(buf, delimiter=";", quoting=csv.QUOTE_ALL, lineterminator="\n").writerows(linhas)
    with zipfile.ZipFile(caminho, "w") as z:
        z.writestr(caminho.stem.upper() + ".CSV", buf.getvalue().encode("latin-1"))


def _estab(basico, situacao="02", uf="BA", mun="3849", ddd="71", tel="33334444", email="", cnae="5611201"):
    c = [""] * 30
    c[0], c[1], c[2], c[4], c[5] = basico, "0001", "99", "Fantasia " + basico, situacao
    c[10], c[11], c[12] = "20200101", cnae, ""
    c[13], c[14], c[15], c[17], c[18] = "RUA", "DAS FLORES", "10", "CENTRO", "40000000"
    c[19], c[20], c[21], c[22], c[27] = uf, mun, ddd if tel else "", tel, email
    return c


@pytest.fixture
def pasta(tmp_path):
    mes = tmp_path / "2026-10"
    mes.mkdir()
    _zip(mes / "Municipios.zip", [["3849", "SALVADOR"], ["3535", "FEIRA DE SANTANA"]])
    estab = [
        _estab("11111111"),                                   # ativo com telefone: entra
        _estab("22222222", tel="", email="CONTATO@EX.COM"),   # só e-mail: entra
        _estab("33333333", situacao="08"),                    # baixado: sai
        _estab("44444444", tel="", email="sem-arroba"),       # sem contato válido: sai
        _estab("55555555", mun="3535"),                       # outro município: sai
    ]
    _zip(mes / "Estabelecimentos0.zip", estab)
    _zip(mes / "Empresas0.zip", [
        ["11111111", "RESTAURANTE EXEMPLO LTDA", "2062"],
        ["22222222", "MARIA EXEMPLO 12345678901", "2135"],
    ])
    for i in range(1, 10):
        _zip(mes / f"Estabelecimentos{i}.zip", [])
        _zip(mes / f"Empresas{i}.zip", [])
    return tmp_path


def test_normalizar():
    assert etl.normalizar("Feira-de-Santana") == "FEIRA DE SANTANA"
    assert etl.normalizar(" São  Félix ") == "SAO FELIX"


def test_limpar_razao_remove_cpf_e_marca_mei():
    assert etl.limpar_razao("MARIA EXEMPLO 12345678901", "2135") == ("MARIA EXEMPLO", 1)
    assert etl.limpar_razao("ACME LTDA", "2062") == ("ACME LTDA", 0)


def test_processar_filtra_ativos_com_contato(pasta):
    assert etl.processar(pasta, "2026-10", [("Salvador", "BA")]) == 2
    con = sqlite3.connect(pasta / "cnpj.db")
    linhas = {r[0]: r for r in con.execute("SELECT cnpj, razao, mei, email, tel1, endereco FROM estab")}
    assert set(linhas) == {"11111111000199", "22222222000199"}
    assert linhas["22222222000199"][1:4] == ("MARIA EXEMPLO", 1, "contato@ex.com")
    assert linhas["11111111000199"][4:] == ("7133334444", "RUA DAS FLORES, 10")
    assert dict(con.execute("SELECT chave, valor FROM info"))["municipios"] == "SALVADOR"


def test_municipio_inexistente(pasta):
    with pytest.raises(SystemExit):
        etl.processar(pasta, "2026-10", [("Atlantida", "BA")])


def test_exportar(pasta, tmp_path):
    etl.processar(pasta, "2026-10", [("Salvador", "BA")])
    saida = tmp_path / "out.csv"
    assert etl.exportar(pasta / "cnpj.db", ["5611"], saida) == 2
    assert etl.exportar(pasta / "cnpj.db", ["47"], saida) == 0
