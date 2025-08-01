
import os, time
from pathlib import Path
import pandas as pd
from .cliente_api import ClienteAPI

class DadosProdutosComex:
    """
    Coleta dados de importação/exportação por produto (NCM) e gera Parquet consolidado.
    """

    def __init__(self, cliente: ClienteAPI, output_dir: str):
        self.cliente = cliente
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _coletar_dados(self, flow: str, ano: int, mes: int) -> pd.DataFrame:
        payload = {
            "flow": flow,
            "monthDetail": True,
            "period": {
                "from": f"{ano}-{mes:02}",
                "to": f"{ano}-{mes:02}"
            },
            "filters": [],
            "details": ["country", "state", "ncm"],
            "metrics": [
                "metricFOB", "metricKG", "metricStatistic",
                "metricFreight", "metricInsurance", "metricCIF"
            ]
        }
        headers = {'Content-Type': "application/json", 'Accept': "application/json"}
        resposta = self.cliente.post("/general?language=pt", payload, headers)
        lista = resposta.get("data", {}).get("list", [])

        if not lista:
            print(f"Nenhum dado em {flow.upper()} {ano}-{mes:02}")
            return pd.DataFrame()

        df = pd.DataFrame(lista)
        df['year'] = ano
        df['month'] = mes
        df['flow'] = flow
        return df

    def coletar_salvar_unificar(self, anos=range(2020, 2025), flows=('import',)):
        for flow in flows:
            for ano in anos:
                for mes in range(1, 13):
                    print(f"🔄 Coletando {flow.upper()} {ano}-{mes:02}...")
                    df_mes = self._coletar_dados(flow, ano, mes)
                    if not df_mes.empty:
                        colunas_numericas = [
                            "metricFOB", "metricKG", "metricStatistic",
                            "metricFreight", "metricInsurance", "metricCIF"
                        ]
                        for col in colunas_numericas:
                            df_mes[col] = pd.to_numeric(df_mes[col], errors='coerce')

                        nome_arquivo = f"comex_{ano}_{mes:02}_{flow}.parquet"
                        df_mes.to_parquet(os.path.join(self.output_dir, nome_arquivo), index=False)
                        print(f"📁 Salvo: {nome_arquivo}")
                    time.sleep(1.0)

        # Unificação
        print("\n🔄 Unificando arquivos...")
        arquivos = list(Path(self.output_dir).glob("comex_*.parquet"))
        if not arquivos:
            print(" Nenhum arquivo encontrado para unificar.")
            return

        df_final = pd.concat([pd.read_parquet(f) for f in arquivos], ignore_index=True)
        caminho_final = os.path.join(self.output_dir, "importacao_exportacao_produtos.parquet")
        df_final.to_parquet(caminho_final, index=False)
        print(f" Arquivo final salvo em: {caminho_final}")

        # Limpeza
        for f in arquivos:
            try:
                os.remove(f)
                print(f" Deletado: {f.name}")
            except Exception as e:
                print(f" Erro ao deletar {f.name}: {e}")

        print(df_final.head())
