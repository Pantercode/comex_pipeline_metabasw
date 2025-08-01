
import os, time
from pathlib import Path
import pandas as pd
from .cliente_api import ClienteAPI

class DadosMunicipiosComex:
    """
    Coleta dados de importação/exportação por UF e país
    para todos os meses/anos especificados e gera um Parquet consolidado.
    """

    def __init__(self, cliente: ClienteAPI, output_dir: str):
        self.cliente = cliente
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _coletar_dados(self, flow: str, ano: int, mes: int) -> pd.DataFrame:
        payload = {
            "flow": flow,
            "monthDetail": False,
            "period": {
                "from": f"{ano}-{mes:02}",
                "to": f"{ano}-{mes:02}"
            },
            "filters": [{"filter": "state", "values": list(range(1, 28))}],
            "details": ["country", "state"],
            "metrics": ["metricFOB", "metricKG"]
        }
        headers = {'Content-Type': "application/json"}
        resposta = self.cliente.post("/cities?language=pt", payload, headers)
        lista = resposta.get("data", {}).get("list", [])

        if not lista:
            print(f"Nenhum dado em {flow.upper()} {ano}-{mes:02}")
            return pd.DataFrame()

        df = pd.DataFrame(lista)
        df['year'] = ano
        df['month'] = mes
        df['flow'] = flow
        return df

    def coletar_salvar_unificar(self, anos=range(2020, 2025), flows=('import', 'export')):
        for flow in flows:
            for ano in anos:
                for mes in range(1, 13):
                    print(f"🔄 Coletando {flow.upper()} {ano}-{mes:02}...")
                    df_mes = self._coletar_dados(flow, ano, mes)
                    if not df_mes.empty:
                        for col in ['metricFOB', 'metricKG']:
                            df_mes[col] = pd.to_numeric(df_mes[col], errors='coerce')

                        nome_arquivo = f"comex_{ano}_{mes:02}_{flow}.parquet"
                        df_mes.to_parquet(os.path.join(self.output_dir, nome_arquivo), index=False)
                        print(f"Salvo: {nome_arquivo}")
                    time.sleep(1.2)

        # Unificação
        print("\n🔄 Unificando arquivos .parquet...")
        arquivos = list(Path(self.output_dir).glob("comex_*.parquet"))
        if not arquivos:
            print("Nenhum arquivo encontrado para unificar.")
            return

        df_final = pd.concat([pd.read_parquet(a) for a in arquivos], ignore_index=True)
        caminho_final = os.path.join(self.output_dir, "importacao_exportacao_municipios.parquet")
        df_final.to_parquet(caminho_final, index=False)
        print(f"Arquivo final salvo em: {caminho_final}")

        # Limpeza
        for f in arquivos:
            try:
                os.remove(f)
                print(f"Arquivo deletado: {f.name}")
            except Exception as e:
                print(f"Erro ao deletar {f.name}: {e}")

        print(df_final.head())
