
import pandas as pd
from .requisicao_base import RequisicaoBase

class CategoriasProdutos(RequisicaoBase):
    """
    Baixa categorias de produtos e salva em Parquet.
    """

    def executar(self):
        response = self.cliente.get("/tables/product-categories?language=pt&page=1&perPage=1000")
        categorias = response.get("data", {}).get("list", [])
        if not categorias:
            print("Nenhuma categoria encontrada.")
            return

        df = pd.DataFrame(categorias)
        df.to_parquet(self.caminho_saida, index=False)
        print(f"Arquivo salvo em: {self.caminho_saida}")
