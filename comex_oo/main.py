
from getpass import getpass
from api.cliente_api import ClienteAPI
from api.classificacoes import ClassificacoesComex
from api.produtos import CategoriasProdutos
from api.modos_transporte import ModosTransporte
from api.localidades import LocalidadesEUA
from api.filtros import BlocosEconomicosCidades, MetricasComex
# from api.dados_municipios import DadosMunicipiosComex
# from api.dados_produtos import DadosProdutosComex
# from banco.insercao_postgres import InsercaoPostgresComex

def main():
    cliente = ClienteAPI()

    ClassificacoesComex(cliente, r"C:\Users\marcell.oliveira\Downloads\comex_oo\stage\classificacoes.parquet").executar()
    CategoriasProdutos(cliente, r"C:\Users\marcell.oliveira\Downloads\comex_oo\stage\categorias_produtos.parquet").executar()
    ModosTransporte(cliente, r"C:\Users\marcell.oliveira\Downloads\comex_oo\stage\modos_transporte.parquet").executar()
    LocalidadesEUA(cliente, r"C:\Users\marcell.oliveira\Downloads\comex_oo\stage\localidade_eua.parquet").executar()
    BlocosEconomicosCidades(cliente, r"C:\Users\marcell.oliveira\Downloads\comex_oo\stage\blocos_economicos_cidade.parquet").executar()
    MetricasComex(cliente, r"C:\Users\marcell.oliveira\Downloads\comex_oo\stage\metricas.parquet").executar()

    # Exemplos demorados:
    # DadosMunicipiosComex(cliente, r"C:\Users\marcell.oliveira\Downloads\comex_oo\stage").coletar_salvar_unificar()
    # DadosProdutosComex(cliente, r"C:\Users\marcell.oliveira\Downloads\comex_oo\stage").coletar_salvar_unificar()

    # Inserção no banco
    # insercao = InsercaoPostgresComex(r"C:\Users\marcell.oliveira\Downloads\comex_oo\stage", senha=getpass("Senha do Postgres: "))
    # insercao.processar_arquivos()

if __name__ == "__main__":
    main()
