
import http.client
import json

class ClienteAPI:
    """
    Cliente HTTP simples para consumir a API ComexStat.
    """

    def __init__(self, host: str = "api-comexstat.mdic.gov.br"):
        self.host = host
        self.conn = http.client.HTTPSConnection(self.host)

    def get(self, endpoint: str) -> dict:
        """Executa requisição GET e retorna JSON já decodificado."""
        self.conn.request("GET", endpoint)
        res = self.conn.getresponse()
        data = res.read()
        return json.loads(data.decode("utf-8"))

    def post(self, endpoint: str, payload: dict, headers: dict | None = None) -> dict:
        """Executa requisição POST com JSON no corpo."""
        if headers is None:
            headers = {
                'Content-Type': "application/json",
                'Accept': "application/json"
            }
        self.conn.request("POST", endpoint, body=json.dumps(payload), headers=headers)
        res = self.conn.getresponse()
        data = res.read()
        return json.loads(data.decode("utf-8"))
