import json
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests
from pydantic import BaseModel

from models import SoccerInfo, identify_class


@dataclass
class RequestSoccer:
    """
    Classe tem como objetivo fazer as requisições e coletar os dados da
    API do transfermarkt

    ...

    Attributes
    ----------
    id : Dict[str, str]
        Id contendo um dicionário com o id sendo uma
        lista de id's a serem coletados e identifier para
        identificar o classe do pydantic a ser utilizada.
    season : Optional[List[str]]
        Lista contendo os anos dos campeonatos para serem coletados.
    endpoint : Dict[str, Optional[str]] = field(default_factory=dict)
        Dicionário padrão para armazenar de forma temporária info de id
        ou season.


    Methods
    ----------
    request_data()
        Realiza as requisições necessárias conforme a quantidade de ids
        e coleta todos os dados do endpoint fornecido além de validar
        em conjunto com o pydantic o formato dos dados.
    run()
        Orquestra e identifica se API precisa de season ou não, e roda
        a run_withoutloop ou run_with_loop.
    run_withoutlop()
        Caso não seja necessária a season o método a ser escolhido será esse
        usa o request_data e depois o save_json.
    run_with_loop()
        Caso seja necessária a season o método a ser escolhido será esse
        usa o request_data e depois o save_json e coleta todos os seasons
        fornecidos.
    save_json()
        Salva os arquivos em json localmente.
    save_s3()
        Salva os arquivos no S3. Em construção
    """

    id: Dict[str, str]
    season: Optional[List[str]]
    endpoint: Dict[str, Optional[str]] = field(default_factory=dict)

    def request_data(self, model: SoccerInfo) -> List[BaseModel]:
        list_of_response = []
        for id in self.id["id"]:
            self.endpoint["id"] = id
            if isinstance(self.season, list):
                endpoint = model.endpoint.format(
                    id=id,
                    season=self.endpoint["season"],
                )
            else:
                endpoint = model.endpoint.format(id=id)
            print(endpoint)
            time.sleep(4)
            response = requests.get(
                f"https://transfermarkt-api.fly.dev/{endpoint}",
            )

            if response.status_code != 200:
                response.raise_for_status()

            table = json.loads(response.text)
            list_of_response.append(table)
        return [model.schema(**table) for table in list_of_response]

    # TODO Criar função para salvar arquivo no S3
    def save_s3(self, table):
        pass

    def save_json(self, table: List[BaseModel]) -> None:
        json_resp = [model.model_dump(mode="json") for model in table]
        data = json.dumps(json_resp, ensure_ascii=False)
        with open(
            f"./amostra/{self.id['identifier']}.json", "w", encoding="utf-8"
        ) as write_file:
            write_file.write(data)

    def run_with_loop(self, model: SoccerInfo) -> None:
        list_of_tables = []
        for season in self.season or []:
            self.endpoint["season"] = season
            table = self.request_data(model)
            list_of_tables.extend(table)

        self.save_json(list_of_tables)

    def run_withoutloop(self, model: SoccerInfo) -> None:
        table = self.request_data(model)
        self.save_json(table)

    def run(self) -> str:
        model = identify_class(self.id)
        if isinstance(self.season, list):
            self.run_with_loop(model)
            return "Loaded with loop"

        self.run_withoutloop(model)
        return "Loaded without loop"
