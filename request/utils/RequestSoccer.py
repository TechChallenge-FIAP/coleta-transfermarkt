import concurrent.futures
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import boto3
import requests
from pydantic import BaseModel
from tqdm import tqdm

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
    request_multhread()
        Verifica a quantidade de endpoints e faz uma coleta em paralelo
        da função request_parallel utilizando 6 threads da sua CPU.
    request_parallel()
        Realiza as requisições necessárias conforme os endpoints recebidos.
    run()
        Orquestra e identifica se API precisa de season ou não, e roda
        a run_withoutloop ou run_with_loop.
    run_withoutloop()
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
    season: Optional[List[str]] = None
    endpoint: Dict[str, Optional[str]] = field(default_factory=dict)
    page_number: Optional[int] = None

    def request_parallel(self, endpoint: str) -> Dict:
        response = requests.get(f"https://transfermarkt-api.fly.dev/{endpoint}")

        if response.status_code != 200:
            response.raise_for_status()

        table = json.loads(response.text)
        return table

    def request_multhread(self, model: SoccerInfo) -> List[BaseModel]:
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            if isinstance(self.season, list):
                endpoints = [
                    model.endpoint.format(
                        id=id,
                        season=self.endpoint["season"],
                    )
                    for id in self.id["id"]
                ]
            elif isinstance(self.page_number, int):
                endpoints = [
                    model.endpoint.format(
                        id=id,
                        page_number=self.page_number,
                    )
                    for id in self.id["id"]
                ]
            else:
                endpoints = [model.endpoint.format(id=id) for id in self.id["id"]]
            list_of_response = []
            future_to_url = {
                executor.submit(self.request_parallel, endpoint): endpoint
                for endpoint in endpoints
            }
            for future in tqdm(concurrent.futures.as_completed(future_to_url)):
                data = future.result()
                list_of_response.append(data)
            executor.shutdown(wait=False)
        return [model.schema(**table) for table in list_of_response]

    def save_s3(self, bucket: str, table: dict):
        json_resp = [model.model_dump(mode="json") for model in table]
        s3 = boto3.resource("s3")
        s3.Bucket(bucket).put_object(
            Key=(
                f"{self.id['identifier']}/"
                "{self.page_number if isinstance(self.page_number, int) else ''}"
                "_{self.id['identifier']}.json"
            ),
            Body=json.dumps(json_resp, ensure_ascii=False),
        )

    def save_json(self, table: List[BaseModel]) -> None:
        json_resp = [model.model_dump(mode="json") for model in table]
        data = json.dumps(json_resp, ensure_ascii=False)
        Path(f"./amostra/landing/{self.id['identifier']}/").mkdir(
            parents=True, exist_ok=True
        )
        with open(
            f"./amostra/landing/{self.id['identifier']}/{self.id['identifier']}.json",
            "w",
            encoding="utf-8",
        ) as write_file:
            write_file.write(data)

    def run_with_loop(self, model: SoccerInfo) -> None:
        list_of_tables = []
        for season in self.season or []:
            self.endpoint["season"] = season
            table = self.request_multhread(model)
            list_of_tables.extend(table)

        self.save_s3(bucket="tech-challenge-3-landing-zone", table=list_of_tables)

    def run_withoutloop(self, model: SoccerInfo) -> int:
        table = self.request_multhread(model)
        last_page: int = table.get("lastPageNumber", None)

        self.save_s3(bucket="tech-challenge-3-landing-zone", table=table)
        if last_page:
            return last_page
        else:
            return -1

    def run(self) -> str | int:
        model = identify_class(self.id)
        if isinstance(self.season, list):
            self.run_with_loop(model)
            return "Loaded with loop"

        return self.run_withoutloop(model)
