import csv

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    StandardOptions,
)
from google.cloud import bigquery

# Configurações

GCS_INPUT = "gs://imdb-repo/raw/imdb_top_1000.csv"
BQ_PROJECT = "inbound-byway-475719-v0" 
BQ_DATASET = "imdb_dataset"
BQ_TABLE = "raw_imdb"
BQ_TABLE_SPEC = f"{BQ_PROJECT}:{BQ_DATASET}.{BQ_TABLE}"


# Colunas do CSV
CSV_COLUMNS = [
    "Poster_Link","Series_Title","Released_Year","Certificate","Runtime","Genre",
    "IMDB_Rating","Overview","Meta_score","Director","Star1","Star2","Star3","Star4",
    "No_of_Votes","Gross"
]


# Schema  para o BigQuery
BQ_SCHEMA = {
    
    "fields": [
        {"name": "link_poster", "type": "STRING", "description": "Link do pôster que o IMDb está usando"},
        {"name": "titulo", "type": "STRING", "description": "Nome do filme ou série"},
        {"name": "ano_lancamento", "type": "INTEGER", "description": "Ano em que o filme foi lançado"},
        {"name": "classificacao", "type": "STRING", "description": "Certificado obtido por esse filme"},
        {"name": "duracao", "type": "STRING", "description": "Duração total do filme"},
        {"name": "genero", "type": "STRING", "description": "Gênero do filme"},
        {"name": "nota_imdb", "type": "FLOAT", "description": "Nota do filme no site IMDb"},
        {"name": "sinopse", "type": "STRING", "description": "Resumo do filme"},
        {"name": "meta_score", "type": "INTEGER", "description": "Pontuação obtida pelo filme"},
        {"name": "diretor", "type": "STRING", "description": "Nome do Diretor"},
        {"name": "ator1", "type": "STRING", "description": "Nome dos atores"},
        {"name": "ator2", "type": "STRING", "description": "Nome dos atores"},
        {"name": "ator3", "type": "STRING", "description": "Nome dos atores"},
        {"name": "ator4", "type": "STRING", "description": "Nome dos atores"},
        {"name": "votos", "type": "INTEGER", "description": "Número total de votos"},
        {"name": "receita", "type": "FLOAT", "description": "Dinheiro arrecadado por aquele filme"},
    ]
}


# === Função de transformação principal ===
def parse_and_transform(line):    
    values = next(csv.reader([line]))
    record = dict(zip(CSV_COLUMNS, values))

    # Converte tipos básicos
    def to_int(val):
        try:
            return int(val.replace(",", "").strip())
        except:
            return None

    def to_float(val):
        try:
            return float(val.replace(",", "").strip())
        except:
            return None

    transformed = {
        "link_poster": record["Poster_Link"],
        "titulo": record["Series_Title"],
        "ano_lancamento": to_int(record["Released_Year"]),
        "classificacao": record["Certificate"],
        "duracao": record["Runtime"],
        "genero": record["Genre"],
        "nota_imdb": to_float(record["IMDB_Rating"]),
        "sinopse": record["Overview"],
        "meta_score": to_int(record["Meta_score"]),
        "diretor": record["Director"],
        "ator1": record["Star1"],
        "ator2": record["Star2"],
        "ator3": record["Star3"],
        "ator4": record["Star4"],
        "votos": to_int(record["No_of_Votes"]),
        "receita": to_float(record["Gross"]),
    }

    return transformed

# Acrescentando descrição na tabela
def update_table_description(project_id, dataset_id, table_id, description):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    table.description = description
    updated_table = client.update_table(table, ["description"])
    print(f"Tabela {table_id} atualizada com a descrição: {updated_table.description}")

# === PIPELINE ===
def run():
    options = PipelineOptions()
    gcloud_options = options.view_as(GoogleCloudOptions)
    gcloud_options.project = BQ_PROJECT
    gcloud_options.job_name = "dataflow-imdb"
    gcloud_options.region = "us-central1"
    gcloud_options.temp_location = "gs://imdb-data-repo/temp"
    gcloud_options.staging_location = "gs://imdb-data-repo/staging"
    #gcloud_options.save_main_session
    options.view_as(StandardOptions).runner = "DataflowRunner" 

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Ler CSV" >> ReadFromText(GCS_INPUT, skip_header_lines=1)
            | "Transformar registros" >> beam.Map(parse_and_transform)
            | "Escrever no BigQuery" >> WriteToBigQuery(
                table=BQ_TABLE_SPEC,
                schema=BQ_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                
            )
        )


if __name__ == "__main__":
    run()
    # Atualizar descrição da tabela após o job Dataflow terminar
    update_table_description(
        BQ_PROJECT,
        BQ_DATASET,
        BQ_TABLE,
        "Banco de dados IMDB com os 1000 melhores filmes e programas de TV"
    )


