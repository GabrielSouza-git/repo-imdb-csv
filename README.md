#  Pipeline de ingestão IMDB com Apache Beam e Dataflow

Este projeto implementa um **pipeline de ingestão de dados** que lê um arquivo CSV do **Google Cloud Storage (GCS)**, transforma e carrega os dados no **BigQuery** usando o **Apache Beam** e o **Google Dataflow**.

---

##  Visão geral

O objetivo é automatizar a ingestão dos dados do dataset **IMDB Top 1000 Movies** (disponível no Kaggle), utilizando ferramentas da **Google Cloud Platform (GCP)**.  
O pipeline executa as seguintes etapas:

1. Lê um arquivo CSV armazenado no GCS.  
2. Transforma e padroniza os campos (tipos, nomes e formatos).  
3. Escreve o resultado final em uma tabela do BigQuery.  

---

##  Requisitos

- **Python 3.8+**
- **Bibliotecas:**
  ```bash
  pip install requirements.txt
  ```

---

### Entendendo o que vamos ingerir
O dataset utilizado será esse: 
[Kaggle_IMDB_Dataset](https://www.kaggle.com/datasets/harshitshankhdhar/imdb-dataset-of-top-1000-movies-and-tv-shows/data)

<div align="center">
<img width="1613" height="495" alt="Image" src="https://github.com/user-attachments/assets/6019a910-3af0-411d-99ae-423323346d19" />
</div>
Antes de sair codando, vale dar uma olhada no conteúdo do arquivo pra entender a estrutura e a tipagem dos dados.  
Logo de cara, já dá pra ver que algumas colunas merecem atenção especial:

- **Poster_Link** → traz uma URL gigante, então o tipo ideal é `STRING`.  
- **Runtime** → já vem em minutos e com escrita "min", então vale deixar como string e tratar esse campo pra facilitar ordenações depois. 
- **Gross** → vem com vírgula nos valores e como o formato numérico padrão usa ponto, vamos precisar ajustar isso durante o processamento.  

Com isso em mente… bora pra prática! 😎  

---

### Passo 1: Criar o bucket no Google Cloud Storage

Vamos começar criando um bucket no **Google Cloud Storage** pra armazenar o arquivo que vamos ingerir.  
O nome do bucket precisa ser **único globalmente**, então escolha um que ainda não exista.  
No meu caso, usei:  

```text
imdb-repo
```

<div align="center">
<img width="1238" height="809" alt="Image" src="https://github.com/user-attachments/assets/78cbe98b-e12c-4c84-a0be-de7e52e55624" />
</div>

### Passo 2: Preparando o ambiente no GCP

Com o bucket criado, o próximo passo é organizar nossos dados dentro dele.

Crie uma pasta chamada **`raw`** dentro do seu bucket, é nela que vamos colocar o arquivo CSV que será ingerido.  
<div align="center">
<img width="1038" height="454" alt="Image" src="https://github.com/user-attachments/assets/75404009-8d97-4387-bb8d-a4296d43afc5" />
</div>

\
Agora, faça o **upload do arquivo `imdb_top_1000.csv`** para dentro dessa pasta.  
<div align="center">
<img width="1658" height="364" alt="Image" src="https://github.com/user-attachments/assets/50148864-95cd-4749-bcc6-4c498f62afcb" />
</div>


### Passo 3: Criando o dataset no BigQuery

Com o arquivo salvo no GCS, bora preparar o destino dos dados no **BigQuery**.

1. Vá até o **BigQuery** no console do GCP.  
   Ao abrir, do lado esquerdo você verá o seu projeto listado.  
   Clique nos **três pontinhos** ao lado do nome do projeto e selecione **"Criar conjunto de dados"**.  
<div align="center">
<img width="1520" height="418" alt="Image" src="https://github.com/user-attachments/assets/e2867006-3055-401d-a012-cdbc0553504a" />
</div>


\
2. No menu que aparecer à direita, defina o nome do seu dataset.  
   No meu caso, usei:  
   ```text
   imdb_dataset
   ```

<img width="528" height="662" alt="Image" src="https://github.com/user-attachments/assets/09bb44fa-dc87-4792-a3c2-e281c59054f9" />
</div>


\
3. Depois que criar, o dataset vai aparecer listado no painel esquerdo do BigQuery.
\
<img width="265" height="680" alt="Image" src="https://github.com/user-attachments/assets/500a4d08-8380-4056-9e50-a97f278cd4c6" />
</div>

   
### Passo 4: Estrutura do código
Com o bucket e o dataset prontos, chegou a hora de criar o código que vai iniciar o job no Dataflow e fazer todo o processamento dos dados.
Agora sim é a parte divertida 😎

#### 4.1 - Importações

O código importa bibliotecas padrão do Python e módulos do Apache Beam: ALTERAR DPS


<img width="1012" height="240" alt="Image" src="https://github.com/user-attachments/assets/4b00cf62-a3a9-402c-baf7-b2a8ae768709" />
</div>


```text
- csv: leitura segura de linhas CSV.
- dotenv: carrega variáveis do .env.
- apache_beam: constrói e executa o pipeline.
- ReadFromText: lê o arquivo CSV.
- WriteToBigQuery: grava os dados processados no BigQuery.
```

#### 4.2 - Configurações principais
Temos nossas constantes que centralizam as variáveis do projeto, servindo para facilitar a leitura e manutenção do código.

<img width="472" height="148" alt="Image" src="https://github.com/user-attachments/assets/09abf432-bdc6-42dc-9fd9-ca7889a3bdcf" />
</div>

```text
- GCS_INPUT = passamos o caminho onde o está nosso arquivo csv
- BQ_PROJECT = id do projeto
- BQ_DATASET = dataset que criamos anteriormente no BigQuery
- BQ_TABLE = nome que a tabela terá assim que criada
```

#### 4.3 - Estrutura e schema
Colunas esperadas do CSV

A função parse_and_transform usa zip(CSV_COLUMNS, values), então a ordem das colunas no CSV deve ser exatamente igual à lista CSV_COLUMNS, se não, os dados ficarão desalinhados.

<img width="689" height="130" alt="Image" src="https://github.com/user-attachments/assets/485b576d-7961-4623-9802-8b3d296bcb67" />
</div>

Schema BigQuery

</div>
<img width="459" height="415" alt="Image" src="https://github.com/user-attachments/assets/9ff5872b-f90e-46e9-96c7-96db559c945e" />
</div>

```text
- Define o esquema utilizado pelo WriteToBigQuery  
- Os nomes devem corresponder às chaves dos dicionários gerados na transformação  
- Tipos BigQuery: STRING, INTEGER, FLOAT.
```

#### 4.4 - Função de transformação 
A função  **`parse_and_transform()`** lê cada linha do CSV, aplica as funções auxiliares **`to_int`** e **`to_float`** para normalizar os tipos, e retorna um dicionário pronto para ingestão no BigQuery.
(Lembram quando abrimos o arquivo csv para dar uma olhada? O campo Gross era um campo decimal mas era separado por vírgula, então utilizamos a função para retirar a virgula)


</div>
<img width="480" height="730" alt="Image" src="https://github.com/user-attachments/assets/8a711c69-b20a-4c7a-ba7b-543ac8ac557c" />
</div>


#### 4.5 - Pipeline Apache Beam
Aqui montamos o fluxo completo de leitura, transformação e escrita no BigQuery, usando o Apache Beam e executando no Google Dataflow.

</div>
<img width="672" height="482" alt="Image" src="https://github.com/user-attachments/assets/b949ff17-117d-4d70-9fdb-50ce8dea8650" />
</div>

```text
- ReadFromText: lê o CSV diretamente do GCS.
- beam.Map(parse_and_transform): aplica a função de transformação linha a linha.
- WriteToBigQuery: grava os dados processados na tabela de destino.
Observação:
O modo WRITE_TRUNCATE substitui o conteúdo da tabela a cada execução.
Use WRITE_APPEND caso queira apenas adicionar novos registros.
```






