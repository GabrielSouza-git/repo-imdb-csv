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

O código importa bibliotecas padrão do Python e módulos do Apache Beam:


<img width="639" height="264" alt="Image" src="https://github.com/user-attachments/assets/f2ccc7a1-1f76-4ea2-b044-98d9ee99f0b2" />
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
- job_name: o nome que o job terá no dataflow
- region: região que estamos utilizando
- temp_location: guarda arquivos temporários do job em execução. Essa pasta é criada automaticamente
- staging_location: Guarda arquivos necessários para iniciar o job. Essa pasta é criada automaticamente
- ReadFromText: lê o CSV diretamente do GCS.
- beam.Map(parse_and_transform): aplica a função de transformação linha a linha.
- WriteToBigQuery: grava os dados processados na tabela de destino.
Observação:
O modo WRITE_TRUNCATE substitui o conteúdo da tabela a cada execução.
Use WRITE_APPEND caso queira apenas adicionar novos registros.
```

### Passo 5: Rodando o código
Agora que já temos tudo configurado, podemos executar o código no terminal

```bash
  python csv_dataflow_imdb.py
```

Ficará algo assim: 
</div>
<img width="1580" height="348" alt="Image" src="https://github.com/user-attachments/assets/53bd033a-3d3e-4059-b10c-4d3bdc6fe844" />
</div>

\
E quando você olhar no Dataflow, verá o seu job executando
</div>
<img width="1874" height="359" alt="Image" src="https://github.com/user-attachments/assets/f7383cf8-e7a1-4084-9567-d6575a0b24ce" />
</div>


Clicando nele, veremos as 3 etapas que criamos no Apache Beam
1. Ler CSV
2. Transformar registros
3. Escrever no BigQuery
   
</div>
<img width="1673" height="800" alt="Image" src="https://github.com/user-attachments/assets/6e054a26-e364-466d-809b-aed57ea563ca" />
</div>

\
O Job foi conclúido com sucesso, agora vamos ver a tabela foi criado no BigQuery
<img width="1897" height="824" alt="Image" src="https://github.com/user-attachments/assets/20eb2906-c0ca-4120-87eb-53350495ee55" />



### Passo 6: Checkando os dados no BigQuery
Lembram que criamos o dataset no Passo 3?
Agora a nossa tabela foi materializada aqui 

</div>
<img width="342" height="808" alt="Image" src="https://github.com/user-attachments/assets/11939028-8928-4e3f-a383-b449a624e58f" />
</div>

\
Ao clicar nela, podemos ver alguns dados sobre ela:
No menu **`Esquema`** podemos ver o nome do campo, tipagem e descrição (mas nesse caso não passamos nenhuma)
<img width="1374" height="771" alt="Image" src="https://github.com/user-attachments/assets/2a7724eb-b571-4d8c-aa5d-1dbe14d3bc9a" />

Na aba **`Detalhes`** podemos ver alguns dados interessantes também:

1. Informações da tabela:
Id da tabela, data de criação e data de modificação, descrição da tabela (não passamos nenhuma) 
2. Informações de armazenamento:
Número de linhas e total de bytes lógicos
</div>
<img width="1027" height="781" alt="Image" src="https://github.com/user-attachments/assets/c111be58-58ae-466e-bbef-75d8e5b81708" />
</div>




\
E na aba **`Visualização`** conseguimos ter uma prévia dos dados da tabela
</div>
<img width="1659" height="755" alt="Image" src="https://github.com/user-attachments/assets/d8c365d6-b0b4-406c-a250-9fac55c47128" />
</div>


\
Pra fazermos querys na tabela é bem simples, podemos clicar em consulta e abrirá uma nova aba 
</div>
<img width="1168" height="731" alt="Image" src="https://github.com/user-attachments/assets/aea3dccd-3a2c-4718-bc05-bfe1b5b236d6" />
</div>

### Passo 7: Fazendo consultas no BigQuery
#### 1 - Quais os 10 primeiros filmes com maiores notas no imdb?

```bash
select titulo, nota_imdb from `inbound-byway-475719-v0.imdb_dataset.raw_imdb` 
order by nota_imdb desc
limit 10
```
Obs: Antes de rodar a query o BigQuery já estima a quantidade de processamento que será consumido ao executar a query
</div>
<img width="613" height="510" alt="Image" src="https://github.com/user-attachments/assets/505fda97-fb71-46a0-a461-5c4655cf87d4" />
</div>

#### 2 - Quais os nomes dos 10 primeiros filmes e suas respectivas notas que mais geraram receita?
```bash
select titulo, nota_imdb, receita from `inbound-byway-475719-v0.imdb_dataset.raw_imdb` 
order by receita desc
limit 10
```
</div>
<img width="813" height="478" alt="Image" src="https://github.com/user-attachments/assets/32a776d0-7e07-4c2c-af45-67df99129ee4" />
</div>

#### 3 - Quais são os 5 anos mais recentes presentes na base e quantos títulos foram lançados em cada um desses anos? (Lembrando que a base são dos 1000 melhores filmes, então já possui um filtro)
```bash
select ano_lancamento, count(*) as qtd from `inbound-byway-475719-v0.imdb_dataset.raw_imdb` 
group by 1
order by 1 desc
limit 5
```
Obs: No BigQuery não precisamos digitar o nome do campo no groupby e no orderby, podemos utilizar o número conforme a posição que ele está na query
</div>
<img width="676" height="356" alt="Image" src="https://github.com/user-attachments/assets/63d34d9b-f53f-41b4-8876-c677bb6bf3e4" />
</div>

#### 4 - Quais os nomes e suas respectivas notas dos 10 primeiros filmes que mais geraram receita?
```bash
select titulo, nota_imdb, receita from `inbound-byway-475719-v0.imdb_dataset.raw_imdb` 
order by receita desc
limit 10
```
</div>
<img width="813" height="478" alt="Image" src="https://github.com/user-attachments/assets/32a776d0-7e07-4c2c-af45-67df99129ee4" />
</div>

#### 5 - Quais são os 5 diretores com maior receita acumulada, e quantos filmes cada um deles possui?
```bash
select diretor, count(*) as qtd_filmes, sum(receita) as soma_receita from `inbound-byway-475719-v0.imdb_dataset.raw_imdb` 
group by 1
order by soma_receita desc
limit 5
```
</div>
<img width="884" height="378" alt="Image" src="https://github.com/user-attachments/assets/be19882b-596e-4755-89b4-f6d4c5b6d612" />
</div>
