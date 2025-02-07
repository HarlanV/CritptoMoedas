

# Instalação:

## Requisitos
- Python 3.10.*
- MySql 8.*


## Instalação (Wndows)

### .env
Renomeie o arquivo ".env_exemplo" para ".env"

### Venv (optativo)
Como boa prática, recomenda-se o uso de ambientes virtuais (venv) para instalação das dependencias. Caso não tenha o ambiente instalado e deseje utilizar, siga os passos a seguir.

1. Com o python ja instalado, baixe a lib com comando abaixo
> pip install virtualenv

2. A seguir, crie um ambiente para seu desenvolvimento. Aqui chamarei de venv_exemplo, mas você é livre para utilizar o nome que lhe parecer melhor.
> python -m venv venv_exemplo

3. Por fim, inicie a utilização do ambiente criado.
> venv_cadastra\Scripts\activate.bat 

### Instalando libs
Para executar o programa, é necessário instalar as dependencias. Para isso, execute o comando abaixo. 

> pip install -r requirements.txt

### MySQL (opção 1 - Para uso local)
Neste projeto usaremos o mysql localmente. Esta opção é ideal para testes e uso individual. 
Para uso compartilhado, ou caso necessite de uma infraestrutura mais robusta, considere o uso de uma solução em nuvem. Sugerimos a opção de 


1. Verifique se o mysql está instalado e executando na sua maquina.

2. Vá até o arquivo .env e inclua as informações de configuração do seu banco ( MYSQL_USER_NAME, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DB).
Atenção: Não é necessário colocar aspas


### Google BigQuery (opção 2 - Para uso em nuvem):
Aqui vamos supor que você já possui um ambiente previamente disponibilizado, inclusive com projetos e acessos criados. Siga os passos abaixo para 

1. Para gerar tabela no google bigquery, é necessário ter uma chave de acesso do tipo json, que deve ser armazenada na raiz do projeto.

2. altere o nome da variavel "key_path" no arquivo main.py de acordo com o nome que deu para sua chave json.

3. Preencha as informações no arquivo .env

4. no arquivo "main.py", dentro da função "persist_table", inverta a ordem dos returns (ou seja, deve retornar persist_table_gcp)


## Instalação (Linux)
(em construção)


## Execução:
Para rodar o projeto, vá até a raiz do projeto e execute o arquivo main.py