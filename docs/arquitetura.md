# Componentes de Infraestrutura do Projeto

## 1. EC2 - Scraper Betfair
Coleta diariamente as odds de eventos futuros da Betfair, processa e envia os resultados para armazenamento em S3.

### Responsabilidades
1. Rodar o script betfair-scraper.py automaticamente no boot
    1. Coletar e transformar os dados brutos
    2. Salvar os resultados em um bucket S3 (s3/betfair-scraper)
    3. Publicar uma mensagem em um SNS Topic <b>(A)</b> indicando a conclusão do processo

## 2. EC2 - Scraper OMQB
Executa após a conclusão do Betfair Scraper, utilizando seus dados como input

A instância só é iniciada quando o SNS Topic A recebe a notificação de conclusão do Betfair Scraper.

### Responsabilidades
1. Baixar do S3 os resultados gerados pelo Betfair Scraper
2. Baixar o cache de dados já minerados do OMQB
3. Obter resultados através de scraping no site OMQB e/ou através dos dados em cache
4. Gerar uma página HTML com a tabela de resultados
5. Notificar o usuário via e-mail após a conclusão do processo, incluindo um link pré-assinado com a página HTML
6. Armazenar resultados .csv em um bucket S3
7. Notificar SNS Topic <b>(B)</b> da conclusão do processo

## S3 - Buckets

O sistema utiliza dois buckets principais:

### betfair-scraper
- Armazena os dados brutos coletados da betfair
- Armazena os links de acesso aos dados de cada liga de futebol
- Armazena o script betfair-scraper.py

### omqb-scraper
- Armazena saídas tratadas e resultados do OMQB
- Armazena o csv de cache, com os resultados que já foram processados no OMQB
- Armazena o script omqb-scraper.py
- Armazena a página HTML com os resultados do dia

Ambos os buckets possuem versionamento ativado e acesso restrito por IAM

## SNS - Comunicação entre os Processos

O sistema utiliza dois SNS Topics como mecanismo de orquestração

### Tópico A
O Betfair Scraper, ao finalizar, publica para este tópico.

Os assinantes são:

    1. Lambda que executa o shutdown da instância Betfair Scraper
    2. Lambda que executa a inicialização da instância OMQB Scraper

### Tópico B
O OMQB Scraper, ao finalizar, publica para este tópico

Os assinantes são:
    
    1. Lambda que executa o shutdown da instância OMQB Scraper


## Lambda Functions

Há três funções Lambda

#### StartEC2BetfairScraper
    Acionada via EventBridge (6am todos os dias)
    Inicializa a instância EC2 Betfair Scraper

#### StopEc2BetfairScraper
    Acionada via SNS Topic A
    Interrompe a instância EC2 Betfair Scraper

#### StartEC2OMQBScraper
    Acionada via SNS Topic A
    Inicializa a instância EC2 OMQB Scraper

#### StopEC2OMQBScraper
    
    Acionada via SNS Topic B
    Interrompe a instância EC2 OMQB Scraper


# Diagrama de Arquitetura da Solução


``` mermaid
graph TD;

A[Betfair-Scraper EC2] -->|Publica Resultado| B[S3 Bucket: betfair-results]
A -->|Envia Notificação| C[SNS: betfair-finished]
C -->|Aciona| D[Lambda: start-omqb-instance]
C -->|Aciona| G[Lambda: stop-betfair-instance]
G -->|Interrompe| A 
D -->|Start| E[OMQB-Scraper EC2]
E -->|Envia Notificação| H[SNS: omqb-finished]
H -->|Aciona| I[Lambda: stop-omqb-instance]
I -->|Interrompe| E
E -->|Lê Dados| B
E -->|Lê Cache| F
E -->|Salva Resultado e HTML| F[S3 Bucket: omqb-results]
J[EventBrigde] -->|Tarefa CRON diária 6AM UTC-3| A
```