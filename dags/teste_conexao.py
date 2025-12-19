from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Essa é a definição básica de uma DAG
with DAG(
    '01_teste_conexao_git',  # Nome que vai aparecer na tela
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Não roda sozinho, só manual
    catchup=False,
    tags=['poc', 'teste']
) as dag:

    # Uma tarefa simples que imprime uma mensagem
    t1 = BashOperator(
        task_id='diga_ola',
        bash_command='echo "Se voce esta lendo isso, o GitSync funcionou!"'
    )