from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Definindo a DAG
with DAG(
    '03_inventario_ambiente',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['poc', 'diagnostico']
) as dag:

    # 1. Verifica se tem Python e qual versão (Geralmente tem)
    check_python = BashOperator(
        task_id='verificar_python',
        bash_command='python --version',
    )

    # 2. Verifica se tem Java (Essencial para o Apache Hop)
    check_java = BashOperator(
        task_id='verificar_java',
        bash_command='java -version 2>&1 || echo "Java NAO instalado"', 
        # O "||" garante que a tarefa não falhe se o Java não existir, apenas avisa.
    )

    # 3. Verifica se tem R (Para seus scripts de análise)
    check_r = BashOperator(
        task_id='verificar_R',
        bash_command='R --version || echo "R NAO instalado"',
    )

    # 4. Teste de Internet (Tenta acessar o Google ou o site da Câmara)
    check_internet = BashOperator(
        task_id='verificar_internet',
        bash_command='curl -I https://dadosabertos.camara.leg.br/api/v2/deputados || echo "Sem Internet"',
    )

    # Define a ordem (opcional, podem rodar em paralelo)
    [check_python, check_java, check_r, check_internet]