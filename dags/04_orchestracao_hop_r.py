from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime


with DAG(
    '04_orchestracao_hop_r',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['poc', 'hop', 'r', 'k8s']
) as dag:

    # --- TAREFA 1: EXECUTAR SCRIPT R ---
    # O Airflow cria um pod isolado só para isso.
    # Usamos a imagem "rocker/r-base" que já vem com R pronto.
    tarefa_r = KubernetesPodOperator(
        task_id="analise_r",
        name="pod-analise-r",
        namespace="airflow", 
        image="rocker/r-base:latest",
        # Comando: Rscript -e "print(...)"
        cmds=["Rscript", "-e", "print('SUCESSO: O R esta rodando via KubernetesPodOperator!')"],
        is_delete_operator_pod=True, # Deleta o pod ao final para não sujar o cluster
        get_logs=True, # Tenta pegar o log para mostrar na UI
        image_pull_policy="Always" # Garante que baixa a imagem se não tiver
    )

    # --- TAREFA 2: EXECUTAR APACHE HOP ---
    # O Airflow cria outro pod isolado.
    # Usamos a imagem "apache/hop" que já vem com Java e Hop prontos.
    # --- TAREFA 2: EXECUTAR APACHE HOP ---
    tarefa_hop = KubernetesPodOperator(
        task_id="workflow_hop",
        name="pod-workflow-hop",
        namespace="airflow",
        image="apache/hop:2.11.0",
        # CORREÇÃO: Vamos rodar direto. Se falhar, queremos ver o VERMELHO.
        cmds=["/bin/bash", "-c", "/opt/hop/hop-run.sh --version"], 
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always"
    )

    # Define a ordem: Primeiro o R, depois o Hop
    tarefa_r >> tarefa_hop