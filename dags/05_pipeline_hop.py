from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

# --- CONFIGURAÇÕES DO PROJETO ---
REPO_GIT = "https://github.com/gibsrs7/poc-airflow-hop.git"
BRANCH = "main"
PASTA_PROJETO = "estudos-hop" # Nome da pasta dentro do Git
NOME_PIPELINE = "dummy.hpl"   # Nome do arquivo do pipeline
# --------------------------------

# Definindo o Volume Compartilhado (O "Disco" temporário)
# É aqui que o Git vai salvar e o Hop vai ler.
volume_compartilhado = k8s.V1Volume(
    name="volume-codigo-projeto",
    empty_dir=k8s.V1EmptyDirVolumeSource()
)

mount_volume = k8s.V1VolumeMount(
    name="volume-codigo-projeto",
    mount_path="/dados_projeto" # Onde a pasta vai aparecer dentro do Linux
)

# Definindo o Init Container (O "Baixador" de código)
# Ele usa uma imagem leve (alpine/git) só para clonar o repo.
init_git_clone = k8s.V1Container(
    name="git-clone-init",
    image="alpine/git:latest",
    volume_mounts=[mount_volume],
    # Comando: Clona o repo para dentro da pasta /dados_projeto
    command=["/bin/sh", "-c"],
    args=[f"git clone {REPO_GIT} --branch {BRANCH} --single-branch /dados_projeto/repo"]
)

with DAG(
    '05_pipeline_prof_hop_gitops',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['poc', 'hop', 'gitops', 'producao']
) as dag:

    # A Tarefa Principal
    run_hop_project = KubernetesPodOperator(
        task_id="executar_pipeline_projeto",
        name="pod-hop-gitops",
        namespace="airflow",
        image="apache/hop:2.11.0",
        
        # Aqui conectamos o Init Container que criamos acima
        init_containers=[init_git_clone],
        
        # Aqui conectamos o volume para o Hop poder ver os arquivos baixados
        volumes=[volume_compartilhado],
        volume_mounts=[mount_volume],
        
        # O Comando de Execução do Hop (Referenciando o caminho do volume)
        # -j: Aponta para a pasta do projeto (carrega metadata e config)
        # -f: Aponta para o arquivo do pipeline
        cmds=[
            "/bin/bash", 
            "-c", 
            """
            echo "=== RODANDO HOP EM MODO DIRETO ===" && \
            /opt/hop/hop-run.sh \
            -f /dados_projeto/repo/estudos-hop/dummy.hpl \
            -l Basic
            """
        ],
        
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always"
    )