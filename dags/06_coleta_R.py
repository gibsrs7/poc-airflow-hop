from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

# --- CONFIGURAÇÕES DO PROJETO ---
REPO_GIT = "https://github.com/gibsrs7/poc-airflow-hop.git"
BRANCH = "main"
PASTA_PROJETO = "scripts/coleta-deputados" # Nome da pasta dentro do Git
NOME_SCRIPT = "coleta_api_deputados.R"   # Nome do arquivo do pipeline

# --------------------------------

# 1. Volume Temporário para o CÓDIGO (Git baixa aqui, R lê daqui)
vol_codigo = k8s.V1Volume(
    name="vol-codigo",
    empty_dir=k8s.V1EmptyDirVolumeSource()
)
mount_codigo = k8s.V1VolumeMount(
    name="vol-codigo",
    mount_path="/repo"
)

# 2. Volume persistente para os dados

vol_dados = k8s.V1Volume(
    name="vol-dados",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="pvc-airflow-dados" # O PVC que criamos
    )
)

mount_dados = k8s.V1VolumeMount(
    name="vol-dados",
    mount_path="/dados"
)

# ------ DEFINIÇÃO DO CONTAINER GIT -------

container_git = k8s.V1Container(
    name="git-clone",
    image="alpine/git",
    volume_mounts=[mount_codigo], # Só precisa montar o disco de código
    command=["/bin/sh", "-c"],
    # Clona para /repo/projeto
    args=[f"git clone {REPO_GIT} --branch {BRANCH} --single-branch /repo/"]
)


with DAG(
    '06_Coleta_R',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['poc', 'etl','R']
)
as dag:

task_coleta_r= KubernetesPodOperator(
        task_id="coleta_deputados",
        name="pod-coleta-deputados",
        namespace="airflow", 
        image="rocker/r-base:latest",

        # Inicando o container git

        init_containers=[container_git],

        volumes=[vol_codigo,vol_dados]
        volumes_mounts=[mount_codigo,mount_dados],

        # Comando: Rscript -e "print(...)"
        cmds=["Rscript", f"/repo/{PASTA_PROJETO}/{SCRIPT}"],
        is_delete_operator_pod=True, # Deleta o pod ao final para não sujar o cluster
        get_logs=True, # Tenta pegar o log para mostrar na UI
        image_pull_policy="Always" # Garante que baixa a imagem se não tiver
)


