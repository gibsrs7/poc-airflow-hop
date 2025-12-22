from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# IMPORTANTE: Importar a classe Secret
from airflow.providers.cncf.kubernetes.secret import Secret 
from kubernetes.client import models as k8s
from datetime import datetime

# --- CONFIGURAÇÕES ---
REPO_GIT = "https://github.com/gibsrs7/poc-airflow-hop.git"
BRANCH = "main"
PASTA_PROJETO = "hop/estudos-hop"
PIPELINE_HOP = "hop/estudos-hop/airflow-hop.hpl"
SCRIPT_R = "scripts/coleta-deputados/coleta_api_deputados.R"

# --- DEFINIÇÃO DOS SEGREDOS (A Mágica acontece aqui) ---
# Secret('env', 'NOME_VAR_ENV', 'NOME_DO_COFRE_K8S', 'CHAVE_NO_COFRE')

# 1. Vai criar a variável de ambiente DB_PASS pegando do secret 'oracle-secrets' chave 'password'
secret_db_pass = Secret('env', 'DB_PASS', 'oracle-secrets', 'password')

# 2. Vai criar a variável de ambiente DB_USER pegando do secret 'oracle-secrets' chave 'username'
secret_db_user = Secret('env', 'DB_USER', 'oracle-secrets', 'username')

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
    args=[f"git clone {REPO_GIT} --branch {BRANCH} --single-branch /repo"]
)

with DAG(
    '07_Pipeline_Completo_Prod_Secrets',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['poc', 'etl', 'prod']
) as dag:

    # Task R (Sem mudanças de senha, pois não usa banco)
    task_coleta_r= KubernetesPodOperator(
        task_id="coleta_deputados",
        name="pod-coleta-deputados",
        namespace="airflow", 
        image="gibsonr7/coleta-r:v1",

        # Inicando o container git

        init_containers=[container_git],

        volumes=[vol_codigo,vol_dados],
        volume_mounts=[mount_codigo,mount_dados],

        # Comando: Rscript -e "print(...)"
        cmds=["Rscript", f"/repo/{SCRIPT_R}"],
        is_delete_operator_pod=False, # Deleta o pod ao final para não sujar o cluster
        get_logs=True, # Tenta pegar o log para mostrar na UI
        image_pull_policy="Always" # Garante que baixa a imagem se não tiver
    )

    # Task Hop (Agora usando Secrets)
    task_hop = KubernetesPodOperator(
        task_id="executar_hop",
        name="pod-hop",
        namespace="airflow",
        image="gibsonr7/hop:v1",

        init_containers=[container_git], 

        volumes=[vol_codigo, vol_dados],
        volume_mounts=[mount_codigo, mount_dados],
        
        # --- AQUI: Injetamos os objetos Secret ---
        secrets=[secret_db_pass, secret_db_user],
        
        # --- Variáveis normais (não secretas) continuam aqui ---
        env_vars={
            "HOP_METADATA_FOLDER": f"/repo/{PASTA_PROJETO}/metadata",
            "DB_HOST": "host.docker.internal", # Em PROD real, isso seria um IP ou URL da AWS/Azure
            "DIR_DADOS": "/dados"
        },
        # Nota: DB_USER e DB_PASS foram removidos do env_vars acima 
        # porque agora são injetados via parâmetro 'secrets'

        cmds=[
            "/bin/bash", "-c",
            f"""
            echo "--- Registrando Projeto ---" && \
            /opt/hop/hop-conf.sh \
                --project="projeto-etl" \
                --project-home="/repo/{PASTA_PROJETO}" \
                --project-create && \
            
            echo "--- Executando Pipeline ---" && \
            /opt/hop/hop-run.sh \
                -j "projeto-etl" \
                -f "/repo/projeto/{PASTA_PROJETO}/{PIPELINE_HOP}" \
                -l Basic
            """
        ],
        
        is_delete_operator_pod=False,
        get_logs=True
    )

    task_r >> task_hop