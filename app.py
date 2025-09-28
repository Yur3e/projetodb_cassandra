"""
Aplicação em Flask para o projeto de banco de dados Cassandra com autenticação, 
perfis e histórico de visualização.
Banco de dados: Cassandra (DataStax via console CQL).
"""

from flask import Flask, render_template, request, redirect, url_for, session as flask_session, g
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid

# ==============================
# CONFIGURAÇÃO DE BANCO DE DADOS
# ==============================
cloud_config = {
    'secure_connect_bundle': 'secure-connect-cql-testando.zip'
}

# Credenciais do cluster Cassandra
CLIENT_ID = "KEY_PESSOAL"
CLIENT_SECRET = "KEY_PESSOAL"
KEYSPACE = 'NOME_PESSOAL'

# ======================
# CONFIGURAÇÃO DO FLASK
# ======================
app = Flask(__name__)
app.secret_key = "supersecretkey"  # Necessário para sessões seguras


def conectar():
    """
    Cria uma conexão com o cluster Cassandra.
    
    Returns:
        tuple: (cluster, session) objetos necessários para executar queries.
    """
    auth_provider = PlainTextAuthProvider(username=CLIENT_ID, password=CLIENT_SECRET)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect(KEYSPACE)
    return cluster, session


# ===============================
#  GERENCIADOR DE USUÁRIO GLOBAL
# ===============================
@app.before_request
def carregar_usuario():
    """
    Executado antes de cada requisição.
    Carrega o usuário autenticado (se existir) e o atribui ao objeto `g`.
    """
    g.user = None
    if "account_id" in flask_session:
        cluster, session = conectar()
        conta = session.execute(
            "SELECT account_id, nome, email, pais, idade, data_criacao FROM contas WHERE account_id = %s",
            [uuid.UUID(flask_session["account_id"])]
        ).one()
        cluster.shutdown()
        g.user = conta


@app.context_processor
def inject_user():
    """
    Torna o usuário (`g.user`) disponível em todos os templates Jinja.
    """
    return dict(user=g.user)


# ==========
# ROTAS WEB
# ==========
@app.route("/")
def index():
    """
    Página inicial.
    Se o usuário não estiver logado, redireciona para o login.
    Caso contrário, envia para a seleção de perfis.
    """
    if not g.user:
        return redirect(url_for("login"))
    return redirect(url_for("conta"))


@app.route("/login", methods=["GET", "POST"])
def login():
    """
    Página de login.
    - GET: renderiza formulário de login.
    - POST: valida credenciais e cria sessão.
    """
    if request.method == "POST":
        email = request.form["email"]
        senha = request.form["senha"]

        cluster, session = conectar()
        query = "SELECT account_id, nome, email, senha FROM contas WHERE email = %s ALLOW FILTERING"
        conta = session.execute(query, [email]).one()
        cluster.shutdown()

        if conta and conta.senha == senha:
            flask_session["account_id"] = str(conta.account_id)
            return redirect(url_for("conta"))
        else:
            return render_template("login.html", erro="E-mail ou senha incorretos")

    return render_template("login.html")


@app.route("/registro", methods=["GET", "POST"])
def registro():
    """
    Página de registro de novos usuários.
    - Verifica se o e-mail já existe.
    - Cria nova conta se não houver conflito.
    """
    if request.method == "POST":
        nome = request.form["nome"]
        email = request.form["email"]
        senha = request.form["senha"]
        pais = request.form["pais"]
        idade = int(request.form["idade"])

        cluster, session = conectar()
        query = "SELECT email FROM contas WHERE email = %s ALLOW FILTERING"
        existente = session.execute(query, [email]).one()

        if existente:
            cluster.shutdown()
            return render_template("registro.html", erro="E-mail já cadastrado")

        account_id = uuid.uuid4()

        session.execute(
            """
            INSERT INTO contas (account_id, nome, email, senha, pais, idade, data_criacao)
            VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now()))
            """,
            [account_id, nome, email, senha, pais, idade]
        )
        cluster.shutdown()
        return redirect(url_for("login"))

    return render_template("registro.html")


@app.route("/logout")
def logout():
    """
    Finaliza a sessão do usuário.
    """
    flask_session.clear()
    return redirect(url_for("login"))


@app.route("/conta")
def conta():
    """
    Página de seleção de perfis de uma conta.
    """
    if not g.user:
        return redirect(url_for("login"))

    cluster, session = conectar()
    account_id = uuid.UUID(flask_session["account_id"])
    perfis = session.execute(
        "SELECT user_id, nome FROM perfis WHERE account_id = %s", [account_id]
    )
    cluster.shutdown()
    return render_template("selecao_perfis.html", perfis=perfis)


@app.route("/perfil/<user_id>")
def perfil(user_id):
    """
    Página de histórico de visualização de um perfil específico.
    
    Args:
        user_id (str): ID do perfil selecionado.
    """
    if not g.user:
        return redirect(url_for("login"))

    cluster, session = conectar()
    account_id = uuid.UUID(flask_session["account_id"])

    # Queries preparadas para eficiência
    get_history_query = session.prepare(
        "SELECT video_id, tempo_assistido, data_visualizacao FROM historico_visualizacao WHERE account_id = ? AND user_id = ?"
    )
    get_video_query = session.prepare("SELECT titulo FROM videos WHERE video_id = ?")

    # Carrega histórico de visualizações
    historico_db = session.execute(get_history_query, [account_id, uuid.UUID(user_id)])
    historico = []
    for item in historico_db:
        video = session.execute(get_video_query, [item.video_id]).one()
        historico.append({
            "titulo": video.titulo if video else "Vídeo não encontrado",
            "tempo": round(item.tempo_assistido / 60, 2),  # minutos, com 2 casas decimais
            "data": item.data_visualizacao
        })

    # Nome do perfil (ALLOW FILTERING) --> OBS: Talvez mudar para otimizar
    perfil_nome = session.execute(
        "SELECT nome FROM perfis WHERE user_id=%s ALLOW FILTERING",
        [uuid.UUID(user_id)]
    ).one()
    perfil = {"user_id": user_id, "nome": perfil_nome.nome if perfil_nome else "Perfil não encontrado"}

    cluster.shutdown()
    return render_template("perfil.html", perfil=perfil, historico=historico)


# ======================
# INICIALIZAÇÃO DA APP
# ======================
if __name__ == "__main__":
    app.run(debug=True)
