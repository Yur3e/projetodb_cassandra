from flask import Flask, render_template, request, redirect, url_for, session as flask_session, g
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import os, shutil

# ==============================
# CONFIGURAÇÃO DE BANCO DE DADOS
# ==============================
cloud_config = {
    'secure_connect_bundle': 'secure-connect-cql-testando.zip'
}

CLIENT_ID = "rQRuyUNYDzfZYLDcorLJjhjF"
CLIENT_SECRET = "v4CHnndgDK315WLNkfwdYs.TX_cGcCZ8Ekn41.JSHAt8A6.H.cmSauHIjdPJhe4JsnKZnUYDkUzkuwFwegXZ6hNT7rfGU9CEBdp+RWLddKYNYi,OshnfRROAEcgifW,6"
KEYSPACE = 'streaming_data'

# ======================
# CONFIGURAÇÃO DO FLASK
# ======================
app = Flask(__name__)
app.secret_key = "supersecretkey"


def conectar():
    """
    Conecta ao cluster Cassandra e retorna (cluster, session).
    """
    auth_provider = PlainTextAuthProvider(username=CLIENT_ID, password=CLIENT_SECRET)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect(KEYSPACE)
    return cluster, session


# ==============================
# CARREGAR USUÁRIO ANTES DA REQ.
# ==============================
@app.before_request
def carregar_usuario():
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
    return dict(user=g.user)


# ======================
#          ROTAS
# ======================
@app.route("/")
def index():
    if not g.user:
        return redirect(url_for("login"))
    return redirect(url_for("conta"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form["email"]
        senha = request.form["senha"]

        cluster, session = conectar()
        # Agora busca na tabela contas_por_email (sem ALLOW FILTERING)
        conta = session.execute(
            "SELECT account_id, nome, email, senha FROM contas_por_email WHERE email = %s",
            [email]
        ).one()
        cluster.shutdown()

        if conta and conta.senha == senha:
            flask_session["account_id"] = str(conta.account_id)
            return redirect(url_for("conta"))
        else:
            return render_template("login.html", erro="E-mail ou senha incorretos")

    return render_template("login.html")


@app.route("/registro", methods=["GET", "POST"])
def registro():
    if request.method == "POST":
        nome = request.form["nome"]
        email = request.form["email"]
        senha = request.form["senha"]
        pais = request.form["pais"]
        idade = int(request.form["idade"])

        cluster, session = conectar()

        # Verifica se já existe no contas_por_email
        existente = session.execute(
            "SELECT email FROM contas_por_email WHERE email = %s",
            [email]
        ).one()

        if existente:
            cluster.shutdown()
            return render_template("registro.html", erro="E-mail já cadastrado")

        # cria conta
        account_id = uuid.uuid4()
        session.execute(
            """
            INSERT INTO contas (account_id, nome, email, senha, pais, idade, data_criacao)
            VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now()))
            """,
            [account_id, nome, email, senha, pais, idade]
        )

        # Contas_por_email (Otimizar login)
        session.execute(
            """
            INSERT INTO contas_por_email (email, account_id, nome, senha)
            VALUES (%s, %s, %s, %s)
            """,
            [email, account_id, nome, senha]
        )

        # Criando perfil padrão com avatar
        user_id = uuid.uuid4()
        avatar_filename = f"{user_id}.png"   # nome do arquivo baseado no user_id
        default_avatar = "default.png"       # imagem base em static/images/perfis

        src = os.path.join("static", "images", "perfis", default_avatar)
        dst = os.path.join("static", "images", "perfis", avatar_filename)
        shutil.copy(src, dst)

        session.execute(
            """
            INSERT INTO perfis (user_id, account_id, nome, avatar)
            VALUES (%s, %s, %s, %s)
            """,
            [user_id, account_id, nome, f"images/perfis/{avatar_filename}"]
        )

        cluster.shutdown()
        return redirect(url_for("login"))

    return render_template("registro.html")


@app.route("/logout")
def logout():
    flask_session.clear()
    return redirect(url_for("login"))


@app.route("/conta")
def conta():
    if not g.user:
        return redirect(url_for("login"))

    cluster, session = conectar()
    account_id = uuid.UUID(flask_session["account_id"])
    perfis = session.execute(
        "SELECT user_id, nome, avatar FROM perfis WHERE account_id = %s", [account_id]
    )
    cluster.shutdown()
    return render_template("selecao_perfis.html", perfis=perfis)


@app.route("/perfil/<user_id>")
def perfil(user_id):
    if not g.user:
        return redirect(url_for("login"))

    cluster, session = conectar()
    account_id = uuid.UUID(flask_session["account_id"])

    get_history_query = session.prepare(
        "SELECT video_id, tempo_assistido, data_visualizacao FROM historico_visualizacao WHERE account_id = ? AND user_id = ?"
    )
    get_video_query = session.prepare("SELECT titulo FROM videos WHERE video_id = ?")

    historico_db = session.execute(get_history_query, [account_id, uuid.UUID(user_id)])
    historico = []
    for item in historico_db:
        video = session.execute(get_video_query, [item.video_id]).one()
        historico.append({
            "titulo": video.titulo if video else "Vídeo não encontrado",
            "tempo": round(item.tempo_assistido / 60, 2),
            "data": item.data_visualizacao
        })

    perfil_row = session.execute(
        "SELECT nome, avatar FROM perfis WHERE user_id=%s ALLOW FILTERING",
        [uuid.UUID(user_id)]
    ).one()
    perfil = {
        "user_id": user_id,
        "nome": perfil_row.nome if perfil_row else "Perfil não encontrado",
        "avatar": perfil_row.avatar if perfil_row else "images/perfis/default.png"
    }

    cluster.shutdown()
    return render_template("perfil.html", perfil=perfil, historico=historico)


# ======================
#       RODAR APP
# ======================
if __name__ == "__main__":
    app.run(debug=True)
