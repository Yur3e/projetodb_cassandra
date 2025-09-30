from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid

cloud_config = {
    'secure_connect_bundle': 'secure-connect-cql-testando.zip'
}

CLIENT_ID = "rQRuyUNYDzfZYLDcorLJjhjF"
CLIENT_SECRET = "v4CHnndgDK315WLNkfwdYs.TX_cGcCZ8Ekn41.JSHAt8A6.H.cmSauHIjdPJhe4JsnKZnUYDkUzkuwFwegXZ6hNT7rfGU9CEBdp+RWLddKYNYi,OshnfRROAEcgifW,6"
KEYSPACE = 'streaming_data'

def conectar():
    auth_provider = PlainTextAuthProvider(username=CLIENT_ID, password=CLIENT_SECRET)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect(KEYSPACE)
    return cluster, session

# Conectar
cluster, session = conectar()

# Buscar todos os registros da tabela antiga
rows = session.execute("SELECT account_id, nome, email, senha FROM contas")

# Inserir na nova tabela
for row in rows:
    session.execute(
        """
        INSERT INTO contas_por_email (email, account_id, nome, senha)
        VALUES (%s, %s, %s, %s)
        """,
        [row.email, row.account_id, row.nome, row.senha]
    )

print("=====Migração concluída=====")
cluster.shutdown()
