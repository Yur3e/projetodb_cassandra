from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# ====== REQUISITOS QUE USEI NO MEU VENV ======
# Requisito 1: python 3.8.10
# Requisito 2: pip install cassandra-driver==3.24.0

# --- 1. CONFIGURAÇÃO DA CONEXÃO ---
cloud_config = {
    'secure_connect_bundle': 'secure-connect-cql-testando.zip'
}
CLIENT_ID = "rQRuyUNYDzfZYLDcorLJjhjF"
CLIENT_SECRET = "v4CHnndgDK315WLNkfwdYs.TX_cGcCZ8Ekn41.JSHAt8A6.H.cmSauHIjdPJhe4JsnKZnUYDkUzkuwFwegXZ6hNT7rfGU9CEBdp+RWLddKYNYi,OshnfRROAEcgifW,6"

KEYSPACE = 'streaming_data'

# --- 2. FUNÇÃO PRINCIPAL PARA CONECTAR E PRINTAR ---
def consultar_dados():
    cluster = None
    try:
        auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)
        print("--- Conexão bem-sucedida ---")

        # Queries com reutilização ('?' placeholder) --> Acessando as tabelas
        get_profiles_query = session.prepare("SELECT user_id, nome FROM perfis WHERE account_id = ?")
        get_history_query = session.prepare("SELECT video_id, tempo_assistido, data_visualizacao FROM historico_visualizacao WHERE account_id = ? AND user_id = ?")
        get_video_query = session.prepare("SELECT titulo FROM videos WHERE video_id = ?")

        # Busca todas as contas na tabela 'accounts'
        accounts = session.execute("SELECT account_id, nome, email FROM contas")

        print("\n--- Relatório de Visualização de Contas ---")
        
        # Itera sobre cada conta encontrada
        for account in accounts:
            print(f"\n[+] Conta: {account.nome} ({account.email})")
            
            # Para cada conta, busca os perfis
            profiles = session.execute(get_profiles_query, [account.account_id])
            
            if not profiles:
                print("  -> Nenhum perfil encontrado para esta conta.")
                continue

            # Itera sobre cada perfil da conta
            for profile in profiles:
                print(f"  [>] Perfil: {profile.nome}")
                
                # Para cada perfil, busca o histórico de visualização
                historico = session.execute(get_history_query, [account.account_id, profile.user_id])
                
                if not historico:
                    print("    - Nenhum histórico de visualização.")
                    continue
                
                # Itera sobre cada item do histórico, mostrando o nome do vídeo
                for item in historico:
                    # Este é o "join" do lado do cliente: buscamos o título do vídeo usando o video_id
                    video = session.execute(get_video_query, [item.video_id]).one()
                    titulo_video = video.titulo if video else "Vídeo não encontrado"
                    
                    print(f"    - Assistiu a '{titulo_video}' (Tempo: {item.tempo_assistido}s em {item.data_visualizacao})")

    except Exception as e:
        print(f"\nOcorreu um erro: {e}")

    finally:
        # Garante que a conexão seja sempre fechada
        if cluster and not cluster.is_shutdown:
            cluster.shutdown()
            print("\n--- Conexão fechada ---")

# --- 3. EXECUTAR O SCRIPT ---
if __name__ == "__main__":
    consultar_dados()