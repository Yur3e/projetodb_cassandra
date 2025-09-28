# Simulador de Histórico de visualização de Streaming com Flask e Cassandra

Este projeto é uma aplicação web desenvolvida em **Flask** que simula uma plataforma de streaming de vídeo.  
A aplicação utiliza o **Apache Cassandra**, através do serviço **DataStax Astra DB**, para gerenciar contas de usuários, perfis e históricos de visualização.

---

## Funcionalidades

- **Autenticação de Usuários**: Registro, login e logout.  
- **Gerenciamento de Contas**: Cada conta pode possuir múltiplos perfis.  
- **Seleção de Perfis**: Usuários podem escolher um perfil específico para acessar o conteúdo.  
- **Histórico de Visualização**: Cada perfil mantém seu próprio histórico de vídeos assistidos.  

---

## Tecnologias Utilizadas

- **Backend**: Flask  
- **Banco de Dados**: Apache Cassandra (DataStax Astra DB)  
- **Linguagem**: Python 3.8
- **Driver do Banco de Dados**: cassandra-driver  

---

## Pré-requisitos

Para executar este projeto localmente, é necessário ter os seguintes itens instalados:

- **Python 3.8.10** (versão recomendada no DataStax).  
  - O projeto foi desenvolvido e testado nesta versão.  
  - O uso de versões muito mais recentes ou antigas pode gerar incompatibilidades.  

- **Conta no DataStax Astra** para criação do banco de dados Cassandra.  

---
