# Configuração de Secrets para Docker Hub

Para que a GitHub Action funcione corretamente, você precisa configurar os seguintes secrets no seu repositório GitHub:

## Secrets Necessários

1. **DOCKER_USERNAME**: Seu nome de usuário do Docker Hub
2. **DOCKER_PASSWORD**: Sua senha ou token de acesso do Docker Hub

## Como Configurar

1. Acesse seu repositório no GitHub
2. Vá em **Settings** → **Secrets and variables** → **Actions**
3. Clique em **New repository secret**
4. Adicione cada secret:
   - Name: `DOCKER_USERNAME`, Value: seu_usuario_docker_hub
   - Name: `DOCKER_PASSWORD`, Value: sua_senha_ou_token_docker_hub

## Recomendações de Segurança

- Use um **Access Token** em vez da senha do Docker Hub
- Para criar um token: Docker Hub → Account Settings → Security → New Access Token
- O token deve ter permissões de **Read, Write, Delete**

## Funcionamento da Action

- **Push para main/master**: Constrói e publica a imagem com tag `latest`
- **Pull Requests**: Apenas constrói a imagem (não publica)
- **Releases**: Publica com tags semânticas (v1.0.0, v1.0, v1, latest)