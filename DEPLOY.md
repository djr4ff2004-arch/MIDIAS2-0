# Guia de Deploy - Telegram VIP Media Bot

Este documento fornece instruções detalhadas para fazer o deploy do bot em diferentes plataformas de hospedagem.

---

## 📋 Pré-requisitos

Antes de iniciar o deploy, certifique-se de ter:

1. **Bot do Telegram criado**: Obtenha o token através do [@BotFather](https://t.me/BotFather).
2. **Canais criados**: Crie os canais VIP e FREE (PT, ES, EN) no Telegram.
3. **Bot como administrador**: Adicione o bot como administrador em todos os canais com permissões de postar mensagens.
4. **IDs dos canais**: Obtenha os IDs dos canais (formato: `-100xxxxxxxxxx`). Você pode usar o bot [@RawDataBot](https://t.me/RawDataBot) para isso.
5. **Seu ID de usuário**: Obtenha seu ID através do [@userinfobot](https://t.me/userinfobot).

---

## 🚂 Deploy no Railway

O Railway é uma plataforma moderna e fácil de usar para deploy de aplicações.

### Passo 1: Preparar o Repositório

1. Crie um repositório no GitHub e faça o push do código:

```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin <URL_DO_SEU_REPOSITORIO>
git push -u origin main
```

### Passo 2: Criar Projeto no Railway

1. Acesse [Railway.app](https://railway.app/) e faça login com sua conta do GitHub.
2. Clique em **"New Project"** → **"Deploy from GitHub repo"**.
3. Selecione o repositório que você criou.
4. O Railway detectará automaticamente que é um projeto Python.

### Passo 3: Configurar Variáveis de Ambiente

1. No painel do projeto, vá para a aba **"Variables"**.
2. Adicione todas as variáveis do arquivo `.env.example`:

```
BOT_TOKEN=seu_token_aqui
ADMIN_ID=seu_id_aqui
VIP_CHANNEL_ID=-1001234567890
FREE_CHANNEL_PT_ID=-1001234567891
FREE_CHANNEL_ES_ID=-1001234567892
FREE_CHANNEL_EN_ID=-1001234567893
SUB_BOT_LINK=https://t.me/SeuBotDeAssinatura
MEDIA_SOURCES=https://coomer.st,https://picazor.com
PREVIEW_TYPE=blur
PREVIEW_QUALITY=50
MAX_FILES_PER_BATCH=10
AUTO_POST_INTERVAL=300
DEFAULT_LANG=pt
```

### Passo 4: Deploy

1. O Railway iniciará o deploy automaticamente após detectar o `Procfile`.
2. Aguarde o build e o deploy serem concluídos.
3. Verifique os logs para confirmar que o bot está rodando.

### Passo 5: Testar

1. Abra o Telegram e envie `/start` para o seu bot.
2. Teste o comando `/search <nome_do_modelo>`.

---

## 🎨 Deploy no Render

O Render é outra excelente opção para hospedar o bot.

### Passo 1: Criar Conta no Render

1. Acesse [Render.com](https://render.com/) e crie uma conta.
2. Conecte sua conta do GitHub.

### Passo 2: Criar um Web Service

1. No dashboard, clique em **"New +"** → **"Background Worker"**.
2. Conecte o repositório do GitHub.
3. Configure:
   - **Name**: `telegram-vip-bot`
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python bot/main.py`

### Passo 3: Adicionar Variáveis de Ambiente

1. Na seção **"Environment"**, adicione todas as variáveis do `.env.example`.

### Passo 4: Deploy

1. Clique em **"Create Background Worker"**.
2. O Render fará o build e iniciará o bot automaticamente.
3. Monitore os logs para verificar o status.

---

## ✈️ Deploy no Fly.io

O Fly.io é ideal para aplicações que precisam de baixa latência.

### Passo 1: Instalar o CLI do Fly.io

```bash
curl -L https://fly.io/install.sh | sh
```

### Passo 2: Fazer Login

```bash
flyctl auth login
```

### Passo 3: Criar um Dockerfile

Crie um arquivo `Dockerfile` na raiz do projeto:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot/ ./bot/

CMD ["python", "bot/main.py"]
```

### Passo 4: Inicializar o App

```bash
flyctl launch
```

Siga as instruções e escolha:
- **App name**: `telegram-vip-bot` (ou outro nome único)
- **Region**: Escolha a região mais próxima de você

### Passo 5: Configurar Variáveis de Ambiente

```bash
flyctl secrets set BOT_TOKEN=seu_token_aqui
flyctl secrets set ADMIN_ID=seu_id_aqui
flyctl secrets set VIP_CHANNEL_ID=-1001234567890
# Continue com todas as outras variáveis...
```

### Passo 6: Deploy

```bash
flyctl deploy
```

### Passo 7: Verificar Status

```bash
flyctl status
flyctl logs
```

---

## 🔧 Manutenção e Monitoramento

### Ver Logs

- **Railway**: Acesse a aba "Deployments" e clique em "View Logs".
- **Render**: Acesse a aba "Logs" no dashboard do serviço.
- **Fly.io**: Use `flyctl logs`.

### Reiniciar o Bot

- **Railway**: Clique em "Restart" no dashboard.
- **Render**: Clique em "Manual Deploy" → "Clear build cache & deploy".
- **Fly.io**: Use `flyctl restart`.

### Atualizar o Código

1. Faça as alterações no código local.
2. Commit e push para o GitHub:

```bash
git add .
git commit -m "Descrição das alterações"
git push
```

3. O Railway e o Render farão o redeploy automaticamente.
4. No Fly.io, execute `flyctl deploy` novamente.

---

## ⚠️ Solução de Problemas

### Bot não responde

1. Verifique se o `BOT_TOKEN` está correto.
2. Confirme que o bot está rodando nos logs.
3. Teste com `/start` diretamente no chat privado com o bot.

### Erro ao enviar para canais

1. Verifique se os IDs dos canais estão corretos (formato `-100xxxxxxxxxx`).
2. Confirme que o bot é administrador dos canais.
3. Verifique se o bot tem permissões para postar mensagens.

### Download de mídias falha

1. Verifique se as URLs das fontes estão corretas.
2. Algumas fontes podem ter mudado a estrutura HTML. Nesse caso, ajuste os seletores CSS em `fetcher.py`.

### Limite de taxa do Telegram

O bot já possui proteção contra rate limiting, mas se você estiver enviando muitas mensagens:

1. Aumente o intervalo entre uploads em `AUTO_POST_INTERVAL`.
2. Reduza `MAX_FILES_PER_BATCH`.

---

## 📞 Suporte

Se você encontrar problemas ou tiver dúvidas:

1. Verifique os logs da aplicação.
2. Revise a documentação do Telegram Bot API: https://core.telegram.org/bots/api
3. Consulte a documentação da plataforma de hospedagem escolhida.

---

**Desenvolvido com ❤️ para automação de conteúdo no Telegram**
