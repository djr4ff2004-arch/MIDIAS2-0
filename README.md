# Telegram VIP Media Bot

Um bot completo e funcional para Telegram, projetado para automatizar a busca, download e distribuição de mídias de criadores em canais VIP e FREE. O bot é altamente customizável, suporta múltiplos idiomas e está pronto para deploy em plataformas como Railway, Render ou Fly.io.

---

## 🚀 Funcionalidades

- **Busca de Mídia**: Busca mídias de modelos/criadores em fontes configuráveis (ex: `coomer.st`, `picazor.com`).
- **Download Automático**: Baixa todas as mídias encontradas para o ambiente local.
- **Upload para Canal VIP**: Envia o conteúdo completo para um canal exclusivo para assinantes.
- **Geração de Prévias**: Cria prévias das mídias (blur, watermark ou baixa resolução) para canais públicos.
- **Canais FREE Multi-idioma**: Publica as prévias em 3 canais diferentes (🇧🇷 PT, 🇪🇸 ES, 🇺🇸 EN).
- **Link de Assinatura**: Direciona usuários dos canais FREE para um bot de assinatura externo.
- **Administração via Telegram**: Permite que o administrador configure tudo através de comandos simples.
- **Estrutura Modular**: Código organizado, limpo e fácil de manter.
- **Pronto para Deploy**: Inclui `Procfile` e suporte a variáveis de ambiente (`.env`) para deploy simplificado.

## 📁 Estrutura do Projeto

O projeto é organizado de forma modular para facilitar a manutenção e expansão.

```
/telegram-vip-bot
├── /bot
│   ├── main.py          # Ponto de entrada principal do bot
│   ├── config.py        # Gerenciador de configurações
│   ├── admin.py         # Comandos de administração
│   ├── fetcher.py       # Módulo de busca e download de mídias
│   ├── uploader.py      # Módulo de upload para o Telegram
│   ├── preview.py       # Gerador de prévias
│   ├── languages.py     # Suporte a múltiplos idiomas
│   └── users.py         # Gerenciamento de usuários (futuro)
├── requirements.txt     # Dependências do Python
├── .env.example         # Arquivo de exemplo para variáveis de ambiente
├── Procfile             # Configuração para deploy (Railway, Render)
├── .gitignore           # Arquivos a serem ignorados pelo Git
└── README.md            # Este arquivo
```

## ⚙️ Instalação e Configuração

Siga os passos abaixo para configurar e executar o bot localmente.

### 1. Pré-requisitos

- Python 3.11+
- Git
- Um bot criado no Telegram via [@BotFather](https://t.me/BotFather)

### 2. Clone o Repositório

```bash
git clone <URL_DO_SEU_REPOSITORIO>
cd telegram-vip-bot
```

### 3. Crie um Ambiente Virtual

```bash
python3 -m venv venv
source venv/bin/activate  # No Windows: venv\Scripts\activate
```

### 4. Instale as Dependências

```bash
pip install -r requirements.txt
```

### 5. Configure as Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto, copiando o `.env.example`.

```bash
cp .env.example .env
```

Agora, edite o arquivo `.env` com suas informações:

- `BOT_TOKEN`: Token do seu bot obtido no @BotFather.
- `ADMIN_ID`: Seu ID de usuário do Telegram. Você pode obtê-lo com o bot [@userinfobot](https://t.me/userinfobot).
- `VIP_CHANNEL_ID`: ID do seu canal VIP. **Importante**: O bot precisa ser administrador do canal. O ID deve ser no formato `-100xxxxxxxxxx`.
- `FREE_CHANNEL_PT_ID`, `FREE_CHANNEL_ES_ID`, `FREE_CHANNEL_EN_ID`: IDs dos seus canais FREE. O bot também precisa ser administrador.
- `SUB_BOT_LINK`: Link para o seu bot de assinaturas (ex: `https://t.me/SeuBotDeAssinatura`).
- `MEDIA_SOURCES`: URLs das fontes de mídia, separadas por vírgula.
- `PREVIEW_TYPE`: Tipo de prévia (`blur`, `watermark` ou `lowres`).

### 6. Execute o Bot

```bash
python bot/main.py
```

## 🤖 Comandos do Bot

### Comandos para Usuários

- `/search <nome_do_modelo>`: Inicia a busca por mídias de um criador.
- `/help`: Mostra a mensagem de ajuda.

### Comandos de Administrador

- `/setvip <id>`: Define o ID do canal VIP.
- `/setfreept <id>`: Define o ID do canal FREE em Português.
- `/setfreees <id>`: Define o ID do canal FREE em Espanhol.
- `/setfreeen <id>`: Define o ID do canal FREE em Inglês.
- `/setsubbot <link>`: Define o link do bot de assinatura.
- `/setsource <url1,url2>`: Define as fontes de mídia.
- `/setpreview <tipo>`: Altera o tipo de prévia (`blur`, `watermark`, `lowres`).
- `/setlang <pt|es|en>`: Define o idioma padrão das mensagens do bot.
- `/stats`: Mostra as configurações e estatísticas atuais.
- `/restart`: Reinicia o bot (útil em ambientes de deploy).

## 🚀 Deploy

Este projeto está pronto para deploy em plataformas que suportam buildpacks do Python (como Heroku, Railway, Render).

### Deploy no Railway

1. **Crie uma conta** no [Railway](https://railway.app/).
2. **Faça o push do código** para um repositório no GitHub.
3. **Crie um novo projeto** no Railway e conecte-o ao seu repositório do GitHub.
4. **Adicione as variáveis de ambiente**: Vá para a aba "Variables" do seu projeto no Railway e adicione todas as variáveis do seu arquivo `.env`.
5. **Configure o comando de início**: O Railway deve detectar o `Procfile` automaticamente e usar `worker: python bot/main.py` como comando de início. Se não, adicione-o manualmente nas configurações de deploy.

O Railway irá instalar as dependências do `requirements.txt` e iniciar o bot automaticamente.

## 📝 Licença

Este projeto é distribuído sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.
