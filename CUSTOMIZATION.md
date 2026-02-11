# Guia de Customização

Este documento explica como customizar e estender o bot para atender às suas necessidades específicas.

---

## 🎨 Customização de Prévias

O bot oferece três tipos de prévias que podem ser configurados através do comando `/setpreview` ou da variável de ambiente `PREVIEW_TYPE`.

### Tipos Disponíveis

#### 1. Blur (Desfoque)

Aplica um desfoque gaussiano intenso na imagem.

**Configuração:**
```bash
/setpreview blur
```

**Personalizar intensidade:**
Edite o arquivo `bot/preview.py`, linha com `GaussianBlur(radius=20)`:
```python
blurred = img.filter(ImageFilter.GaussianBlur(radius=30))  # Aumenta o desfoque
```

#### 2. Watermark (Marca d'água)

Adiciona uma marca d'água no centro da imagem.

**Configuração:**
```bash
/setpreview watermark
```

**Personalizar texto:**
Edite o arquivo `bot/preview.py`, linha com `watermark_text`:
```python
watermark_text = "SEU TEXTO AQUI"
```

**Personalizar posição:**
Altere as coordenadas `x` e `y` no código:
```python
# Centro (padrão)
x = (width - text_width) // 2
y = (height - text_height) // 2

# Canto inferior direito
x = width - text_width - 20
y = height - text_height - 20
```

#### 3. Lowres (Baixa Resolução)

Reduz a resolução da imagem.

**Configuração:**
```bash
/setpreview lowres
```

**Ajustar qualidade:**
Use a variável `PREVIEW_QUALITY` no `.env` (1-100):
```
PREVIEW_QUALITY=30  # Menor = mais pixelado
```

---

## 🌐 Adicionar Novas Fontes de Mídia

Para adicionar uma nova fonte de mídia (além de Coomer e Picazor):

### Passo 1: Adicionar URL da Fonte

```bash
/setsource https://coomer.st,https://picazor.com,https://novafonte.com
```

### Passo 2: Implementar o Método de Busca

Edite o arquivo `bot/fetcher.py` e adicione um novo método:

```python
async def _search_novafonte(self, model_name: str, base_url: str) -> List[MediaItem]:
    """
    Search media on NovaFonte.com
    """
    media_items = []
    
    try:
        # Construir URL de busca
        search_url = f"{base_url}/models/{model_name}"
        
        async with self.session.get(search_url) as response:
            if response.status != 200:
                return media_items
            
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Encontrar imagens (ajuste os seletores conforme necessário)
            for img in soup.find_all('img', class_='media-item'):
                img_url = img.get('src')
                if img_url:
                    img_url = urljoin(base_url, img_url)
                    filename = os.path.basename(urlparse(img_url).path)
                    media_items.append(MediaItem(img_url, filename, "photo"))
    
    except Exception as e:
        logger.error(f"Error in _search_novafonte: {e}")
    
    return media_items
```

### Passo 3: Adicionar Detecção da Fonte

No método `search_media`, adicione a detecção:

```python
async def search_media(self, model_name: str) -> List[MediaItem]:
    all_media = []
    
    for source in config.MEDIA_SOURCES:
        try:
            if "coomer" in source.lower():
                media = await self._search_coomer(model_name, source)
                all_media.extend(media)
            elif "picazor" in source.lower():
                media = await self._search_picazor(model_name, source)
                all_media.extend(media)
            elif "novafonte" in source.lower():  # ADICIONE AQUI
                media = await self._search_novafonte(model_name, source)
                all_media.extend(media)
            else:
                logger.warning(f"Unknown source: {source}")
        except Exception as e:
            logger.error(f"Error searching {source}: {e}")
    
    return all_media
```

---

## 🗣️ Adicionar Novos Idiomas

Para adicionar suporte a um novo idioma (ex: Francês):

### Passo 1: Adicionar Traduções

Edite o arquivo `bot/languages.py` e adicione um novo dicionário:

```python
TRANSLATIONS = {
    "pt": { ... },
    "es": { ... },
    "en": { ... },
    "fr": {  # NOVO IDIOMA
        "search_usage": "❌ Utilisation: /search <nom_du_modèle>",
        "searching": "🔍 Recherche de médias pour: {name}",
        # ... adicione todas as chaves
    }
}
```

### Passo 2: Adicionar Canal FREE

1. Crie um novo canal no Telegram para o idioma.
2. Adicione a variável de ambiente:

```
FREE_CHANNEL_FR_ID=-1001234567894
```

3. Atualize o `config.py`:

```python
self.FREE_CHANNEL_FR_ID = int(os.getenv("FREE_CHANNEL_FR_ID", 0))
```

4. Atualize o método `get_free_channel_by_lang`:

```python
def get_free_channel_by_lang(self, lang: str) -> Optional[int]:
    channels = {
        'pt': self.FREE_CHANNEL_PT_ID,
        'es': self.FREE_CHANNEL_ES_ID,
        'en': self.FREE_CHANNEL_EN_ID,
        'fr': self.FREE_CHANNEL_FR_ID  # NOVO
    }
    return channels.get(lang)
```

### Passo 3: Adicionar Comando Admin

Adicione um novo comando em `bot/admin.py`:

```python
async def cmd_setfreefr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set FREE FR channel ID"""
    if not await admin_only(update, context):
        return
    
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("❌ Uso: /setfreefr <channel_id>")
        return
    
    try:
        channel_id = int(context.args[0])
        config.set_value("FREE_CHANNEL_FR_ID", channel_id)
        
        await update.message.reply_text(
            get_text("free_channel_set", config.DEFAULT_LANG, lang="FR", channel_id=channel_id)
        )
        logger.info(f"FREE FR channel set to {channel_id}")
    except ValueError:
        await update.message.reply_text(
            get_text("invalid_channel", config.DEFAULT_LANG)
        )
```

### Passo 4: Registrar o Comando

No arquivo `bot/main.py`, adicione o handler:

```python
from admin import cmd_setfreefr  # Adicione ao import

# No método setup_handlers:
self.app.add_handler(CommandHandler("setfreefr", cmd_setfreefr))
```

---

## 📊 Adicionar Sistema de Estatísticas

Para rastrear estatísticas avançadas (downloads, uploads, usuários):

### Passo 1: Criar Banco de Dados

Instale SQLite ou PostgreSQL e crie as tabelas necessárias.

### Passo 2: Atualizar `users.py`

Adicione métodos para rastrear eventos:

```python
def log_search(self, user_id: int, model_name: str, results: int):
    """Log a search event"""
    # Salvar no banco de dados
    pass

def log_download(self, user_id: int, media_count: int):
    """Log a download event"""
    # Salvar no banco de dados
    pass
```

### Passo 3: Integrar no Fluxo

No arquivo `bot/main.py`, adicione logs após cada operação:

```python
# Após busca
user_manager.log_search(user_id, model_name, len(media_items))

# Após download
user_manager.log_download(user_id, len(downloaded))
```

---

## 🔒 Adicionar Autenticação de Usuários

Para restringir o acesso ao bot apenas a usuários autorizados:

### Passo 1: Criar Lista de Usuários Permitidos

No arquivo `.env`:

```
ALLOWED_USERS=123456789,987654321,555555555
```

### Passo 2: Validar no Comando

Edite o arquivo `bot/main.py`:

```python
async def cmd_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Verificar se o usuário está autorizado
    allowed_users = [int(uid) for uid in config.get_value("ALLOWED_USERS", "").split(",") if uid]
    
    if user_id not in allowed_users and user_id != config.ADMIN_ID:
        await update.message.reply_text("❌ Você não tem permissão para usar este bot.")
        return
    
    # Continuar com a busca...
```

---

## 🎯 Adicionar Agendamento de Posts

Para agendar posts automáticos em horários específicos:

### Passo 1: Instalar APScheduler

```bash
pip install apscheduler
```

### Passo 2: Adicionar ao `requirements.txt`

```
apscheduler==3.10.4
```

### Passo 3: Implementar Agendamento

No arquivo `bot/main.py`:

```python
from apscheduler.schedulers.asyncio import AsyncIOScheduler

class VIPBot:
    def __init__(self):
        self.app: Application = None
        self.uploader: TelegramUploader = None
        self.scheduler = AsyncIOScheduler()
    
    async def scheduled_post(self):
        """Post agendado"""
        # Implementar lógica de post automático
        pass
    
    def run(self):
        # ... código existente ...
        
        # Agendar posts diários às 10h
        self.scheduler.add_job(
            self.scheduled_post,
            'cron',
            hour=10,
            minute=0
        )
        self.scheduler.start()
        
        # Iniciar bot
        self.app.run_polling(allowed_updates=Update.ALL_TYPES)
```

---

## 🛠️ Dicas Avançadas

### Melhorar Performance

1. **Cache de buscas**: Armazene resultados de buscas recentes para evitar requisições duplicadas.
2. **Download paralelo**: Use `asyncio.gather()` para baixar múltiplas mídias simultaneamente.
3. **Compressão de imagens**: Reduza o tamanho dos arquivos antes do upload.

### Segurança

1. **Validação de entrada**: Sempre valide e sanitize inputs dos usuários.
2. **Rate limiting**: Implemente limites de requisições por usuário.
3. **Logs**: Mantenha logs detalhados de todas as operações.

### Manutenção

1. **Backups**: Configure backups automáticos do banco de dados e configurações.
2. **Monitoramento**: Use ferramentas como Sentry ou LogDNA para monitorar erros.
3. **Atualizações**: Mantenha as dependências atualizadas regularmente.

---

**Boa customização! 🚀**
