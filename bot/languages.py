"""
Multi-language support module
Provides translations for PT, ES, and EN
"""

from typing import Dict, Any

TRANSLATIONS = {
    "pt": {
        # Commands
        "search_usage": "❌ Uso: /search <nome_do_modelo>",
        "searching": "🔍 Buscando mídias de: {name}",
        "found_media": "✅ Encontradas {count} mídias!",
        "no_media_found": "❌ Nenhuma mídia encontrada para: {name}",
        "downloading": "⬇️ Baixando mídias... ({current}/{total})",
        "uploading_vip": "⬆️ Enviando para o canal VIP... ({current}/{total})",
        "uploading_free": "⬆️ Enviando prévias para canais FREE...",
        "complete": "✅ Processo concluído!\n📊 {vip_count} mídias no VIP\n📊 {free_count} prévias nos canais FREE",
        "error_occurred": "❌ Erro: {error}",
        "private_welcome": "👋 Olá, {name}!\n\nUse /search <nome> para buscar mídias.\nUse /ref para gerar seu link de convite.",
        "public_welcome": "👋 Olá, {name}!\n\nAqui você recebe previews e novidades.\n\nUse /ref para gerar seu link de convite.",
        "referral_title": "🎁 Programa de Recompensa",
        "referral_new": "✅ Nova indicação registrada! Total: {count}",
        "referral_goal_reached_admin": "🏆 {name} atingiu {goal} indicações (user_id: {user_id}).",
        "page_info": "📄 Página {current}/{total}",
        "next_page": "Próxima Página ➡️",
        "prev_page": "⬅️ Página Anterior",
        "download_and_upload": "📥 Baixando e Enviando... ({current}/{total})",
        
        # Admin commands
        "admin_only": "❌ Este comando é apenas para administradores.",
        "vip_channel_set": "✅ Canal VIP configurado: {channel_id}",
        "free_channel_set": "✅ Canal FREE {lang_code} configurado: {channel_id}",
        "sub_bot_set": "✅ Link do bot de assinatura configurado: {link}",
        "source_set": "✅ Fontes de mídia atualizadas: {sources}",
        "preview_set": "✅ Tipo de prévia configurado: {type}",
        "lang_set": "✅ Idioma padrão configurado: {new_lang}",
        "invalid_channel": "❌ ID de canal inválido. Use o formato: -1001234567890",
        "invalid_lang": "❌ Idioma inválido. Use: pt, es ou en",
        
        # Stats
        "stats_title": "📊 **Estatísticas do Bot**\n",
        "stats_vip": "🔒 Canal VIP: `{vip}`\n",
        "stats_free": "🆓 Canais FREE:\n  🇧🇷 PT: `{pt}`\n  🇪🇸 ES: `{es}`\n  🇺🇸 EN: `{en}`\n",
        "stats_sources": "🌐 Fontes de mídia: {count}\n",
        "stats_preview": "🖼️ Tipo de prévia: {type}\n",
        "stats_batch": "📦 Máx. arquivos/lote: {max}\n",
        "stats_interval": "⏱️ Intervalo de posts: {interval}s\n",
        
        # Preview captions - High Conversion
        "preview_caption": "🔥 **ISSO É SÓ O COMEÇO...**\n\nAcabamos de liberar o pack completo da **{name}** no nosso canal VIP! 😈\n\n🔞 O que você está perdendo:\n✅ Conteúdo sem censura\n✅ Vídeos exclusivos em 4K\n✅ Atualizações diárias\n\n⚠️ **OFERTA POR TEMPO LIMITADO!** O acesso pode fechar a qualquer momento.\n\n👇 **LIBERE O ACESSO COMPLETO AGORA:**\n👉 {sub_link}",
        
        # Help
        "help_user": """
🤖 **Bot de Mídias VIP**

**Comandos disponíveis:**
/search <nome> - Buscar mídias de um modelo/criador
/help - Mostrar esta mensagem

📌 As mídias completas são enviadas para o canal VIP.
📌 Prévias são publicadas nos canais FREE.
        """,
        
        "help_admin": """
🔧 **Co	**Configuração de Canais:**
	/setvip <channel_id> - Definir canal VIP
	/setfreept <channel_id> - Definir canal FREE PT
	/setfreees <channel_id> - Definir canal FREE ES
	/setfreeen <channel_id> - Definir canal FREE EN

	**Configurações:**
	/setsubbot_pt <link> - Link bot assinatura PT
	/setsubbot_es <link> - Link bot assinatura ES
	/setsubbot_en <link> - Link bot assinatura EN
	/setsource <url1,url2> - Fontes de mídia
	/setpreview <blur|watermark|lowres> - Tipo de prévia
	/setlang <pt|es|en> - Idioma padrão
**Informações:**
/stats - Estatísticas do bot
/restart - Reiniciar bot

💡 IDs de canal devem ser no formato: -1001234567890
        """
    },
    
    "es": {
        # Commands
        "search_usage": "❌ Uso: /search <nombre_del_modelo>",
        "searching": "🔍 Buscando medios de: {name}",
        "found_media": "✅ ¡{count} medios encontrados!",
        "no_media_found": "❌ No se encontraron medios para: {name}",
        "downloading": "⬇️ Descargando medios... ({current}/{total})",
        "uploading_vip": "⬆️ Enviando al canal VIP... ({current}/{total})",
        "uploading_free": "⬆️ Enviando vistas previas a canales FREE...",
        "complete": "✅ ¡Proceso completado!\n📊 {vip_count} medios en VIP\n📊 {free_count} vistas previas en canales FREE",
        "error_occurred": "❌ Error: {error}",
        "private_welcome": "👋 Hola, {name}!\n\nUsa /search <nombre> para buscar medios.\nUsa /ref para generar tu enlace de invitación.",
        "public_welcome": "👋 Hola, {name}!\n\nAquí recibes previews y novedades.\n\nUsa /ref para generar tu enlace de invitación.",
        "referral_title": "🎁 Programa de Recompensa",
        "referral_message": "{title}\n\nTu enlace de invitación:\n{link}\n\nReferidos confirmados: {count}",
        "referral_new": "✅ ¡Nuevo referido registrado! Total: {count}",
        "referral_goal_reached_user": "🏆 ¡Felicidades! Llegaste a {goal} referidos. Habla con el ADM para reclamar tu recompensa.",
        "referral_goal_reached_admin": "🏆 {name} llegó a {goal} referidos (user_id: {user_id}).",
        "broadcast_usage": "📢 Envía /broadcast para iniciar un envío. /cancel para cancelar.",
        "page_info": "📄 Página {current}/{total}",
        "next_page": "Próxima Página ➡️",
        "prev_page": "⬅️ Página Anterior",
        "download_and_upload": "📥 Descargando y Enviando... ({current}/{total})",
        
        # Admin commands
        "admin_only": "❌ Este comando es solo para administradores.",
        "vip_channel_set": "✅ Canal VIP configurado: {channel_id}",
        "free_channel_set": "✅ Canal FREE {lang_code} configurado: {channel_id}",
        "sub_bot_set": "✅ Enlace del bot de suscripción configurado: {link}",
        "source_set": "✅ Fuentes de medios actualizadas: {sources}",
        "preview_set": "✅ Tipo de vista previa configurado: {type}",
        "lang_set": "✅ Idioma predeterminado configurado: {new_lang}",
        "invalid_channel": "❌ ID de canal inválido. Use el formato: -1001234567890",
        "invalid_lang": "❌ Idioma inválido. Use: pt, es o en",
        
        # Stats
        "stats_title": "📊 **Estadísticas del Bot**\n",
        "stats_vip": "🔒 Canal VIP: `{vip}`\n",
        "stats_free": "🆓 Canales FREE:\n  🇧🇷 PT: `{pt}`\n  🇪🇸 ES: `{es}`\n  🇺🇸 EN: `{en}`\n",
        "stats_sources": "🌐 Fuentes de medios: {count}\n",
        "stats_preview": "🖼️ Tipo de vista previa: {type}\n",
        "stats_batch": "📦 Máx. archivos/lote: {max}\n",
        "stats_interval": "⏱️ Intervalo de publicaciones: {interval}s\n",
        
        # Preview captions - High Conversion
        "preview_caption": "🔥 **ESTO ÉS SOLO EL COMIENZO...**\n\n¡Acabamos de publicar el pack completo de **{name}** en nuestro canal VIP! 😈\n\n🔞 Lo que te estás perdiendo:\n✅ Contenido sin censura\n✅ Videos exclusivos en 4K\n✅ Actualizaciones diarias\n\n⚠️ **¡OFERTA POR TIEMPO LIMITADO!** El acceso puede cerrar en cualquier momento.\n\n👇 **LIBERA EL ACCESO COMPLETO AHORA:**\n👉 {sub_link}",
        
        # Help
        "help_user": """
🤖 **Bot de Medios VIP**

**Comandos disponibles:**
/search <nombre> - Buscar medios de un modelo/creador
/help - Mostrar este mensaje

📌 Los medios completos se envían al canal VIP.
📌 Las vistas previas se publican en los canales FREE.
        """,
        
        "help_admin": """
🔧 **Comandos de Administrador**

**Configuración de Canales:**
/setvip <channel_id> - Definir canal VIP
/setfreept <channel_id> - Definir canal FREE PT
/setfreees <channel_id> - Definir canal FREE ES
/setfreeen <channel_id> - Definir canal FREE EN

	**Configuraciones:**
	/setsubbot_pt <link> - Enlace bot suscripción PT
	/setsubbot_es <link> - Enlace bot suscripción ES
	/setsubbot_en <link> - Enlace bot suscripción EN
	/setsource <url1,url2> - Fuentes de medios
/setpreview <blur|watermark|lowres> - Tipo de vista previa
/setlang <pt|es|en> - Idioma predeterminado

**Información:**
/stats - Estadísticas del bot
/restart - Reiniciar bot

💡 Los IDs de canal deben estar en formato: -1001234567890
        """
    },
    
    "en": {
        # Commands
        "search_usage": "❌ Usage: /search <model_name>",
        "searching": "🔍 Searching media for: {name}",
        "found_media": "✅ Found {count} media files!",
        "no_media_found": "❌ No media found for: {name}",
        "downloading": "⬇️ Downloading media... ({current}/{total})",
        "uploading_vip": "⬆️ Uploading to VIP channel... ({current}/{total})",
        "uploading_free": "⬆️ Uploading previews to FREE channels...",
        "complete": "✅ Process completed!\n📊 {vip_count} media in VIP\n📊 {free_count} previews in FREE channels",
        "error_occurred": "❌ Error: {error}",
        "private_welcome": "👋 Hi, {name}!\n\nUse /search <name> to search media.\nUse /ref to generate your invite link.",
        "public_welcome": "👋 Hi, {name}!\n\nHere you will receive previews and news.\n\nUse /ref to generate your invite link.",
        "referral_title": "🎁 Reward Program",
        "referral_message": "{title}\n\nYour invite link:\n{link}\n\nConfirmed referrals: {count}",
        "referral_new": "✅ New referral registered! Total: {count}",
        "referral_goal_reached_user": "🏆 Congrats! You reached {goal} referrals. Talk to the admin to claim your reward.",
        "referral_goal_reached_admin": "🏆 {name} reached {goal} referrals (user_id: {user_id}).",
        "broadcast_usage": "📢 Send /broadcast to start a broadcast. /cancel to cancel.",
        "page_info": "📄 Page {current}/{total}",
        "next_page": "Next Page ➡️",
        "prev_page": "⬅️ Previous Page",
        "download_and_upload": "📥 Downloading and Uploading... ({current}/{total})",
        
        # Admin commands
        "admin_only": "❌ This command is for administrators only.",
        "vip_channel_set": "✅ VIP channel configured: {channel_id}",
        "free_channel_set": "✅ FREE channel {lang_code} configured: {channel_id}",
        "sub_bot_set": "✅ Subscription bot link configured: {link}",
        "source_set": "✅ Media sources updated: {sources}",
        "preview_set": "✅ Preview type configured: {type}",
        "lang_set": "✅ Default language configured: {new_lang}",
        "invalid_channel": "❌ Invalid channel ID. Use format: -1001234567890",
        "invalid_lang": "❌ Invalid language. Use: pt, es or en",
        
        # Stats
        "stats_title": "📊 **Bot Statistics**\n",
        "stats_vip": "🔒 VIP Channel: `{vip}`\n",
        "stats_free": "🆓 FREE Channels:\n  🇧🇷 PT: `{pt}`\n  🇪🇸 ES: `{es}`\n  🇺🇸 EN: `{en}`\n",
        "stats_sources": "🌐 Media sources: {count}\n",
        "stats_preview": "🖼️ Preview type: {type}\n",
        "stats_batch": "📦 Max files/batch: {max}\n",
        "stats_interval": "⏱️ Post interval: {interval}s\n",
        
        # Preview captions - High Conversion
        "preview_caption": "🔥 **THIS IS JUST THE BEGINNING...**\n\nWe just released **{name}**'s full pack in our VIP channel! 😈\n\n🔞 What you're missing out on:\n✅ Uncensored content\n✅ Exclusive 4K videos\n✅ Daily updates\n\n⚠️ **LIMITED TIME OFFER!** Access may close at any moment.\n\n👇 **UNLOCK FULL ACCESS NOW:**\n👉 {sub_link}",
        
        # Help
        "help_user": """
🤖 **VIP Media Bot**

**Available commands:**
/search <name> - Search media for a model/creator
/help - Show this message

📌 Full media is sent to the VIP channel.
📌 Previews are posted in FREE channels.
        """,
        
        "help_admin": """
🔧 **Administrator Commands**

**Channel Configuration:**
/setvip <channel_id> - Set VIP channel
/setfreept <channel_id> - Set FREE channel PT
/setfreees <channel_id> - Set FREE channel ES
/setfreeen <channel_id> - Set FREE channel EN

	**Settings:**
	/setsubbot_pt <link> - Subscription bot link PT
	/setsubbot_es <link> - Subscription bot link ES
	/setsubbot_en <link> - Subscription bot link EN
	/setsource <url1,url2> - Media sources
/setpreview <blur|watermark|lowres> - Preview type
/setlang <pt|es|en> - Default language

**Information:**
/stats - Bot statistics
/restart - Restart bot

💡 Channel IDs must be in format: -1001234567890
        """
    }
}


def get_text(key: str, lang: str = "pt", **kwargs) -> str:
    """
    Get translated text for a given key and language
    
    Args:
        key: Translation key
        lang: Language code (pt, es, en)
        **kwargs: Format parameters
    
    Returns:
        Translated and formatted text
    """
    if lang not in TRANSLATIONS:
        lang = "pt"
    
    text = TRANSLATIONS[lang].get(key, TRANSLATIONS["pt"].get(key, key))
    
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    
    return text


def get_all_langs() -> list:
    """Get list of all supported languages"""
    return list(TRANSLATIONS.keys())
