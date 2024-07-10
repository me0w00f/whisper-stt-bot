import logging
import os
import uuid
import asyncio
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from telethon import TelegramClient, events
import io
import aiohttp
from pydub import AudioSegment
import subprocess

# 加载环境变量
load_dotenv()

# 环境变量检查
required_vars = ['TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'TELEGRAM_BOT_TOKEN', 'API_KEY', 'MAX_RATE', 'RATE_REC_TIME', 'RATE_LIMITS','API_URL']
if any([os.getenv(var) is None for var in required_vars]):
    missing = [var for var in required_vars if os.getenv(var) is None]
    logging.error("Missing configuration for: " + ', '.join(missing))
    exit(1)

try:
    max_rate = int(os.getenv('MAX_RATE'))
    rate_rec_time = int(os.getenv('RATE_REC_TIME'))
    if max_rate < 1 or rate_rec_time < 1:
        raise ValueError("max_rate和rate_rec_time必须是大于等于1的整数。")
except ValueError as e:
    logging.error(f"环境变量配置错误: {str(e)}")
    exit(1)

api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
api_url= os.getenv('API_URL')
api_key = os.getenv('API_KEY')
max_rate = int(os.getenv('MAX_RATE'))
rate_rec_time = int(os.getenv('RATE_REC_TIME'))
rate_limits = os.getenv('RATE_LIMITS')

# 创建所需目录
os.makedirs('cache', exist_ok=True)
os.makedirs('logs', exist_ok=True)

# 日志设置
today_date = datetime.now().strftime("%Y%m%d")
log_filename = f'logs/sttbot_{today_date}.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_filename, 'a'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

# 初始化数据库
conn = sqlite3.connect('rate_limit.db')
c = conn.cursor()
c.execute('''
CREATE TABLE IF NOT EXISTS user_rates (
    user_id INTEGER PRIMARY KEY,
    rate INTEGER,
    recving BOOLEAN
)
''')
conn.commit()

def ensure_user_rate(user_id):
    c.execute('SELECT rate FROM user_rates WHERE user_id = ?', (user_id,))
    if c.fetchone() is None:
        c.execute('INSERT INTO user_rates (user_id, rate, recving) VALUES (?, ?, ?)', (user_id, max_rate, False))
        conn.commit()
        logging.info(f"用户 {user_id} - 初始化次数限制，总次数为 {max_rate}。")

async def decrement_rate(user_id):
    ensure_user_rate(user_id)
    c.execute('SELECT rate, recving FROM user_rates WHERE user_id = ?', (user_id,))
    rate, recving = c.fetchone()
    if rate > 0:
        c.execute('UPDATE user_rates SET rate = rate - 1 WHERE user_id = ?', (user_id,))
        conn.commit()
        logging.info(f"用户 {user_id} - 次数减少，新次数为 {rate - 1}。")
        if rate - 1 < max_rate and not recving:
            c.execute('UPDATE user_rates SET recving = ? WHERE user_id = ?', (True, user_id))
            conn.commit()
            asyncio.create_task(recover_rate(user_id))
            logging.info(f"用户 {user_id} - 触发次数恢复进程。")
        elif rate - 1 < max_rate and recving:
            logging.info(f"用户 {user_id} - 已经在恢复中，不额外触发恢复进程。")
        return True
    else:
        logging.info(f"用户 {user_id} - 达到次数限制，无法处理请求。")
    return False

async def recover_rate(user_id):
    await asyncio.sleep(rate_rec_time)
    c.execute('SELECT rate, recving FROM user_rates WHERE user_id = ?', (user_id,))
    rate, recving = c.fetchone()
    if rate < max_rate and recving:
        c.execute('UPDATE user_rates SET rate = rate + 1 WHERE user_id = ?', (user_id,))
        conn.commit()
        logging.info(f"用户 {user_id} - 次数恢复一个，新次数为 {rate + 1}。")
        asyncio.create_task(recover_rate(user_id))
    else:
        c.execute('UPDATE user_rates SET recving = ? WHERE user_id = ?', (False, user_id))
        conn.commit()
        logging.info(f"用户 {user_id} - 次数完全恢复，恢复进程结束。")

client = TelegramClient('bot', api_id, api_hash).start(bot_token=bot_token)

@client.on(events.NewMessage(pattern='(?i)'))
async def handler(event):
    user_id = event.sender_id
    event_uuid = str(uuid.uuid4())
    
    if event.message.media and hasattr(event.message, 'document') and event.message.document:
        file_size = event.message.document.size
        if file_size > 500 * 1024 * 1024:  # 大于500MB的文件不处理
            logger.warning(f"{event_uuid} - 文件大小超过限制，不下载")
            await event.reply("文件太大，无法处理。")
            return
        
        file_ext = event.message.document.mime_type.split('/')[-1] if event.message.document.mime_type else 'unknown'
        logger.info(f"{event_uuid} - 文件扩展名为: {file_ext}")
        filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{event_uuid}.{file_ext}"
        file_path = os.path.join('cache', filename)

        logger.info(f"{event_uuid} - 收到媒体文件，正在下载: {filename}")
        await event.message.download_media(file=file_path)
        logger.info(f"{event_uuid} - 音频文件下载完成，文件保存为 {filename}")
        
        # 检查速率限制并尝试扣除次数
        if rate_limits.upper() == 'ON' and not await decrement_rate(user_id):
            await event.reply("您已达到速率限制。请稍后再试。")
            os.remove(file_path)  # 删除下载的文件以免浪费空间
            return

        # 转录音频文件
        transcription_text = await transcribe_audio(file_path, "https://api.groq.com/openai/v1/audio/transcriptions", event_uuid)
        if transcription_text:
            logger.info(f"{event_uuid} - 音频转写完成")
            with io.BytesIO(transcription_text.encode('utf-8')) as txt_file:
                logger.info(f"{event_uuid} - 转写结果发送中")
                txt_file.name = "transcription.txt"
                await event.reply(file=txt_file)
                logger.info(f"{event_uuid} - 转写结果已发送")
        else:
            await event.reply("转写失败，请稍后重试")
            logger.warning(f"{event_uuid} - 转写失败")
    else:
        await event.reply("请发送一个有效的音频文件。")
        logger.warning(f"{event_uuid} - 收到非音频文件")

async def transcribe_audio(file_path, api_url, event_uuid):
    try:
        logger.info(f"{event_uuid} - 开始处理音频文件: {file_path}")

        # 读取音频文件
        logger.info(f"{event_uuid} - 打开音频文件: {file_path}")
        with open(file_path, "rb") as audio_file:
            audio_data = audio_file.read()

        # 使用pydub转换音频
        audio = AudioSegment.from_file(io.BytesIO(audio_data), format=file_path.split('.')[-1])
        logging.info(f"{event_uuid} - 音频重新采样到16000Hz中: {file_path}")
        audio = audio.set_frame_rate(16000)

        # 使用ffmpeg进行格式转换
        ffmpeg_command = ['ffmpeg', '-i', 'pipe:0', '-acodec', 'libopus', '-f', 'ogg', 'pipe:1']
        process = subprocess.Popen(ffmpeg_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # 使用 BytesIO 导出音频并直接读取字节
        audio_bytes = audio.export(format='wav').read()
        output_stream, error = process.communicate(input=audio_bytes)

        if process.returncode != 0:
            logger.error(f"{event_uuid} - ffmpeg 处理失败，错误信息: {error.decode()}")
            # 保留文件以便调试
            logger.info(f"{event_uuid} - 保留原始文件以便调试: {file_path}")
            return None

        # 使用 aiohttp 发送转录请求
        async with aiohttp.ClientSession() as session:
            headers = {'Authorization': f'Bearer {api_key}'}
            data = aiohttp.FormData()
            data.add_field('file', output_stream, filename="filename.ogg", content_type='application/ogg')
            data.add_field('model', 'whisper-large-v3')

            async with session.post(api_url, headers=headers, data=data) as response:
                if response.status == 200:
                    transcription = await response.json()
                    logger.info(f"{event_uuid} - 音频转录成功")
                    return transcription['text']
                else:
                    logger.error(f"{event_uuid} - 音频转录失败: {await response.text()}")
                    return None
    except Exception as e:
        logger.error(f"{event_uuid} - 处理音频时出错: {str(e)}")
        return None
    finally:
        # 如果ffmpeg处理成功，删除文件
        if process and process.returncode == 0:
            os.remove(file_path)
            logger.info(f"{event_uuid} - 音频文件已删除: {file_path}")

if __name__ == '__main__':
    logger.info("Bot已启动")
    client.run_until_disconnected()
    logger.info("Bot已退出")