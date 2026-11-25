import asyncio
import json
import websockets
import redis.asyncio as redis
from websockets.server import WebSocketServerProtocol

# --- Конфигурация ---
REDIS_URL = "redis://localhost:6379/0"
HOST = "0.0.0.0"
PORT = 8765

# Глобальные структуры для управления комнатами
# room_id -> set(websocket connections)
rooms: dict[str, set[WebSocketServerProtocol]] = {}
# Блокировка для безопасного изменения rooms
rooms_lock = asyncio.Lock()

# Асинхронный клиент Redis
redis_client = redis.from_url(REDIS_URL, decode_responses=True)


async def get_room_state(room_id: str) -> dict:
    """Получает сохранённое состояние комнаты из Redis."""
    key = f"room:{room_id}:state"
    data = await redis_client.hgetall(key)
    if not data:
        # Если комната новая, начинаем с 0 секунды и паузы
        return {"position": 0.0, "is_playing": False}
    return {
        "position": float(data.get("position", 0)),
        "is_playing": data.get("is_playing") == "1"
    }


async def save_room_state(room_id: str, position: float, is_playing: bool):
    """Сохраняет текущее состояние комнаты в Redis."""
    key = f"room:{room_id}:state"
    await redis_client.hset(key, mapping={
        "position": position,
        "is_playing": "1" if is_playing else "0"
    })
    # Автоудаление через 2 часа, чтобы не копить мусор
    await redis_client.expire(key, 7200)


async def redis_listener(room_id: str):
    """
    Фоновый слушатель Redis Pub/Sub для конкретной комнаты.
    Получает сообщения из Redis и рассылает их всем WebSocket-клиентам комнаты.
    """
    pubsub = redis_client.pubsub()
    channel = f"room:{room_id}:sync"
    await pubsub.subscribe(channel)
    print(f"[Redis] Подписка на канал: {channel}")

    async for message in pubsub.listen():
        # Пропускаем служебные сообщения подписки
        if message["type"] != "message":
            continue

        try:
            payload = json.loads(message["data"])
            # Рассылаем всем клиентам в этой комнате
            async with rooms_lock:
                clients = rooms.get(room_id, set()).copy()

            if not clients:
                continue

            # Отправляем сообщение всем, кто ещё подключён
            disconnected = set()
            for ws in clients:
                try:
                    await ws.send(json.dumps(payload))
                except websockets.exceptions.ConnectionClosed:
                    disconnected.add(ws)
                except Exception as e:
                    print(f"[Broadcast Error] {e}")
                    disconnected.add(ws)

            # Чистим отвалившиеся соединения
            if disconnected:
                async with rooms_lock:
                    rooms[room_id] -= disconnected

        except json.JSONDecodeError:
            print(f"[Redis] Ошибка парсинга JSON: {message['data']}")
        except Exception as e:
            print(f"[Redis Listener Error] {e}")


async def handle_client(websocket: WebSocketServerProtocol):
    """
    Обработчик одного WebSocket-соединения.
    Путь должен содержать room_id, например: ws://localhost:8765/room123
    """
    # Извлекаем room_id из пути подключения
    # websockets 12+ передаёт path внутри websocket.path
    path = websocket.path if hasattr(websocket, "path") else "/"
    room_id = path.strip("/").split("/")[0] or "default"

    print(f"[WS] Клиент подключился к комнате: {room_id}")

    # Добавляем клиента в комнату
    async with rooms_lock:
        if room_id not in rooms:
            rooms[room_id] = set()
            # При первом клиенте в комнате запускаем слушатель Redis
            asyncio.create_task(redis_listener(room_id))
        rooms[room_id].add(websocket)

    # Отправляем текущее состояние новому зрителю
    state = await get_room_state(room_id)
    await websocket.send(json.dumps({
        "type": "init",
        "data": state
    }))

    try:
        async for raw_message in websocket:
            try:
                msg = json.loads(raw_message)
                event_type = msg.get("type")
                data = msg.get("data", {})

                if event_type in ("play", "pause", "seek"):
                    position = float(data.get("position", 0))

                    # 1. Сохраняем состояние в Redis
                    is_playing = event_type == "play"
                    await save_room_state(room_id, position, is_playing)

                    # 2. Публикуем событие в Redis Pub/Sub
                    channel = f"room:{room_id}:sync"
                    payload = {
                        "type": event_type,
                        "data": {"position": position},
                        "sender": id(websocket)  # чтобы клиент мог игнорировать своё же событие
                    }
                    await redis_client.publish(channel, json.dumps(payload))

            except json.JSONDecodeError:
                print(f"[WS] Неверный JSON от клиента: {raw_message}")
            except Exception as e:
                print(f"[WS Error] {e}")

    except websockets.exceptions.ConnectionClosed:
        print(f"[WS] Клиент отключился от комнаты: {room_id}")
    finally:
        # Удаляем клиента из комнаты
        async with rooms_lock:
            if room_id in rooms:
                rooms[room_id].discard(websocket)
                # Если комната опустела, можно оставить слушатель (он продолжит работать)
                # или отменить его. Для простоты оставляем.


async def main():
    print(f"Сервер запущен: ws://{HOST}:{PORT}")
    print("Подключение: ws://localhost:8765/название_комнаты")
    async with websockets.serve(handle_client, HOST, PORT):
        await asyncio.Future()  # Работаем вечно


if __name__ == "__main__":
    asyncio.run(main())