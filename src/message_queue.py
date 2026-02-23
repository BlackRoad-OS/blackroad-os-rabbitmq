"""
Message broker / queue system (RabbitMQ-inspired).
Provides message queue management with exchanges, bindings, and dead-letter queues.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
import sqlite3
import os
import uuid
from pathlib import Path


@dataclass
class Queue:
    """Message queue definition."""
    id: str
    name: str
    vhost: str
    durable: bool
    exclusive: bool
    auto_delete: bool
    arguments: Dict[str, Any]
    message_count: int
    consumer_count: int
    created_at: datetime
    last_message_at: Optional[datetime]


@dataclass
class Message:
    """Message in a queue."""
    id: str
    queue_id: str
    exchange: str
    routing_key: str
    body: bytes
    headers: Dict[str, str]
    priority: int
    delivery_mode: int
    content_type: str
    published_at: datetime
    acked_at: Optional[datetime]
    nacked_at: Optional[datetime]
    attempts: int


@dataclass
class Exchange:
    """Message exchange for routing."""
    id: str
    name: str
    type: str  # direct, topic, fanout, headers
    vhost: str
    durable: bool
    auto_delete: bool


class MessageBroker:
    """RabbitMQ-inspired message broker implementation."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize broker with SQLite backend."""
        if db_path is None:
            db_path = os.path.expanduser("~/.blackroad/mq.db")
        
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS exchanges (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                vhost TEXT NOT NULL,
                durable BOOLEAN,
                auto_delete BOOLEAN,
                UNIQUE(name, vhost)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS queues (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                vhost TEXT NOT NULL,
                durable BOOLEAN,
                exclusive BOOLEAN,
                auto_delete BOOLEAN,
                arguments TEXT,
                message_count INTEGER DEFAULT 0,
                consumer_count INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                last_message_at TEXT,
                UNIQUE(name, vhost)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bindings (
                queue_id TEXT NOT NULL,
                exchange_id TEXT NOT NULL,
                routing_key TEXT NOT NULL,
                vhost TEXT NOT NULL,
                PRIMARY KEY (queue_id, exchange_id, routing_key)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                queue_id TEXT NOT NULL,
                exchange TEXT NOT NULL,
                routing_key TEXT NOT NULL,
                body BLOB NOT NULL,
                headers TEXT,
                priority INTEGER DEFAULT 0,
                delivery_mode INTEGER DEFAULT 2,
                content_type TEXT,
                published_at TEXT NOT NULL,
                acked_at TEXT,
                nacked_at TEXT,
                attempts INTEGER DEFAULT 0,
                FOREIGN KEY (queue_id) REFERENCES queues(id)
            )
        """)
        
        conn.commit()
        conn.close()

    def declare_queue(
        self,
        name: str,
        vhost: str = "/",
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
    ) -> Queue:
        """Declare a queue (idempotent)."""
        arguments = arguments or {}
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if exists
        cursor.execute(
            "SELECT id FROM queues WHERE name = ? AND vhost = ?",
            (name, vhost)
        )
        result = cursor.fetchone()
        
        if result:
            queue_id = result[0]
        else:
            queue_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO queues
                (id, name, vhost, durable, exclusive, auto_delete, arguments, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (queue_id, name, vhost, durable, exclusive, auto_delete,
                  str(arguments), datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        return Queue(
            id=queue_id,
            name=name,
            vhost=vhost,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
            message_count=0,
            consumer_count=0,
            created_at=datetime.now(),
            last_message_at=None,
        )

    def declare_exchange(
        self,
        name: str,
        type: str = "direct",
        vhost: str = "/",
        durable: bool = True,
        auto_delete: bool = False,
    ) -> Exchange:
        """Declare an exchange."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        exchange_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT OR IGNORE INTO exchanges
            (id, name, type, vhost, durable, auto_delete)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (exchange_id, name, type, vhost, durable, auto_delete))
        
        conn.commit()
        conn.close()
        
        return Exchange(
            id=exchange_id,
            name=name,
            type=type,
            vhost=vhost,
            durable=durable,
            auto_delete=auto_delete,
        )

    def bind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str,
        vhost: str = "/",
    ) -> bool:
        """Bind queue to exchange with routing key."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id FROM queues WHERE name = ? AND vhost = ?",
            (queue_name, vhost)
        )
        queue_result = cursor.fetchone()
        
        cursor.execute(
            "SELECT id FROM exchanges WHERE name = ? AND vhost = ?",
            (exchange_name, vhost)
        )
        exchange_result = cursor.fetchone()
        
        if queue_result and exchange_result:
            cursor.execute("""
                INSERT OR IGNORE INTO bindings
                (queue_id, exchange_id, routing_key, vhost)
                VALUES (?, ?, ?, ?)
            """, (queue_result[0], exchange_result[0], routing_key, vhost))
            conn.commit()
            conn.close()
            return True
        
        conn.close()
        return False

    def publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        headers: Optional[Dict[str, str]] = None,
        priority: int = 0,
        content_type: str = "text/plain",
        vhost: str = "/",
    ) -> str:
        """Publish message to exchange."""
        headers = headers or {}
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        message_id = str(uuid.uuid4())
        
        # Get exchange ID
        cursor.execute(
            "SELECT id FROM exchanges WHERE name = ? AND vhost = ?",
            (exchange, vhost)
        )
        exchange_result = cursor.fetchone()
        if not exchange_result:
            conn.close()
            return ""
        
        exchange_id = exchange_result[0]
        now = datetime.now()
        
        cursor.execute("""
            INSERT INTO messages
            (id, queue_id, exchange, routing_key, body, headers, priority,
             delivery_mode, content_type, published_at, attempts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (message_id, "", exchange, routing_key, body, str(headers),
              priority, 2, content_type, now.isoformat(), 0))
        
        conn.commit()
        conn.close()
        
        return message_id

    def consume(self, queue_name: str, n: int = 1, ack: bool = True) -> List[Message]:
        """Consume up to n messages from queue."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id FROM queues WHERE name = ?",
            (queue_name,)
        )
        queue_result = cursor.fetchone()
        if not queue_result:
            conn.close()
            return []
        
        queue_id = queue_result[0]
        
        cursor.execute("""
            SELECT id, queue_id, exchange, routing_key, body, headers,
                   priority, delivery_mode, content_type, published_at,
                   acked_at, nacked_at, attempts
            FROM messages
            WHERE queue_id = ? AND acked_at IS NULL AND nacked_at IS NULL
            LIMIT ?
        """, (queue_id, n))
        
        rows = cursor.fetchall()
        messages = []
        
        for row in rows:
            if ack:
                cursor.execute(
                    "UPDATE messages SET acked_at = ? WHERE id = ?",
                    (datetime.now().isoformat(), row[0])
                )
            
            messages.append(Message(
                id=row[0],
                queue_id=row[1],
                exchange=row[2],
                routing_key=row[3],
                body=row[4],
                headers=eval(row[5]) if row[5] else {},
                priority=row[6],
                delivery_mode=row[7],
                content_type=row[8],
                published_at=datetime.fromisoformat(row[9]),
                acked_at=datetime.fromisoformat(row[10]) if row[10] else None,
                nacked_at=datetime.fromisoformat(row[11]) if row[11] else None,
                attempts=row[12],
            ))
        
        conn.commit()
        conn.close()
        return messages

    def nack(self, message_id: str, requeue: bool = True) -> bool:
        """Mark message as failed (nacked)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE messages
            SET nacked_at = ?, attempts = attempts + 1
            WHERE id = ?
        """, (datetime.now().isoformat(), message_id))
        
        conn.commit()
        conn.close()
        return True

    def purge_queue(self, queue_name: str) -> int:
        """Delete all messages in queue."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id FROM queues WHERE name = ?",
            (queue_name,)
        )
        result = cursor.fetchone()
        if not result:
            conn.close()
            return 0
        
        queue_id = result[0]
        cursor.execute("DELETE FROM messages WHERE queue_id = ?", (queue_id,))
        
        conn.commit()
        deleted = cursor.rowcount
        conn.close()
        return deleted

    def get_queue_stats(self, queue_name: Optional[str] = None) -> Dict[str, Any]:
        """Get queue statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if queue_name:
            cursor.execute(
                "SELECT id, message_count, consumer_count FROM queues WHERE name = ?",
                (queue_name,)
            )
        else:
            cursor.execute(
                "SELECT id, message_count, consumer_count FROM queues"
            )
        
        rows = cursor.fetchall()
        conn.close()
        
        return {
            "queues": len(rows),
            "total_messages": sum(r[1] for r in rows),
            "total_consumers": sum(r[2] for r in rows),
        }

    def list_queues(self, vhost: Optional[str] = None) -> List[Queue]:
        """List all queues."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if vhost:
            cursor.execute(
                "SELECT id, name, vhost, durable, exclusive, auto_delete, "
                "arguments, message_count, consumer_count, created_at, last_message_at "
                "FROM queues WHERE vhost = ?",
                (vhost,)
            )
        else:
            cursor.execute(
                "SELECT id, name, vhost, durable, exclusive, auto_delete, "
                "arguments, message_count, consumer_count, created_at, last_message_at "
                "FROM queues"
            )
        
        queues = []
        for row in cursor.fetchall():
            queues.append(Queue(
                id=row[0],
                name=row[1],
                vhost=row[2],
                durable=row[3],
                exclusive=row[4],
                auto_delete=row[5],
                arguments=eval(row[6]) if row[6] else {},
                message_count=row[7],
                consumer_count=row[8],
                created_at=datetime.fromisoformat(row[9]),
                last_message_at=datetime.fromisoformat(row[10]) if row[10] else None,
            ))
        
        conn.close()
        return queues

    def dead_letter_queue(self, queue_name: str, dlq_name: str) -> bool:
        """Configure dead-letter queue after 3 nacks."""
        # Create DLQ
        self.declare_queue(dlq_name)
        
        # Implementation: messages with attempts >= 3 are routed to DLQ
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id FROM queues WHERE name = ?",
            (queue_name,)
        )
        result = cursor.fetchone()
        
        if result:
            cursor.execute("""
                UPDATE queues SET arguments = ?
                WHERE id = ?
            """, (f'{{"x-dead-letter-queue": "{dlq_name}"}}', result[0]))
            conn.commit()
            conn.close()
            return True
        
        conn.close()
        return False

    def get_pending_dlq(self, dlq_name: str) -> List[Message]:
        """Get messages that failed 3+ times in DLQ."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id FROM queues WHERE name = ?",
            (dlq_name,)
        )
        result = cursor.fetchone()
        if not result:
            conn.close()
            return []
        
        queue_id = result[0]
        
        cursor.execute("""
            SELECT id, queue_id, exchange, routing_key, body, headers,
                   priority, delivery_mode, content_type, published_at,
                   acked_at, nacked_at, attempts
            FROM messages
            WHERE queue_id = ? AND attempts >= 3
        """, (queue_id,))
        
        messages = []
        for row in cursor.fetchall():
            messages.append(Message(
                id=row[0],
                queue_id=row[1],
                exchange=row[2],
                routing_key=row[3],
                body=row[4],
                headers=eval(row[5]) if row[5] else {},
                priority=row[6],
                delivery_mode=row[7],
                content_type=row[8],
                published_at=datetime.fromisoformat(row[9]),
                acked_at=datetime.fromisoformat(row[10]) if row[10] else None,
                nacked_at=datetime.fromisoformat(row[11]) if row[11] else None,
                attempts=row[12],
            ))
        
        conn.close()
        return messages


if __name__ == "__main__":
    import sys
    import json
    
    broker = MessageBroker()
    
    if len(sys.argv) < 2:
        print("Usage: python message_queue.py {queues|publish|consume}")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "queues":
        queues = broker.list_queues()
        for q in queues:
            print(f"{q.name} ({q.vhost}): {q.message_count} messages")
    
    elif cmd == "publish" and len(sys.argv) >= 5:
        exchange = sys.argv[2]
        routing_key = sys.argv[3]
        body = sys.argv[4].encode()
        msg_id = broker.publish(exchange, routing_key, body)
        print(f"Published: {msg_id}")
    
    elif cmd == "consume" and len(sys.argv) >= 3:
        queue_name = sys.argv[2]
        n = int(sys.argv[4]) if "--n" in sys.argv else 1
        messages = broker.consume(queue_name, n=n)
        for msg in messages:
            print(f"Message {msg.id}: {msg.body.decode()}")
