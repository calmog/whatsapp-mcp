import sqlite3
import re
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Tuple
import os.path
import requests
import json
import audio

MESSAGES_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'whatsapp-bridge', 'store', 'messages.db')
# whatsmeow's own session store. It holds the authoritative phone<->LID mapping
# (whatsmeow_lid_map) and the full address book (whatsmeow_contacts). Modern WhatsApp
# keys most 1:1 chats by a LID JID (<id>@lid) that does NOT contain the phone number,
# so phone-number lookups against messages.db alone silently miss them.
WHATSAPP_SESSION_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'whatsapp-bridge', 'store', 'whatsapp.db')
WHATSAPP_API_BASE_URL = "http://localhost:8080/api"

_SYNC_HINT = (
    "No data found in the local database. The chat may exist on your phone but was not "
    "included in WhatsApp's initial history sync. You can still send a message directly. "
    "To attempt fetching recent messages for a known JID, call request_history_sync(chat_jid) and wait a few seconds."
)

# The Go bridge owns both SQLite files and writes to them continuously. The Python side
# only ever reads, so open read-only (never contend for a write lock) and wait out the
# bridge's brief write locks rather than erroring out immediately.
_DB_BUSY_TIMEOUT_MS = 5000


def _connect_messages() -> sqlite3.Connection:
    """Open messages.db read-only with a busy timeout (the bridge is the only writer)."""
    conn = sqlite3.connect(f"file:{MESSAGES_DB_PATH}?mode=ro", uri=True, timeout=_DB_BUSY_TIMEOUT_MS / 1000)
    conn.execute(f"PRAGMA busy_timeout = {_DB_BUSY_TIMEOUT_MS}")
    return conn


def _attach_session(conn: sqlite3.Connection) -> bool:
    """Attach whatsmeow's session DB (read-only) as `wa` to resolve LIDs and contact names.

    Returns True if the session DB is present and attached, False otherwise (callers
    fall back to messages.db-only behaviour so nothing breaks if it's missing/locked).
    Read-only so we never block the bridge, which writes this DB far more often.
    """
    if not os.path.exists(WHATSAPP_SESSION_DB_PATH):
        return False
    try:
        conn.execute("ATTACH DATABASE ? AS wa", (f"file:{WHATSAPP_SESSION_DB_PATH}?mode=ro",))
        # Confirm the tables we rely on actually exist in this whatsmeow version.
        conn.execute("SELECT 1 FROM wa.whatsmeow_lid_map LIMIT 1")
        return True
    except sqlite3.Error:
        return False


def _digits(value: Optional[str]) -> str:
    """Strip a phone number / JID down to bare digits for matching against the lid map."""
    return re.sub(r"\D", "", value or "")


def _jid_user(jid: str) -> str:
    """User part of a JID without the @server or any :device / .agent suffix.

    A JID can carry a device/agent suffix (e.g. `<lid>:43@lid`, `<phone>.0@...`).
    The lid map is keyed by the bare user id, so strip the suffix before any lookup.
    """
    user = jid.partition("@")[0]
    return user.split(":", 1)[0].split(".", 1)[0]


def _candidate_jids_for_phone(conn: sqlite3.Connection, phone: str, has_session: bool) -> List[str]:
    """Map a phone number to every chat JID it could be stored under.

    A direct chat may live under `<number>@s.whatsapp.net` or, more commonly now, under
    a `<lid>@lid` JID whose digits are unrelated to the phone number. We resolve the LID
    via whatsmeow's lid map so phone-number lookups find LID-keyed chats too.
    """
    digits = _digits(phone)
    candidates = [f"{digits}@s.whatsapp.net"]
    if has_session and digits:
        row = conn.execute("SELECT lid FROM wa.whatsmeow_lid_map WHERE pn = ?", (digits,)).fetchone()
        if row and row[0]:
            candidates.append(f"{row[0]}@lid")
        # The caller may have passed a LID instead of a phone number.
        row = conn.execute("SELECT pn FROM wa.whatsmeow_lid_map WHERE lid = ?", (digits,)).fetchone()
        if row and row[0]:
            candidates.append(f"{row[0]}@s.whatsapp.net")
        candidates.append(f"{digits}@lid")
    # De-duplicate while preserving order.
    return list(dict.fromkeys(candidates))


def _jid_for_bare_number(conn: sqlite3.Connection, number: str, has_session: bool) -> str:
    """Turn a bare sender id (no @, as stored in messages.sender) into its best JID.

    A message sender is stored as a bare number that may be either a LID or a phone
    number; consult the lid map to pick the right server suffix so name resolution can
    find the contact. Falls back to the phone server when the map doesn't know it.
    """
    digits = _digits(number)
    if has_session and digits:
        if conn.execute("SELECT 1 FROM wa.whatsmeow_lid_map WHERE lid = ?", (digits,)).fetchone():
            return f"{digits}@lid"
        if conn.execute("SELECT 1 FROM wa.whatsmeow_lid_map WHERE pn = ?", (digits,)).fetchone():
            return f"{digits}@s.whatsapp.net"
    return f"{digits}@s.whatsapp.net" if digits else number


def _alternate_jid(conn: sqlite3.Connection, jid: str, has_session: bool) -> Optional[str]:
    """Return the phone<->LID counterpart of a JID via the lid map, if known."""
    if not (has_session and jid):
        return None
    server = jid.partition("@")[2]
    user = _jid_user(jid)
    if server == "lid":
        row = conn.execute("SELECT pn FROM wa.whatsmeow_lid_map WHERE lid = ?", (user,)).fetchone()
        if row and row[0]:
            return f"{row[0]}@s.whatsapp.net"
    elif server == "s.whatsapp.net":
        row = conn.execute("SELECT lid FROM wa.whatsmeow_lid_map WHERE pn = ?", (user,)).fetchone()
        if row and row[0]:
            return f"{row[0]}@lid"
    return None


def _resolve_contact_name(conn: sqlite3.Connection, jid: str, has_session: bool, fallback: Optional[str] = None) -> Optional[str]:
    """Look up a human name for a JID from the address book, preferring full > push name.

    A name you save in your phone's address book syncs to whatsmeow keyed by the *phone*
    JID, while the chat itself is often keyed by a LID JID. So we look under both the
    given JID and its phone<->LID counterpart, and prefer a saved full name over a
    self-set push name across whichever row has it.
    """
    if has_session and jid:
        jids = [jid]
        alt = _alternate_jid(conn, jid, has_session)
        if alt:
            jids.append(alt)
        placeholders = ",".join("?" * len(jids))
        rows = conn.execute(
            "SELECT full_name, first_name, push_name, business_name "
            f"FROM wa.whatsmeow_contacts WHERE their_jid IN ({placeholders})",
            jids,
        ).fetchall()
        # Prefer full_name (you saved it) > first_name > push_name > business_name,
        # checking that priority across every matching row.
        for column in range(4):
            for row in rows:
                if row[column]:
                    return row[column]
    return fallback


def _phone_for_jid(conn: sqlite3.Connection, jid: str, has_session: bool) -> str:
    """Best-effort phone number for a JID; resolves LID JIDs back to their phone number."""
    server = jid.partition("@")[2]
    user = _jid_user(jid)
    if server == "lid" and has_session:
        row = conn.execute("SELECT pn FROM wa.whatsmeow_lid_map WHERE lid = ?", (user,)).fetchone()
        if row and row[0]:
            return row[0]
    return user


def _needs_name_resolution(name: Optional[str], jid: str) -> bool:
    """True when the stored chat name is missing or just a raw number.

    Besides an empty name or the chat's own JID/LID digits, the bridge sometimes
    stores an @lid chat's name as the *wrong* party's bare number (e.g. the owner's
    own LID, taken from a from-me message). Any name with no letters at all is treated
    as unresolved and sent through address-book resolution.
    """
    if not name:
        return True
    if name == jid.partition("@")[0]:
        return True
    return not any(ch.isalpha() for ch in name)


def _ts(value: Optional[str]) -> Optional[datetime]:
    """Parse a stored ISO timestamp, tolerating nulls/garbage (returns None)."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None

@dataclass
class Message:
    timestamp: datetime
    sender: str
    content: str
    is_from_me: bool
    chat_jid: str
    id: str
    chat_name: Optional[str] = None
    media_type: Optional[str] = None

@dataclass
class Chat:
    jid: str
    name: Optional[str]
    last_message_time: Optional[datetime]
    last_message: Optional[str] = None
    last_sender: Optional[str] = None
    last_is_from_me: Optional[bool] = None

    @property
    def is_group(self) -> bool:
        """Determine if chat is a group based on JID pattern."""
        return self.jid.endswith("@g.us")

@dataclass
class Contact:
    phone_number: str
    name: Optional[str]
    jid: str

@dataclass
class MessageContext:
    message: Message
    before: List[Message]
    after: List[Message]

def get_sender_name(sender_jid: str) -> str:
    try:
        conn = _connect_messages()
        has_session = _attach_session(conn)
        cursor = conn.cursor()

        # The sender is usually a bare LID/phone number; map it to a proper JID and try
        # the address book first, since the chats table can't name LID senders.
        jid = sender_jid if '@' in sender_jid else _jid_for_bare_number(conn, sender_jid, has_session)
        name = _resolve_contact_name(conn, jid, has_session)
        if name and not _needs_name_resolution(name, jid):
            return name

        # Fall back to the chats table, but never return a bare-number chat name (the
        # bridge stores the wrong party's number for @lid chats) — that's not a real name.
        cursor.execute("SELECT name FROM chats WHERE jid = ? LIMIT 1", (jid,))
        result = cursor.fetchone()
        if not result:
            phone_part = sender_jid.split('@')[0] if '@' in sender_jid else sender_jid
            cursor.execute("SELECT name FROM chats WHERE jid LIKE ? LIMIT 1", (f"%{phone_part}%",))
            result = cursor.fetchone()
        if result and result[0] and not _needs_name_resolution(result[0], jid):
            return result[0]

        return name or sender_jid

    except sqlite3.Error as e:
        print(f"Database error while getting sender name: {e}")
        return sender_jid
    finally:
        if 'conn' in locals():
            conn.close()

def _display_chat_name(chat_jid: str, raw_name: Optional[str]) -> Optional[str]:
    """Resolve a chat's display name, fixing the bare-number names the bridge stores
    for @lid chats. Returns the raw name unchanged when it's already a real name."""
    if not chat_jid or not _needs_name_resolution(raw_name, chat_jid):
        return raw_name
    try:
        conn = _connect_messages()
        has_session = _attach_session(conn)
        return _resolve_contact_name(conn, chat_jid, has_session, fallback=raw_name)
    except sqlite3.Error:
        return raw_name
    finally:
        if 'conn' in locals():
            conn.close()


def format_message(message: Message, show_chat_info: bool = True) -> None:
    """Print a single message with consistent formatting."""
    output = ""

    chat_name = _display_chat_name(message.chat_jid, message.chat_name)
    if show_chat_info and chat_name:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] Chat: {chat_name} "
    else:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] "
        
    content_prefix = ""
    if hasattr(message, 'media_type') and message.media_type:
        content_prefix = f"[{message.media_type} - Message ID: {message.id} - Chat JID: {message.chat_jid}] "
    
    try:
        sender_name = get_sender_name(message.sender) if not message.is_from_me else "Me"
        output += f"From: {sender_name}: {content_prefix}{message.content}\n"
    except Exception as e:
        print(f"Error formatting message: {e}")
    return output

def format_messages_list(messages: List[Message], show_chat_info: bool = True) -> None:
    output = ""
    if not messages:
        output += "No messages to display."
        return output
    
    for message in messages:
        output += format_message(message, show_chat_info)
    return output

def list_messages(
    after: Optional[str] = None,
    before: Optional[str] = None,
    sender_phone_number: Optional[str] = None,
    chat_jid: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1
) -> List[Message]:
    """Get messages matching the specified criteria with optional context."""
    try:
        conn = _connect_messages()
        cursor = conn.cursor()
        
        # Build base query
        query_parts = ["SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type FROM messages"]
        query_parts.append("JOIN chats ON messages.chat_jid = chats.jid")
        where_clauses = []
        params = []
        
        # Add filters
        if after:
            try:
                after = datetime.fromisoformat(after)
            except ValueError:
                raise ValueError(f"Invalid date format for 'after': {after}. Please use ISO-8601 format.")
            
            where_clauses.append("messages.timestamp > ?")
            params.append(after)

        if before:
            try:
                before = datetime.fromisoformat(before)
            except ValueError:
                raise ValueError(f"Invalid date format for 'before': {before}. Please use ISO-8601 format.")
            
            where_clauses.append("messages.timestamp < ?")
            params.append(before)

        if sender_phone_number:
            where_clauses.append("messages.sender = ?")
            params.append(sender_phone_number)
            
        if chat_jid:
            where_clauses.append("messages.chat_jid = ?")
            params.append(chat_jid)
            
        if query:
            where_clauses.append("LOWER(messages.content) LIKE LOWER(?)")
            params.append(f"%{query}%")
            
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add pagination
        offset = page * limit
        query_parts.append("ORDER BY messages.timestamp DESC")
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])
        
        cursor.execute(" ".join(query_parts), tuple(params))
        messages = cursor.fetchall()
        
        result = []
        for msg in messages:
            message = Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7]
            )
            result.append(message)
            
        if include_context and result:
            # Add context for each message
            messages_with_context = []
            for msg in result:
                context = get_message_context(msg.id, context_before, context_after)
                messages_with_context.extend(context.before)
                messages_with_context.append(context.message)
                messages_with_context.extend(context.after)
            
            return format_messages_list(messages_with_context, show_chat_info=True)
            
        # Format and display messages without context
        if not result and chat_jid:
            return _SYNC_HINT
        return format_messages_list(result, show_chat_info=True)
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5
) -> MessageContext:
    """Get context around a specific message."""
    try:
        conn = _connect_messages()
        cursor = conn.cursor()
        
        # Get the target message first
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.chat_jid, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.id = ?
        """, (message_id,))
        msg_data = cursor.fetchone()
        
        if not msg_data:
            raise ValueError(f"Message with ID {message_id} not found")
            
        target_message = Message(
            timestamp=datetime.fromisoformat(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[8]
        )
        
        # Get messages before
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ? AND messages.timestamp < ?
            ORDER BY messages.timestamp DESC
            LIMIT ?
        """, (msg_data[7], msg_data[0], before))
        
        before_messages = []
        for msg in cursor.fetchall():
            before_messages.append(Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7]
            ))
        
        # Get messages after
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ? AND messages.timestamp > ?
            ORDER BY messages.timestamp ASC
            LIMIT ?
        """, (msg_data[7], msg_data[0], after))
        
        after_messages = []
        for msg in cursor.fetchall():
            after_messages.append(Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7]
            ))
        
        return MessageContext(
            message=target_message,
            before=before_messages,
            after=after_messages
        )
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active"
) -> List[Chat]:
    """Get chats matching the specified criteria."""
    try:
        conn = _connect_messages()
        has_session = _attach_session(conn)
        cursor = conn.cursor()

        # Build base query
        query_parts = ["""
            SELECT
                chats.jid,
                chats.name,
                chats.last_message_time,
                messages.content as last_message,
                messages.sender as last_sender,
                messages.is_from_me as last_is_from_me
            FROM chats
        """]
        
        if include_last_message:
            query_parts.append("""
                LEFT JOIN messages ON chats.jid = messages.chat_jid 
                AND chats.last_message_time = messages.timestamp
            """)
            
        where_clauses = []
        params = []
        
        if query:
            where_clauses.append("(LOWER(chats.name) LIKE LOWER(?) OR chats.jid LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])
            
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add sorting
        order_by = "chats.last_message_time DESC" if sort_by == "last_active" else "chats.name"
        query_parts.append(f"ORDER BY {order_by}")
        
        # Add pagination
        offset = (page ) * limit
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])
        
        cursor.execute(" ".join(query_parts), tuple(params))
        chats = cursor.fetchall()
        
        result = []
        for chat_data in chats:
            name = chat_data[1]
            if _needs_name_resolution(name, chat_data[0]):
                name = _resolve_contact_name(conn, chat_data[0], has_session, fallback=name)
            chat = Chat(
                jid=chat_data[0],
                name=name,
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5]
            )
            result.append(chat)

        return result

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def search_contacts(query: str) -> List[Contact]:
    """Search contacts by name or phone number.

    Searches both the synced chats (messages.db) and whatsmeow's full address book
    (whatsmeow_contacts), and translates phone numbers to LIDs via the lid map so that
    LID-keyed chats — whose JID never contains the phone number — are still found.
    """
    # An empty query would `LIKE '%%'` against every contact and then drive per-row
    # name resolution over all of them — reject it rather than scan the whole book.
    if not query or not query.strip():
        return []
    try:
        conn = _connect_messages()
        has_session = _attach_session(conn)
        cursor = conn.cursor()

        search_pattern = '%' + query + '%'
        # jid -> name, in insertion order (chats first, then address-book-only contacts).
        found: dict = {}

        # 1. Chats we've actually synced (covers groups-excluded direct chats).
        cursor.execute("""
            SELECT DISTINCT jid, name
            FROM chats
            WHERE (LOWER(name) LIKE LOWER(?) OR LOWER(jid) LIKE LOWER(?))
                AND jid NOT LIKE '%@g.us'
            ORDER BY name, jid
            LIMIT 50
        """, (search_pattern, search_pattern))
        for jid, name in cursor.fetchall():
            found[jid] = name

        if has_session:
            # 2. The full address book — contacts you've never opened a chat with, and
            #    LID contacts whose chat name is just a number.
            cursor.execute("""
                SELECT their_jid, full_name, first_name, push_name, business_name
                FROM wa.whatsmeow_contacts
                WHERE their_jid NOT LIKE '%@g.us'
                    AND (LOWER(full_name) LIKE LOWER(?)
                         OR LOWER(first_name) LIKE LOWER(?)
                         OR LOWER(push_name) LIKE LOWER(?)
                         OR LOWER(business_name) LIKE LOWER(?)
                         OR their_jid LIKE ?)
                LIMIT 50
            """, (search_pattern, search_pattern, search_pattern, search_pattern, search_pattern))
            for jid, full_name, first_name, push_name, business_name in cursor.fetchall():
                name = full_name or first_name or push_name or business_name
                if jid not in found or _needs_name_resolution(found[jid], jid):
                    found[jid] = name

            # 3. Phone-number query: translate a digit string through the lid map in both
            #    directions so a number finds its LID chat (and vice versa). Require enough
            #    digits to be selective — a 1-2 digit query matches almost every row — and
            #    cap the rows we resolve so a short/loose query can't trigger a full scan
            #    plus per-row contact resolution over the whole table.
            digits = _digits(query)
            if len(digits) >= 5:
                cursor.execute(
                    "SELECT lid, pn FROM wa.whatsmeow_lid_map WHERE pn LIKE ? OR lid LIKE ? LIMIT 50",
                    (f"%{digits}%", f"%{digits}%"),
                )
                for lid, pn in cursor.fetchall():
                    for candidate in (f"{lid}@lid", f"{pn}@s.whatsapp.net"):
                        if candidate in found:
                            continue
                        # Only surface a translated JID if it's a real contact; a bare
                        # lid-map row with no contact entry would just be a nameless dup.
                        name = _resolve_contact_name(conn, candidate, has_session)
                        if name:
                            found[candidate] = name

        # For each candidate JID, look up how recently its synced chat was active, so we
        # can collapse the multiple JIDs a single person can have (phone JID + LID +
        # device suffixes) into one entry, keeping the JID whose chat is the most recently
        # active — i.e. the live LID chat over a stale phone-number chat — so downstream
        # message lookups land on the current conversation.
        chat_times: dict = {}
        if found:
            placeholders = ",".join("?" * len(found))
            for jid, last_time in conn.execute(
                f"SELECT jid, last_message_time FROM chats WHERE jid IN ({placeholders})",
                list(found),
            ).fetchall():
                chat_times[jid] = _ts(last_time)

        by_phone: dict = {}
        best_time: dict = {}
        order: List[str] = []
        for jid, name in found.items():
            if _needs_name_resolution(name, jid):
                name = _resolve_contact_name(conn, jid, has_session, fallback=name)
            phone = _phone_for_jid(conn, jid, has_session)
            contact = Contact(phone_number=phone, name=name, jid=jid)
            key = phone or jid
            incoming = chat_times.get(jid)
            if key not in by_phone:
                by_phone[key] = contact
                best_time[key] = incoming
                order.append(key)
            elif incoming and (best_time[key] is None or incoming > best_time[key]):
                by_phone[key] = contact  # a more recently active chat wins
                best_time[key] = incoming

        return [by_phone[key] for key in order][:50]

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> List[Chat]:
    """Get all chats involving the contact.
    
    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    try:
        conn = _connect_messages()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            JOIN messages m ON c.jid = m.chat_jid
            WHERE m.sender = ? OR c.jid = ?
            ORDER BY c.last_message_time DESC
            LIMIT ? OFFSET ?
        """, (jid, jid, limit, page * limit))
        
        chats = cursor.fetchall()
        
        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5]
            )
            result.append(chat)
            
        return result
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_last_interaction(jid: str) -> str:
    """Get most recent message involving the contact."""
    try:
        conn = _connect_messages()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                m.timestamp,
                m.sender,
                c.name,
                m.content,
                m.is_from_me,
                c.jid,
                m.id,
                m.media_type
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.sender = ? OR c.jid = ?
            ORDER BY m.timestamp DESC
            LIMIT 1
        """, (jid, jid))
        
        msg_data = cursor.fetchone()
        
        if not msg_data:
            return None
            
        message = Message(
            timestamp=datetime.fromisoformat(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[7]
        )
        
        return format_message(message)
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_chat(chat_jid: str, include_last_message: bool = True) -> Optional[Chat]:
    """Get chat metadata by JID."""
    try:
        conn = _connect_messages()
        has_session = _attach_session(conn)
        cursor = conn.cursor()

        query = """
            SELECT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
        """

        if include_last_message:
            query += """
                LEFT JOIN messages m ON c.jid = m.chat_jid
                AND c.last_message_time = m.timestamp
            """

        query += " WHERE c.jid = ?"

        cursor.execute(query, (chat_jid,))
        chat_data = cursor.fetchone()

        # If a phone JID was passed but the chat is stored under a LID (or vice versa),
        # retry against the alternate JID before giving up.
        if not chat_data and has_session:
            for alt in _candidate_jids_for_phone(conn, _digits(chat_jid), has_session):
                if alt == chat_jid:
                    continue
                cursor.execute(query, (alt,))
                chat_data = cursor.fetchone()
                if chat_data:
                    break

        if not chat_data:
            return _SYNC_HINT

        name = chat_data[1]
        if _needs_name_resolution(name, chat_data[0]):
            name = _resolve_contact_name(conn, chat_data[0], has_session, fallback=name)

        return Chat(
            jid=chat_data[0],
            name=name,
            last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5]
        )

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_direct_chat_by_contact(sender_phone_number: str) -> Optional[Chat]:
    """Get chat metadata by sender phone number.

    Resolves the phone number to every JID it could be stored under (including a
    LID JID via whatsmeow's lid map), so chats keyed by LID are found.
    """
    try:
        conn = _connect_messages()
        has_session = _attach_session(conn)
        cursor = conn.cursor()

        candidates = _candidate_jids_for_phone(conn, sender_phone_number, has_session)
        placeholders = ",".join("?" * len(candidates))
        cursor.execute(f"""
            SELECT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            LEFT JOIN messages m ON c.jid = m.chat_jid
                AND c.last_message_time = m.timestamp
            WHERE c.jid IN ({placeholders}) AND c.jid NOT LIKE '%@g.us'
            ORDER BY c.last_message_time DESC
            LIMIT 1
        """, candidates)

        chat_data = cursor.fetchone()

        if not chat_data:
            # Fall back to the original substring match in case the number is embedded
            # in a JID we didn't enumerate (e.g. a device-suffixed JID).
            cursor.execute("""
                SELECT c.jid, c.name, c.last_message_time, m.content, m.sender, m.is_from_me
                FROM chats c
                LEFT JOIN messages m ON c.jid = m.chat_jid AND c.last_message_time = m.timestamp
                WHERE c.jid LIKE ? AND c.jid NOT LIKE '%@g.us'
                LIMIT 1
            """, (f"%{_digits(sender_phone_number)}%",))
            chat_data = cursor.fetchone()

        if not chat_data:
            return _SYNC_HINT

        name = chat_data[1]
        if _needs_name_resolution(name, chat_data[0]):
            name = _resolve_contact_name(conn, chat_data[0], has_session, fallback=name)

        return Chat(
            jid=chat_data[0],
            name=name,
            last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5]
        )

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def request_history_sync(chat_jid: str) -> Tuple[bool, str]:
    """Request additional history for a chat that exists in the local database."""
    try:
        response = requests.post(f"{WHATSAPP_API_BASE_URL}/sync", json={"chat_jid": chat_jid})
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        return False, f"Error: HTTP {response.status_code} - {response.text}"
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"


def send_message(recipient: str, message: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "message": message,
        }
        
        response = requests.post(url, json=payload)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def send_file(recipient: str, media_path: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        if not media_path:
            return False, "Media path must be provided"
        
        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"
        
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "media_path": media_path
        }
        
        response = requests.post(url, json=payload)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def send_audio_message(recipient: str, media_path: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        if not media_path:
            return False, "Media path must be provided"
        
        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"

        if not media_path.endswith(".ogg"):
            try:
                media_path = audio.convert_to_opus_ogg_temp(media_path)
            except Exception as e:
                return False, f"Error converting file to opus ogg. You likely need to install ffmpeg: {str(e)}"
        
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "media_path": media_path
        }
        
        response = requests.post(url, json=payload)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def download_media(message_id: str, chat_jid: str) -> Optional[str]:
    """Download media from a message and return the local file path.
    
    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message
    
    Returns:
        The local file path if download was successful, None otherwise
    """
    try:
        url = f"{WHATSAPP_API_BASE_URL}/download"
        payload = {
            "message_id": message_id,
            "chat_jid": chat_jid
        }
        
        response = requests.post(url, json=payload)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("success", False):
                path = result.get("path")
                print(f"Media downloaded successfully: {path}")
                return path
            else:
                print(f"Download failed: {result.get('message', 'Unknown error')}")
                return None
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
            return None
            
    except requests.RequestException as e:
        print(f"Request error: {str(e)}")
        return None
    except json.JSONDecodeError:
        print(f"Error parsing response: {response.text}")
        return None
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return None
