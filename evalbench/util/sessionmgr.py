import os
import tempfile
from threading import Thread, Lock
import logging
import time
from absl import app
import uuid

def _get_session_resources_path() -> str:
    primary_path = "/tmp_sessions/"
    try:
        os.makedirs(primary_path, exist_ok=True)
        return primary_path
    except (PermissionError, OSError):
        fallback_path = os.path.join(tempfile.gettempdir(), "sessions")
        os.makedirs(fallback_path, exist_ok=True)
        return fallback_path

SESSION_RESOURCES_PATH = _get_session_resources_path()


class RWLock:
    def __init__(self):
        self.lock = Lock()
        self.write_lock = Lock()
        self.readers = 0

    def acquire_read(self):
        with self.lock:
            self.readers += 1
            if self.readers == 1:
                self.write_lock.acquire()

    def release_read(self):
        with self.lock:
            self.readers -= 1
            if self.readers == 0:
                self.write_lock.release()

    def acquire_write(self):
        self.write_lock.acquire()

    def release_write(self):
        self.write_lock.release()


class SessionManager:
    def __init__(
        self,
    ):
        self.running = True
        self.sessions = {}
        self.ttl = 10800
        self.lock = RWLock()
        self.load_sessions_from_disk()
        logging.debug("Starting reaper...")
        reaper = Thread(target=self.reaper, args=[])
        reaper.daemon = True
        reaper.start()

    def load_sessions_from_disk(self):
        try:
            if not os.path.exists(SESSION_RESOURCES_PATH):
                return
            for sid in os.listdir(SESSION_RESOURCES_PATH):
                dir_path = os.path.join(SESSION_RESOURCES_PATH, sid)
                if os.path.isdir(dir_path):
                    mtime = os.path.getmtime(dir_path)
                    logging.info(f"Loading session {sid} from disk with mtime {mtime}.")
                    self.sessions[sid] = {
                        "create_ts": mtime,
                        "session_id": sid,
                    }
        except Exception as e:
            logging.error(f"Error loading sessions from disk: {e}")

    def set_ttl(self, ttl):
        self.ttl = ttl

    def get_ttl(self):
        return self.ttl

    def get_session(self, session_id):
        self.lock.acquire_read()
        try:
            return self.sessions.get(session_id)
        finally:
            self.lock.release_read()

    def write_resource_files(self, session_id, resources):
        if not session_id or session_id.strip() in {"", ".", ".."}:
            raise ValueError(f"Invalid session_id: {session_id}")
        safe_session_id = os.path.basename(os.path.normpath(session_id))
        base_path = os.path.abspath(SESSION_RESOURCES_PATH).rstrip(os.sep) + os.sep
        session_dir = os.path.abspath(os.path.join(SESSION_RESOURCES_PATH, safe_session_id)).rstrip(os.sep) + os.sep
        if not session_dir.startswith(base_path):
            raise ValueError(f"Invalid session_id: {session_id}")
        for resource in resources:
            safe_rel_path = os.path.normpath(resource.address).lstrip("/")
            full_path = os.path.abspath(os.path.join(session_dir, safe_rel_path))
            if not full_path.startswith(session_dir):
                raise ValueError(f"Invalid resource address: {resource.address}")
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "wb") as f:
                f.write(resource.content)

    def prune_resource_files(self, session_id):
        if not session_id or session_id.strip() in {"", ".", ".."}:
            raise ValueError(f"Invalid session_id: {session_id}")
        safe_session_id = os.path.basename(os.path.normpath(session_id))
        base_path = os.path.abspath(SESSION_RESOURCES_PATH).rstrip(os.sep) + os.sep
        path = os.path.abspath(os.path.join(SESSION_RESOURCES_PATH, safe_session_id))
        if not (path + os.sep).startswith(base_path):
            raise ValueError(f"Invalid session_id: {session_id}")
        if not os.path.exists(path):
            return
        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                if os.path.islink(dir_path):
                    os.unlink(dir_path)
                else:
                    os.rmdir(dir_path)
        os.rmdir(path)

    def create_session(self, session_id):
        if not session_id or str(session_id).strip() in {"", ".", ".."}:
            raise ValueError(f"Invalid session_id: {session_id}")
        self.lock.acquire_write()
        try:
            if session_id in self.sessions:
                logging.info(f"Session {session_id} already exists.")
                return self.sessions[session_id]
            logging.info(f"Create session {session_id}.")
            self.sessions[session_id] = {
                "create_ts": time.time(), "session_id": session_id}
            return self.sessions[session_id]
        finally:
            self.lock.release_write()

    def get_sessions(self):
        self.lock.acquire_read()
        try:
            return dict(self.sessions)
        finally:
            self.lock.release_read()

    def delete_session(self, session_id):
        self.lock.acquire_write()
        try:
            if session_id in self.sessions:
                del self.sessions[session_id]
        finally:
            self.lock.release_write()

    def shutdown(self):
        self.running = False

    def reaper(self):
        while self.running:
            now = time.time()
            self.lock.acquire_read()
            try:
                to_delete = [sid for sid, s in self.sessions.items() if now - s["create_ts"] > self.ttl]
            finally:
                self.lock.release_read()

            for sid in to_delete:
                logging.info(f"Delete session {sid}.")
                self.delete_session(sid)
                try:
                    self.prune_resource_files(sid)
                except Exception as e:
                    logging.error(f"Error pruning session resources for {sid}: {e}")
            time.sleep(10)
