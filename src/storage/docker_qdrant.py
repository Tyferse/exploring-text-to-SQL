import subprocess
import sys
import time
from qdrant_client import QdrantClient


def ensure_qdrant_running(host: str = "localhost", port: int = 6333) -> bool:
    """Проверяет доступность Qdrant. Если нет — пытается запустить Docker."""
    try:
        client = QdrantClient(host=host, port=port, timeout=3)
        client.get_collections()
        print(f"Qdrant is running at {host}:{port}")
        return True
    except Exception:
        print(f"Qdrant not found at {host}:{port}. Attempting to start Docker container...")
        try:
            cmd = [
                "docker", "run", "-d",
                "-p", f"{port}:{port}", "-p", f"{port+1}:{port+1}",
                "-v", "qdrant_data:/qdrant/storage",
                "--name", "qdrant_local",
                "qdrant/qdrant"
            ]
            subprocess.run(cmd, check=True, capture_output=True)
            print("Waiting for Qdrant to initialize...")
            for _ in range(10):
                time.sleep(2)
                try:
                    QdrantClient(host=host, port=port, timeout=3).get_collections()
                    print("Qdrant started successfully.")
                    return True
                except:
                    continue
            print("Failed to connect to Qdrant after startup.")
            return False
        except FileNotFoundError:
            print("Docker not found. Install Docker or run manually:")
            print(f'   docker run -d -p {port}:{port} -v qdrant_data:/qdrant/storage qdrant/qdrant')
            return False
        except subprocess.CalledProcessError as e:
            if "already in use" in str(e):
                print("Container already exists. Starting it...")
                subprocess.run(["docker", "start", "qdrant_local"], capture_output=True)
                time.sleep(3)
                return ensure_qdrant_running(host, port)
            print(f"❌ Docker run failed: {e}")
            return False
