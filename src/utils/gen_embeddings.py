import sys
sys.path.insert(0, '.')

import argparse
import os
from typing import Dict, Optional

from src.storage.vector_manager import VectorStoreManager


def gen_column_embeddings(
        preprocessing_results: Dict[str, str] = None,
        input_data_root: str = "Spider2/spider2-lite",
        storage_root: str = "storage",
        location: Optional[str] = None,
        embedding_model: str = "microsoft/harrier-oss-v1-270m",
        device: str = "cpu", 
        quantization: bool = False,
        batch_size: int = 256,
        max_workers: int = 2,
        max_cached_sessions: int = 2, 
        backend: str = "qdrant",
        force_rebuild: bool = False,
        **kwargs
    ):
    vsm = VectorStoreManager(
        storage_root=storage_root,
        location=location,
        max_cached_sessions=max_cached_sessions, 
        embedding_model=embedding_model,
        backend=backend,
        device=device,
        quantization=quantization,
        log_path=os.path.join("logs/dbs", input_data_root)
    )

    if preprocessing_results is None:
        preprocessing_results = {
            file.rsplit('_', 1)[0]: os.path.join(storage_root, input_data_root, "schema_cache", file) 
            for file in os.listdir(os.path.join(storage_root, input_data_root, "schema_cache"))
            if file.endswith("_meta.json")
        }

    vsm.build_from_preprocessing_results(
        preprocessing_results=preprocessing_results,
        context_id=input_data_root,
        batch_size=batch_size,
        max_workers=max_workers,
        force_rebuild=force_rebuild
    )
    vsm.close_all()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Builds vector indexes for database schemas from preprocessed metadata files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "embed_type", type=str, default="column", choices=["column"],
        help="Тип генерируемых эмбеддингов. colums - столбцы."
    )
    parser.add_argument(
        "input_data_root", type=str, default="Spider2/spider2-lite",
        help="Относительный путь к папке датасета внутри storage_root. "
             "Используется для поиска schema_cache и как context_id для Qdrant."
    )
    parser.add_argument(
        "--storage_root", type=str, default="storage",
        help="Корневая директория для кэшированных схем и векторных баз данных."
    )
    parser.add_argument(
        "--location", type=str, default=None,
        help="URL локального сервера с векторной базой данных."
    )
    parser.add_argument(
        "--embedding_model", type=str, default="microsoft/harrier-oss-v1-270m",
        help="Идентификатор HuggingFace модели или локальный путь для создания эмбеддингов. "
             "Поддерживает модели с prompt-based кодированием (Harrier, Qwen3 и др.)."
    )
    parser.add_argument(
        "--device", type=str, default="cpu",  # choices=["cpu", "cuda", "cuda:0", "mps"],
        help="Устройство для инференса модели эмбеддингов. "
             "'cuda' использует доступный GPU, 'cpu' — процессор, 'mps' — Apple Silicon."
    )
    parser.add_argument(
        "--quantization", action="store_true",
        help="Включить int8 скалярное квантование векторов. Сокращает потребление RAM/диска в ~4 раза "
             "с минимальным влиянием на точность поиска. Рекомендуется для датасетов >50k столбцов."
    )
    parser.add_argument(
        "--batch_size", type=int, default=256,
        help="Размер батча генерируемых эмбеддингов."
    )
    parser.add_argument(
        "--max_workers", type=int, default=2,
        help="Количество параллельных потоков для кодирования батчей и upsert в Qdrant. "
             "Для CPU: 2-4. Для CUDA: 1."
    )
    parser.add_argument(
        "--max_cached_sessions", type=int, default=2,
        help="Максимальное число сессий векторного хранилища (разных датасетов) в RAM. "
             "Использует LRU-вытеснение для предотвращения OOM при работе с несколькими контекстами."
    )
    parser.add_argument(
        "--backend", type=str, default="qdrant", choices=["qdrant"],
        help="Движок векторной базы данных. На текущий момент поддерживается только Qdrant."
    )
    parser.add_argument(
        "--force_rebuild", action="store_true",
        help="Удалить существующую коллекцию и перестроить индекс с нуля. "
             "Используйте при смене модели эмбеддингов или для очистки повреждённых данных."
    )
    args = parser.parse_args()

    if args.embed_type == 'column':
        gen_column_embeddings(
            None, args.input_data_root, args.storage_root, args.location, 
            args.embedding_model, args.device, args.quantization, 
            args.batch_size, args.max_workers, args.max_cached_sessions, 
            args.backend, args.force_rebuild
        )
    else:
        raise NotImplementedError
