import sys
sys.path.insert(0, '.')

import argparse
import json
import os
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import List, Dict, Set, Any, Optional

from src.storage.core import VectorSearchResult
from src.storage.vector_manager import VectorStoreManager
from src.utils.logger import get_logger
from src.utils.preprocessing import remove_digits
from src.utils.run_manager import get_run_path, resolve_run_id


def normalize_name(name: str) -> str:
    """Нормализует имя: нижний регистр, удаление спецсимволов."""
    return re.sub(r'[^a-z0-9]', '', name.lower())


def sliding_window_match(candidate: str, target: str) -> bool:
    """
    Проверяет, содержится ли target в candidate как подпоследовательность.
    Пример: "user.address.city" содержит "address.city" -> True
    """
    cand_parts = normalize_name(candidate).split('.')
    target_parts = normalize_name(target).split('.')
    
    if len(target_parts) > len(cand_parts):
        return False
    
    for i in range(len(cand_parts) - len(target_parts) + 1):
        window = cand_parts[i:i + len(target_parts)]
        if window == target_parts:
            return True

    return False


def filter_results_by_name(
    results: List[VectorSearchResult],
    target_table: Optional[str] = None,
    target_column: Optional[str] = None,
    max_results: int = 10
) -> List[VectorSearchResult]:
    """
    Фильтрует результаты векторного поиска с помощью fallback-стратегий:
    1. Точное совпадение (после нормализации)
    2. Совпадение без учёта цифр
    3. Совпадение только столбца
    """
    if not target_table and not target_column:
        return results[:max_results]
    
    filtered: List[VectorSearchResult] = []
    
    # Уровень 1: точное совпадение
    for r in results:
        table_match = target_table is None or normalize_name(r.metadata.get("table_name", "")) == normalize_name(target_table)
        col_match = target_column is None or normalize_name(r.metadata.get("column_name", "")) == normalize_name(target_column)
        if table_match and col_match:
            filtered.append(r)
    
    if len(filtered) >= max_results:
        return filtered[:max_results]
    
    # Уровень 2: совпадение с удалением цифр
    for r in results:
        if r in filtered:
            continue
        table_match = target_table is None or sliding_window_match(remove_digits(r.metadata.get("table_name", "")), remove_digits(target_table))
        col_match = target_column is None or remove_digits(normalize_name(r.metadata.get("column_name", ""))) == remove_digits(normalize_name(target_column))
        if table_match and col_match:
            filtered.append(r)
    
    if len(filtered) >= max_results:
        return filtered[:max_results]
    
    # Уровень 3: только столбец (если таблица не указана)
    if target_column and target_table is None:
        for r in results:
            if r in filtered:
                continue
            if normalize_name(r.metadata.get("column_name", "")) == normalize_name(target_column):
                filtered.append(r)
    
    return filtered[:max_results]


def enrich_nested_results(
    search_results: List[VectorSearchResult],
    schema_meta: Dict,
    max_expand_per_table: int = 5,
    max_total_results: int = 15
) -> List[VectorSearchResult]:
    """
    Дополняет результаты поиска родительскими структурами и соседними вложенными столбцами.
    
    Args:
        search_results: Результаты векторного поиска.
        schema_meta: Метаданные схемы из _meta.json.
        max_expand_per_table: Макс. число соседних столбцов на один родитель.
        max_total_results: Лимит на общий размер результата.
    """
    enriched: List[VectorSearchResult] = []
    seen_cols: Set[str] = set()
    tables_to_expand: Dict[str, Set[str]] = defaultdict(set)
    
    # 1. Добавляем исходные результаты и ищем вложенные столбцы
    for res in search_results:
        col = res.metadata.get("column_name", "")
        table = res.metadata.get("table_name", "")
        full_key = f"{table}.{col}"
        
        if full_key in seen_cols:
            continue

        seen_cols.add(full_key)
        enriched.append(res)
        
        # Если столбец вложенный, помечаем таблицу и префикс для расширения
        if "." in col:
            prefix = col.split(".")[0]
            tables_to_expand[table].add(prefix)
            
    # 2. Для каждой таблицы находим родительские и соседние столбцы
    for table, prefixes in tables_to_expand.items():
        table_data = schema_meta.get("tables", {}).get(table, {})
        all_cols = table_data.get("columns", [])
        all_types = table_data.get("types", [])
        all_descs = table_data.get("descriptions", [])
        all_vals = table_data.get("descriptions", [])
        col_info = dict(zip(all_cols, zip(all_types, all_descs, all_vals)))
        
        expanded_count = 0
        for prefix in prefixes:
            # Добавляем сам родительский столбец (если он есть)
            if prefix in col_info and f"{table}.{prefix}" not in seen_cols:
                typ, desc, _ = col_info[prefix]
                enriched.append(VectorSearchResult(
                    id=None, text="",
                    metadata={
                        "table_name": table,
                        "column_name": prefix,
                        "column_type": typ,
                        "description": desc or "",
                        "column_vals": [],
                        "role": "parent_structure"
                    },
                    score=0.0,  # Контекстный, не ранжируемый
                    rank=len(enriched)
                ))
                seen_cols.add(f"{table}.{prefix}")
                
            # Добавляем соседние вложенные столбцы
            for col in all_cols:
                if col.startswith(f"{prefix}.") and f"{table}.{col}" not in seen_cols:
                    typ, desc, vals = col_info[col]
                    enriched.append(VectorSearchResult(
                        text="",
                        metadata={
                            "table_name": table,
                            "column_name": col,
                            "column_type": typ,
                            "description": desc or "",
                            "column_vals": vals or [],
                            "role": "sibling_column"
                        },
                        score=0.0,
                        rank=len(enriched)
                    ))
                    seen_cols.add(f"{table}.{col}")
                    expanded_count += 1
                    
            if expanded_count >= max_expand_per_table:
                break
                
    # 3. Сортировка: точные совпадения -> родитель -> соседи
    def sort_key(r):
        role = r.metadata.get("role", "exact_match")
        order = {"exact_match": 0, "parent_structure": 1, "sibling_column": 2}
        return (order.get(role, 3), -r.score)
        
    enriched.sort(key=sort_key)
    return enriched[:max_total_results]


def format_schema_for_prompt(results: List[VectorSearchResult]) -> str:
    """
    Форматирует результаты в читаемый вид для промпта LLM.
    """
    lines = []
    table_cols = defaultdict(list)
    
    for r in results:
        table = r.metadata["table_name"]
        col = r.metadata["column_name"]
        typ = r.metadata["column_type"]
        desc = r.metadata.get("description", "")
        vals = r.metadata.get("column_vals", [])
        role = r.metadata.get("role", "exact_match")
        
        marker = ""
        if role == "parent_structure":
            marker = " [STRUCT]"
        elif role == "sibling_column":
            marker = " [CONTEXT]"
            
        desc_text = f": {desc.rstrip()}" if desc else ""
        values_text = f"{'.' if not desc.endswith(('.', '!', '?')) else ''} | Sampled values: {', '.join(vals)}" if vals else ""
        table_cols[table].append(f"  - {col} ({typ}){marker}{desc_text}{values_text}")
    
    for table, cols in table_cols.items():
        lines.append(f"Table: {table}")
        lines.extend(cols)
        lines.append("")
        
    return "\n".join(lines).strip()


class RetrievalCache:
    """
    Отслеживает использованные индексы для каждого instance_id в рамках run_id.
    Позволяет итеративно запрашивать результаты без дублирования.
    """
    
    def __init__(
        self,
        run_id: str,
        cache_dir: Optional[str] = None,  # Если None, используется runs/{run_id}/cache
        runs_root: str = "logs/runs"
    ):
        self.run_id = run_id
        self.runs_root = runs_root
        
        # Определяем путь к кэшу
        if cache_dir:
            self.cache_dir = cache_dir
        else:
            self.cache_dir = get_run_path(run_id, runs_root, stage="schema_linking/retrieval_cache")
        
        os.makedirs(self.cache_dir, exist_ok=True)
        self.logger = get_logger("retrieval_cache", log_file=os.path.join(runs_root, run_id, "schema_linking", "retrieve.log"))
        self.logger.info(f"RetrievalCache initialized at {self.cache_dir} (run_id={run_id})")
    
    def _get_cache_path(self, instance_id: str) -> str:
        return os.path.join(self.cache_dir, f"{instance_id}.json")
    
    def get_used_indices(self, instance_id: str) -> Set[int]:
        """Возвращает множество уже использованных индексов для instance_id."""
        path = self._get_cache_path(instance_id)
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(int(idx) for idx in data.get("used_indices", []))
            except Exception as e:
                self.logger.warning(f"Failed to load cache for {instance_id}: {e}")
        return set()
    
    def add_used_indices(self, instance_id: str, indices: list[int]) -> None:
        """Добавляет новые индексы в кэш (объединяет с существующими)."""
        existing = self.get_used_indices(instance_id)
        existing.update(int(idx) for idx in indices)
        
        path = self._get_cache_path(instance_id)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({"used_indices": sorted(list(existing))}, f, indent=2)
        except Exception as e:
            self.logger.warning(f"Failed to save cache for {instance_id}: {e}")
    
    def clear(self, instance_id: str) -> None:
        """Очищает кэш для instance_id."""
        path = self._get_cache_path(instance_id)
        if os.path.exists(path):
            os.remove(path)
            self.logger.info(f"Cleared cache for {instance_id}")
    
    def clear_all(self) -> None:
        """Очищает весь кэш для текущего run_id."""
        import shutil
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Cleared all cache for run_id={self.run_id}")
    
    def get_status(self, instance_id: str, total_available: int) -> Dict[str, Any]:
        """Возвращает статус использования кэша."""
        used = len(self.get_used_indices(instance_id))
        return {
            "run_id": self.run_id,
            "instance_id": instance_id,
            "total_available": total_available,
            "used_count": used,
            "remaining_count": max(0, total_available - used),
            "is_complete": used >= total_available
        }
    
    def cache_union(self, instance_db: Dict[str, str]):
        all_indices = {}
        for file in os.listdir(self.cache_dir):
            with open(self.cache_dir, file, encoding="utf-8") as f:
                used_indices = json.load(f).get("used_indices")
            
            instance_id = file.rsplit(".", 1)[0]
            all_indices[instance_id] = {"db_id": instance_db[instance_id], "used_indices": used_indices}
        
        with open(os.path.join(self.cache_dir, "used_indices.json"), 'w', encoding='utf-8') as f:
            json.dump(all_indices, f)


class SchemaRetriever:
    def __init__(
        self,
        vsm: VectorStoreManager,
        cache: Optional[RetrievalCache] = None,
        initial_top_k: int = 80,
        expansion_top_k: int = 20,
        max_total_columns: int = 100
    ):
        self.vsm = vsm
        self.cache = cache or RetrievalCache()
        self.initial_top_k = initial_top_k
        self.expansion_top_k = expansion_top_k
        self.max_total_columns = max_total_columns
    
    def select_schema(
        self,
        instance_id: str,
        question: str,
        db_id: str,
        schema_meta: dict,
        force_refresh: bool = False
    ) -> List[dict]:
        """
        Итеративно подбирает столбцы для генерации SQL.
        """
        # 1. Загружаем уже использованные индексы
        used_ids = self.cache.get_used_indices(instance_id) if not force_refresh else set()
        
        # 2. Первый запрос: семантический поиск
        results = self.vsm.search_batch(
            queries_by_db={db_id: [question]},
            top_k=self.initial_top_k,
            filters={"db_id": db_id}
        )[db_id][question]
        
        # 3. Гибридное расширение: добавляем точные совпадения по имени
        results = filter_results_by_name(
            results, 
            target_table=None,
            target_column=None,
            max_results=self.initial_top_k
        )
        
        # 4. Фильтруем уже использованные
        new_results = [r for r in results if r.id not in used_ids]
        
        # 5. Обогащаем вложенными структурами
        enriched = enrich_nested_results(
            new_results, 
            schema_meta=schema_meta,
            max_expand_per_table=5,
            max_total_results=min(len(new_results) + 10, self.max_total_columns)
        )
        
        # 6. Сохраняем в кэш
        new_ids = {r.id for r in enriched if r.metadata.get("internal_id")}
        self.cache.add_used_indices(instance_id, list(new_ids))
        
        # 7. Преобразуем в формат для агента/генерации
        return [
            {
                "table": r.metadata["table_name"],
                "column": r.metadata["column_name"],
                "type": r.metadata["column_type"],
                "description": r.metadata.get("description", ""),
                "role": r.metadata.get("role", "retrieved"),
                "score": r.score
            }
            for r in enriched
        ]
    
    def expand_schema(
        self,
        instance_id: str,
        question: str,
        db_id: str,
        schema_meta: dict
    ) -> List[dict]:
        """Запрашивает следующие top-K результатов, исключая уже полученные."""
        return self.select_schema(
            instance_id=instance_id,
            question=question,
            db_id=db_id,
            schema_meta=schema_meta,
            force_refresh=False  # Использует кэш
        )

def retrieve_columns(
        run_name, 
        vsm: VectorStoreManager,
        tasks: Optional[List[Dict[str, str]]] = None,
        input_data_root: str = "Spider2/spider2-lite",
        data_root: str = "data",
        storage_root: str = "storage", 
        topk: int = 100,
        max_workers: int = 2,
        force_refresh: bool = False
    ):
    run_id = resolve_run_id(input_data_root=input_data_root, custom_suffix=run_name, use_latest=True)
    cache = RetrievalCache(run_id)
    retriever = SchemaRetriever(
        vsm=vsm, cache=cache, initial_top_k=topk, 
        expansion_top_k=topk // 5, max_total_columns=topk
    )

    if tasks is None:
        tasks_file = [file for file in os.listdir(os.path(data_root, input_data_root)) 
                      if file.endswith('.jsonl')][0]
        with open(os.path.join(data_root, input_data_root, tasks_file), 'r', encoding='utf-8') as f:
            tasks = [json.loads(line.strip()) for line in f.readlines()]

    q_key = "question"
    if "question" not in tasks[0]:
        q_key = "instuction"

    if input_data_root == "Spider2/spider2-lite":
        inst2dialect = {"sf": "snowflake", "bq": "bigquery", "ga": "bigquery", "local": "sqlite"}
        tasks = [(instance["instance_id"], 
                  inst2dialect[remove_digits(instance["instance_id"]).split("_")[0]] + "_" + instance["db_id"], 
                  instance[q_key])
                 for instance in tasks]
    else:
        tasks = [(instance["instance_id"], 
                  instance.get("dialect", "") + ("_" if instance.get("dialect") else "") + instance["db_id"], 
                  instance[q_key])
                 for instance in tasks]
    
    instance_db = {task[0]: task[1] for task in tasks}
    tasks = [task for task in tasks if os.path.exists(cache._get_cache_path(task[0]))]

    def process_instance(instance_data):
        nonlocal retriever, storage_root, input_data_root, force_refresh
        loaded_meta = json.load(open(
            os.path.join(storage_root, input_data_root, "schema_cache", 
                         instance_data[1] + "_meta.json"), 
            encoding='utf-8')
        )
        selected_columns = retriever.select_schema(
            instance_id=instance_data[0],
            question=instance_data[2],
            db_id=instance_data[1],
            schema_meta=loaded_meta[instance_data[1]],
            force_refresh=force_refresh
        )
        if len(selected_columns) < retriever.max_total_columns:
            more_columns = retriever.expand_schema(
                instance_id=instance_data[0],
                question=instance_data[2],
                db_id=instance_data[1],
                schema_meta=loaded_meta[instance_data[1]]
            )
            selected_columns.extend(more_columns)

    with ThreadPoolExecutor(max_workers) as executor:
        futures = [executor.submit(process_instance, task) for task in tasks]
        with tqdm(total=len(futures), desc="Retrieve for questions", ncols=160, leave=True) as pbar:
            for _ in as_completed(futures):
                pbar.update(1)
    
    cache.cache_union(instance_db)
    vsm.close_all()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Finds columns relevant to questions.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "input_data_root", type=str, default="Spider2/spider2-lite",
        help="Относительный путь к папке датасета внутри data_root. "
             "Используется для получения вопросов."
    )
    parser.add_argument(
        "run_name", type=str, default="", 
        help="Название запуска, использовавшегося для формирования логов в logs/runs директории."
    )
    parser.add_argument(
        "--data_root", type=str, default="data",
        help="Путь к папке с входными данными"
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
        "--topk", type=int, default=100,
        help="Число результатов поиска на каждый пример."
    )
    parser.add_argument(
        "--batch_size", type=int, default=256,
        help="Размер батча для поиска."
    )
    parser.add_argument(
        "--max_workers", type=int, default=2,
        help="Количество параллельных потоков для векторного поиска в Qdrant. "
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
        "--force_refresh", action="store_true",
        help="Заново выполнить поиск столбцов и перезаписать кэш."
    )
    args = parser.parse_args()

    vsm = vsm = VectorStoreManager(
        storage_root=args.storage_root,
        location=args.location,
        max_cached_sessions=args.max_cached_sessions, 
        embedding_model=args.embedding_model,
        backend=args.backend,
        device=args.device,
        quantization=args.quantization,
        log_path=os.path.join("logs/dbs", args.input_data_root)
    )
    retrieve_columns(
        args.run_name, vsm, None, args.input_data_root, args.data_root, 
        args.storage_root, args.topk, args.max_workers, args.force_refresh
    )
