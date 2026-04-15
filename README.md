## Исходный код для исследования методов text-to-SQL.

![График изменения точности топ-1 метода на бенчмарках BIRD и Spider 2.0](images/benchmarks_ex.png)

## Текушие наработки

Для оценки используется бенчмарк (Spider 2.0)[https://github.com/xlang-ai/Spider2] (преимущественно датасет Spider 2.0-lite).

На данный момент проведён анализ затрат и ошибок двух baseline методов: (DIN-SQL)[https://arxiv.org/abs/2304.11015] и (DAIL-SQL)[https://arxiv.org/abs/2308.15363] и одного из топа по бенчмарку - (ReFoRCE)[https://arxiv.org/abs/2502.00675]. Результаты располагаются в папке `analysis`.

Планируется проанализировать метод (AutoLink)[https://arxiv.org/abs/2511.17190], также из топа Spider 2.0-lite.


