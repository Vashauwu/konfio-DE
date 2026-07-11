"""
Motor de DAG mínimo para orquestar el pipeline con dependencias EXPLÍCITAS
entre pasos.

Por qué no Airflow/Dagster: para un batch job de una sola corrida (sin
scheduling, sin backfills, sin necesidad de UI), esas herramientas son
infraestructura sin beneficio real — el requisito del enunciado es que las
dependencias sean claras y verificables, no que exista un orquestador
externo. Este motor cubre exactamente eso:

- Cada paso se declara como un `Step` (nombre, función, dependencias).
- Se resuelve el orden de ejecución con orden topológico (Kahn).
- Detecta ciclos y dependencias no declaradas ANTES de ejecutar nada.
- Loggea el grafo completo antes de correr, así queda visible en los logs
  qué paso depende de cuáles otros — auditable sin leer el código.
- Cada resultado se guarda en un `context` compartido bajo la llave del
  nombre del paso, disponible para los pasos que dependen de él.
"""
from dataclasses import dataclass, field
from typing import Any, Callable

from src.common import get_logger

logger = get_logger("dag")


@dataclass
class Step:
    name: str
    func: Callable[[dict], Any]
    depends_on: list = field(default_factory=list)


class DAG:
    def __init__(self):
        self.steps: dict[str, Step] = {}

    def add_step(self, name: str, func: Callable[[dict], Any], depends_on: list | None = None) -> "DAG":
        if name in self.steps:
            raise ValueError(f"El paso '{name}' ya está registrado en el DAG.")
        self.steps[name] = Step(name=name, func=func, depends_on=depends_on or [])
        return self

    def _topological_order(self) -> list:
        for name, step in self.steps.items():
            for dep in step.depends_on:
                if dep not in self.steps:
                    raise ValueError(
                        f"El paso '{name}' depende de '{dep}', que no está registrado en el DAG."
                    )

        in_degree = {name: 0 for name in self.steps}
        graph = {name: [] for name in self.steps}
        for name, step in self.steps.items():
            for dep in step.depends_on:
                graph[dep].append(name)
                in_degree[name] += 1

        queue = sorted([n for n, d in in_degree.items() if d == 0])
        order = []
        while queue:
            queue.sort()  # orden determinista entre pasos sin dependencia mutua
            n = queue.pop(0)
            order.append(n)
            for m in graph[n]:
                in_degree[m] -= 1
                if in_degree[m] == 0:
                    queue.append(m)

        if len(order) != len(self.steps):
            remaining = set(self.steps) - set(order)
            raise ValueError(f"Ciclo detectado en el DAG entre los pasos: {remaining}")
        return order

    def log_graph(self) -> None:
        logger.info("Grafo de dependencias del pipeline:")
        for name, step in self.steps.items():
            deps = ", ".join(step.depends_on) if step.depends_on else "(sin dependencias)"
            logger.info(f"  {name}  <-  {deps}")

    def run(self, context: dict) -> dict:
        order = self._topological_order()
        self.log_graph()
        logger.info(f"Orden de ejecución resuelto: {' -> '.join(order)}")

        for name in order:
            step = self.steps[name]
            logger.info(f"[DAG] Ejecutando '{name}' (depende de: {step.depends_on or 'nada'})...")
            context[name] = step.func(context)
        return context
