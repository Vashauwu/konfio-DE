# Konfio — Pipeline de Tipos de Cambio (Prueba Técnica Data Engineer)

Pipeline batch en PySpark que extrae tipos de cambio históricos de la
[Frankfurter API](https://api.frankfurter.dev), los transforma y enriquece,
detecta cambios incrementales (CDC), los persiste en tablas **Apache
Iceberg** transaccionales, y emite eventos de cambio tipo Kafka.

## Cómo ejecutar

```bash
docker compose up --build
```

Esto levanta el broker de **Kafka real** (`apache/kafka`, modo KRaft),
espera a que esté saludable, y luego construye/corre el pipeline de punta
a punta sin intervención manual. Al terminar:

- `./warehouse/` contiene el catálogo Iceberg (`db.tipos_cambio_enriquecidos`,
  `db.metricas_mensuales`, `db.anomalias`, `db.reporte_calidad`, `db.dim_currency`)
- `./events/` contiene un archivo `.json` por cada cambio detectado por el CDC
  (copia de auditoría local)
- El topic de Kafka `exchange-rate-events` contiene esos mismos eventos,
  publicados en tiempo real por el pipeline

### Ver time travel y schema evolution en vivo (demo)

```bash
docker compose --profile demo up iceberg-demo
```

Lista los snapshots de `db.tipos_cambio_enriquecidos`, compara el conteo de
filas entre el snapshot más antiguo y el estado actual (time travel), y
agrega una columna nueva a la tabla ya persistida sin reescribirla (schema
evolution). Para ver más de un snapshot con datos reales, corre
`docker compose up` una segunda vez antes de este demo.

Para volver a correrlo (y verificar idempotencia):

```bash
docker compose up
```

Si no hubo cambios en la fuente, verás en los logs `operation_type` = `NONE`
para todas las filas y ningún archivo nuevo en `/events/`.

### Correr el notebook de EDA

El notebook usa librerías que **no** están en la imagen de producción
(`jupyter`, `matplotlib`, `pandas`) — se mantienen separadas en
`requirements-dev.txt` para no inflar la imagen del pipeline. Con el
pipeline ya corrido al menos una vez (`docker compose up`):

```bash
docker compose run --rm -p 8888:8888 pipeline bash -c \
  "pip install -r requirements-dev.txt && jupyter notebook --ip 0.0.0.0 --allow-root --no-browser --notebook-dir=notebooks"
```

Abre la URL con el token que imprime la terminal (`http://127.0.0.1:8888/...`)
y corre `eda.ipynb`.

### Correr los tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

> Los tests unitarios usan una `SparkSession` local sin el catálogo Iceberg
> (no requieren red ni el jar de Iceberg), para poder correr rápido y
> aislados de la infraestructura de persistencia.

## Arquitectura

```
Frankfurter API
      │
      ▼
  extract.py          → DataFrame crudo, schema tipado
      │
      ▼
  transform.py         → limpieza → enriquecimiento → agregaciones/anomalías/calidad
      │
      ▼
  cdc.py                → compara contra Iceberg existente (hash de fila)
      │                    → operation_type: INSERT / UPDATE / DELETE / NONE
      ▼
  load.py                 → MERGE INTO Iceberg (transaccional, idempotente)
      │
      ├──► model.py         → dim_currency (esquema estrella)
      └──► events.py           → JSON por cambio detectado (/events/)
```

## Puntos extra implementados

### 1. DAG explícito con dependencias (`src/dag.py`)

`main.py` ya no encadena llamadas de función a ciegas: declara cada paso
como un nodo (`dag.add_step(nombre, función, depends_on=[...])`) y un motor
mínimo (`DAG.run`) resuelve el orden de ejecución por orden topológico
(Kahn), detecta ciclos/dependencias no declaradas antes de correr nada, y
loggea el grafo completo al inicio de cada corrida. No se usó Airflow ni
Dagster: para un batch de una sola corrida sin scheduling ni backfills,
esas herramientas son infraestructura sin beneficio real — lo que pide el
enunciado es que las dependencias sean *explícitas y verificables*, no que
exista un orquestador externo.

El grafo resultante tiene una rama principal y dos ramas que dependen de
ella en distintos puntos:
```
extract → clean → enrich → cdc → load_merge → read_current_state → {monthly_metrics, anomalies, quality_report}
                              ↘ events_build → {events_disk, events_kafka}
loan_requests ↘
               daily_risk (depende de read_current_state Y loan_requests)
dim_currency (sin dependencias — se puede recalcular en cualquier momento)
```

### 2. Segunda fuente de datos + join enriquecido (`src/secondary_source.py`)

Se simula un CSV de datos operativos internos (`data/solicitudes_credito.csv`,
generado sintéticamente para este ejercicio): volumen diario de solicitudes
de crédito, monto promedio y tasa de aprobación — el tipo de dato que
vendría de un core bancario/CRM en un escenario real.

Se une con la tasa MXN diaria en `db.riesgo_diario` (grano: 1 fila = 1 día
calendario), usando **forward-fill** para propagar la última tasa hábil
conocida a los fines de semana (el CSV de solicitudes sí tiene datos todos
los días, la fuente de tipo de cambio no). El valor de negocio es
justamente ese cruce: permite preguntar "¿la demanda de crédito se mueve
con la volatilidad cambiaria?" — una pregunta real de un equipo de riesgo,
más representativa del contexto de Konfio que agregar una tercera API de
tipos de cambio redundante. Ver el análisis en `notebooks/eda.ipynb`.

### 3. Compaction / rewrite de Iceberg (`src/load.py::rewrite_data_files`)

Además de `MERGE INTO`, particionado, time travel y schema evolution
(sección "Uso de Iceberg" abajo), se agregó compaction vía el procedimiento
nativo `CALL local.system.rewrite_data_files(...)`, que combina archivos
Parquet pequeños generados por corridas incrementales sucesivas — mitiga el
*small files problem* sin alterar los datos ni el historial de snapshots.
Demostrado en `docker compose --profile demo up iceberg-demo`.

### 4. CI/CD (`.github/workflows/ci.yml`)

Un workflow de GitHub Actions corre en cada push/PR a `main`: instala Java
17 + Python 3.11, corre `pytest tests/ -v`, valida sintaxis de todos los
módulos, y construye la imagen Docker como verificación final. No requiere
ningún secreto ni credencial — corre "out of the box" al hacer fork/clone.

### 5. Notebook de EDA (`notebooks/eda.ipynb`)

Análisis exploratorio sobre las tablas ya persistidas en Iceberg: series de
tiempo por moneda, anomalías resaltadas sobre la curva de MXN, volatilidad
mensual comparada entre monedas, y el cruce solicitudes de crédito vs.
tipo de cambio de la segunda fuente. Ver instrucciones de ejecución más
abajo.

### 6. Dominio opcional de pagos/tarjetas (`src/payments_domain.py`)

Se modeló el dominio opcional que menciona la sección 4.4 del enunciado:
`dim_customer` (1 fila = 1 cliente), `dim_card` (1 fila = 1 tarjeta, FK a
cliente), y `fact_transactions` (1 fila = 1 transacción, FK a tarjeta) —
datos sintéticos generados con semilla fija en `data/{customers,cards,
transactions}.csv`.

El punto interesante no es solo tener las tres tablas, sino que
`fact_transactions` se **integra con el pipeline principal**: cada
transacción se convierte a USD usando la tasa de `db.tipos_cambio_
enriquecidos` correspondiente a la moneda de la tarjeta y la fecha (mismo
patrón de forward-fill para fines de semana que en `secondary_source.py`).
Esto demuestra que el modelo no vive aislado — depende de datos ya
persistidos por otra rama del pipeline.

Se carga con *overwrite* completo (no MERGE INTO/CDC) porque los CSVs son
estáticos entre corridas — el requisito de CDC del enunciado ya está
cubierto en profundidad por `fact_exchange_rates`; replicar la misma
lógica aquí sería complejidad sin una razón de negocio real.

### 7. Great Expectations (`src/quality_ge.py`)

Se agregó como una capa de calidad **adicional y distinta** al reporte de
calidad nativo (`db.reporte_calidad`), no un reemplazo:

| | Responde la pregunta | Implementado en |
|---|---|---|
| Reporte de calidad nativo | ¿Qué fechas faltan y por qué (fin de semana vs. hueco real)? | `transform.py::build_quality_report` |
| Great Expectations | ¿Los valores que SÍ llegaron cumplen las reglas de negocio (nulos, rangos, categorías válidas)? | `quality_ge.py::validate_enriched_rates` |

Se usa la API clásica `SparkDFDataset` (paquete `great_expectations==0.15.x`)
en vez del Data Context/Fluent API de versiones más nuevas — validar un
DataFrame puntual dentro de un pipeline batch no justifica el andamiaje
completo de datasources/checkpoints/stores en YAML que trae la API nueva.
Mismo criterio de simplicidad que ya se aplicó para no usar Airflow.

Es un *gate* informativo, no bloqueante: si una expectativa falla, se
loggea con detalle y se persiste en `/app/reports/ge_validation_result.json`,
pero el pipeline continúa — igual filosofía que el reporte de calidad
nativo.

## Decisiones de diseño y trade-offs

### Extracción
- **Un solo request de rango** (`/v1/{start}..{end}`) en vez de iterar
  día por día: menos llamadas, menor riesgo de rate limiting. Trade-off:
  si el rango es muy largo, la respuesta es más pesada de parsear en
  memoria antes de convertirla a DataFrame — aceptable para el volumen de
  este ejercicio (meses, no décadas).
- **Reintentos con backoff exponencial** (`tenacity`) solo en errores
  transitorios (timeout, conexión, 5xx, 429). Un 4xx no se reintenta
  porque indica un error de la solicitud, no algo temporal.
- **Fines de semana/festivos**: la API simplemente no devuelve esos días.
  No se tratan como error; se documentan como tal en el reporte de calidad
  (`db.reporte_calidad`, columna `reason = 'weekend_or_holiday'`), distinto
  de un hueco real de datos (`'missing_data'`).
- **Anclaje fuera de rango**: se observó que si `start_date` cae en un día
  sin tasa publicada (ej. `2024-01-01`, feriado), la API puede devolver
  también el último día hábil *anterior* al rango pedido (`2023-12-29`).
  Se filtra explícitamente en `extract.py::_filter_to_configured_range`
  en vez de asumir que la API respeta el rango exacto — con logging de
  advertencia si algo se descarta.

### Transformación
- **Ventanas móviles por número de observaciones, no por días calendario**:
  como la fuente no trae fines de semana, una ventana de "7 días calendario"
  dejaría huecos. Se usa `rowsBetween(-6, 0)` sobre los datos ordenados por
  fecha por moneda — equivalente a "últimas 7 observaciones hábiles".
- **Rangos de sanidad para las tasas** (`0.0001` a `10000`): filtra errores
  evidentes de la fuente sin intentar "arreglar" el dato (no se imputan
  valores en un contexto financiero — mejor descartar y reportar que
  inventar).
- **Anomalías sobre la variación diaria (%), no sobre el nivel de la tasa**:
  un z-score sobre `daily_change_pct` respecto a su media/desviación móvil
  de 30 observaciones detecta "movimientos inusuales", que es la señal de
  riesgo relevante — una tasa alta pero estable no es una anomalía.

### CDC
- **Llave de negocio**: `(date, currency)` — el grano natural de la fuente.
- **Estrategia: hash de fila** (SHA-256) sobre las columnas de negocio
  (`rate`), no comparación campo a campo. Ventaja: si se agregan columnas
  de negocio a futuro, la lógica de comparación no necesita cambiar.
  Trade-off: se pierde el detalle de "qué campo cambió exactamente"; para
  este dominio (una sola columna de negocio) esa pérdida es irrelevante.
- **DELETE lógico (soft delete)**: si una llave que existía en Iceberg ya
  no aparece en el snapshot nuevo, se marca `operation_type = 'DELETE'`
  pero no se borra físicamente — permite auditar vía time travel.
- **Idempotencia**: si se corre el pipeline dos veces con el mismo dato de
  entrada, el hash no cambia → `operation_type = 'NONE'` → el `MERGE INTO`
  no toca esas filas y no se generan eventos. Validado en
  `tests/test_cdc.py::test_idempotent_double_run_produces_no_changes`.

### Modelado de datos
Modelo dimensional simple (esquema estrella), grano explícito:

| Tabla | Grano | Descripción |
|---|---|---|
| `fact_exchange_rates` (`db.tipos_cambio_enriquecidos`) | 1 fila = 1 tipo de cambio observado para 1 moneda en 1 fecha (base USD) | Hechos: tasa, variación diaria, medias móviles, volatilidad + columnas de auditoría CDC |
| `dim_currency` (`db.dim_currency`) | 1 fila = 1 moneda | Dimensión de crecimiento lento (SCD tipo 1 — no se justifica versionar el "nombre" de una moneda) |

No se modeló el dominio opcional de pagos/tarjetas (`fact_transactions`,
`dim_customer`, `dim_card`) para mantener el alcance enfocado en hacer bien
el CDC y la persistencia transaccional en Iceberg, que son los criterios
con mayor peso en la evaluación (40% combinado).

### Carga — Apache Iceberg
- **Catálogo `hadoop`** (filesystem local, sin infraestructura externa),
  cumpliendo el requisito explícito del enunciado.
- **Particionado por `(year, month)`**: las consultas típicas de este
  dominio (reportes mensuales, series de tiempo) filtran por fecha. No se
  particiona por `currency` porque el volumen por moneda es bajo y
  generaría demasiadas particiones pequeñas (*small files problem*).
- **`MERGE INTO`** aplica el resultado del CDC de forma atómica —
  demuestra Iceberg como tabla transaccional, no solo almacenamiento.
- **Time travel**: `src/load.py::read_table_as_of_snapshot` y
  `list_snapshots` permiten consultar/listar snapshots anteriores.
  Demostrado en vivo con `docker compose --profile demo up iceberg-demo`
  (compara conteo de filas entre el snapshot más antiguo y el actual).
- **Schema evolution**: `src/load.py::add_column_if_not_exists` agrega una
  columna sin reescribir la tabla completa. Demostrado en el mismo
  `iceberg-demo` (agrega `source_system` y confirma que el conteo de filas
  no cambia — solo se tocó metadata).
- Las tablas derivadas (`metricas_mensuales`, `anomalias`, `reporte_calidad`)
  se recalculan por *overwrite* completo en cada corrida en vez de llevar
  su propia lógica de MERGE: son 100% derivables de la tabla de hechos ya
  incremental, así que un MERGE ahí sería complejidad sin beneficio real.

### Eventos — Kafka real
- **Broker real** (`apache/kafka:3.7.0`, modo KRaft, sin Zookeeper) definido
  como servicio en `docker-compose.yml`. `docker compose up` lo levanta
  automáticamente antes que el pipeline (`depends_on: condition:
  service_healthy`).
- El pipeline actúa como **productor real** (`src/events.py::publish_events_to_kafka`,
  librería `kafka-python`): cada cambio detectado por el CDC se publica en
  el topic `exchange-rate-events`, con `key = entity_id` (garantiza orden
  por moneda+fecha dentro de una partición) y `acks='all'` (espera
  confirmación del broker antes de dar el evento por enviado).
- Además de Kafka, se sigue escribiendo una copia en `/events/*.json`
  (auditoría local, independiente de si el broker está disponible).
- **Consumidor de demostración** (`src/consumer_demo.py`): no corre por
  defecto — se activa manualmente para poder mostrar en vivo que el topic
  tiene mensajes reales:
  ```bash
  docker compose --profile demo up kafka-consumer-demo
  ```
  Se detiene solo tras 15s sin mensajes nuevos.
- **Consistencia con CDC garantizada por construcción**: los eventos se
  generan a partir del *mismo* DataFrame que produjo el CDC (`changes_df`),
  nunca se recalculan por separado.

## Supuestos explícitos

1. Periodo de extracción: `2024-01-01` a `2024-06-30` (sugerido por el
   enunciado), configurable en `config/settings.yaml`.
2. Monedas: `MXN`, `EUR` (obligatorias) + `BRL`, `CAD` (adicionales,
   elegidas por ser economías latinoamericanas comparables, relevantes
   para el contexto de negocio de una fintech de PyMEs en LatAm).
3. Como no se dispone de dos snapshots reales de la API en distintos
   momentos, la demostración de CDC ocurre de forma natural al correr el
   pipeline dos veces: la primera carga es 100% `INSERT`; una segunda
   corrida sin cambios en la fuente produce 100% `NONE`. Para forzar un
   `UPDATE`/`DELETE` de forma controlada, se puede editar manualmente una
   fila en la tabla Iceberg entre corridas, o correr `pytest
   tests/test_cdc.py` que sí simula explícitamente dos snapshots.
4. "Rango razonable" de una tasa de cambio se definió como `[0.0001,
   10000]` — suficientemente amplio para no descartar monedas legítimas,
   suficientemente estrecho para atrapar errores evidentes de parseo.
5. Se asume una sola ejecución del pipeline por vez (no hay lógica de
   locking para corridas concurrentes) — razonable para un batch job
   diario/manual como el que pide el enunciado.

## Estructura del repositorio

```
README.md
Dockerfile
docker-compose.yml
requirements.txt
requirements-dev.txt         # solo para el notebook (jupyter, matplotlib, pandas)
.github/workflows/
  ci.yml                       # CI: tests + build de Docker en cada push/PR
config/
  settings.yaml        # toda la configuración parametrizable
data/
  solicitudes_credito.csv  # segunda fuente de datos (CSV simulado)
  customers.csv               # dominio de pagos: clientes
  cards.csv                     # dominio de pagos: tarjetas
  transactions.csv                # dominio de pagos: transacciones
src/
  common.py              # SparkSession + config + logging compartidos
  dag.py                   # motor de DAG (orden topológico, detección de ciclos)
  extract.py                 # Frankfurter API con retry/backoff
  transform.py                 # limpieza, enriquecimiento, agregaciones, anomalías, calidad
  cdc.py                         # detección de cambios (hash de fila)
  secondary_source.py              # segunda fuente + join enriquecido
  payments_domain.py                 # dominio opcional pagos/tarjetas + integración FX
  quality_ge.py                        # validación con Great Expectations
  model.py                               # fact/dim (tipos de cambio)
  load.py                                  # persistencia Iceberg (MERGE, particiones, time travel, compaction)
  events.py                                  # generación de eventos + publicación a Kafka
  consumer_demo.py                             # consumidor de demo del topic Kafka
  iceberg_demo.py                                # demo de time travel, schema evolution, compaction
  main.py                                          # orquestador (construye y corre el DAG)
tests/
  test_transform.py
  test_cdc.py
  test_secondary_source.py
  test_payments_domain.py
  test_extract.py
  conftest.py                             # fixture de SparkSession para tests
notebooks/
  eda.ipynb                                 # análisis exploratorio sobre las tablas Iceberg
warehouse/                                  # generado por el pipeline (catálogo Iceberg)
events/                                       # generado por el pipeline (eventos JSON)
```

## Mapeo a criterios de evaluación

| Criterio | Dónde se cubre |
|---|---|
| Funcionalidad end-to-end | `docker compose up` corre todo sin intervención manual (`src/main.py`, DAG explícito) |
| CDC | `src/cdc.py` + `tests/test_cdc.py` (idempotencia, INSERT/UPDATE/DELETE) |
| Calidad de código | Módulos separados por responsabilidad, funciones puras y testeables, logging consistente, CI valida sintaxis en cada push |
| Modelado de datos | `src/model.py` + `src/secondary_source.py`, grano documentado arriba |
| Uso de Iceberg | `src/load.py`: MERGE INTO, particiones, time travel, schema evolution, **compaction** (4/4, el enunciado pide 2) |
| Testing | `tests/test_transform.py`, `tests/test_cdc.py`, `tests/test_secondary_source.py` — corridos automáticamente en CI |
| Kafka | broker real (`docker-compose.yml`), productor y consumidor reales (`src/events.py`, `src/consumer_demo.py`) |
| Extras | DAG explícito, segunda fuente + join, compaction, CI/CD, notebook de EDA, dominio de pagos/tarjetas, Great Expectations, retry/backoff, config externalizada |

## Limitaciones conocidas / próximos pasos

- Great Expectations y el dominio de pagos/tarjetas ya están implementados
  (ver secciones 6 y 7 arriba) — la limitación restante es de alcance
  menor: la migración a un orquestador real tipo Airflow no se hizo por
  no justificarse para un batch de una sola corrida diaria (ver
  discusión de trade-offs en la sección de DAG arriba).
- `great_expectations==0.15.50` es una dependencia con bastantes paquetes
  transitivos; si el build de Docker falla por conflicto de versiones,
  la validación de GE se puede aislar en su propio ambiente/imagen sin
  afectar el resto del pipeline (es un paso más del DAG, no una
  dependencia estructural de las demás capas).
