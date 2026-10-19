# Исполнимый план по результатам аудита 19.10.2026

Репозиторий: AlexBomber12/pipeline-orchestrator, main. Исходный SHA: `7167c2e916a2bbb843ecb6a6ce7fc15795e61802`. Основание: повторный аудит v14 по 10 критериям.

## Что меняется

64 старые незавершенные спецификации PR-393–467 заменяются 38 новыми PR-468–505. 10 подтвержденно завершенных и активная PR-459 сохраняются побайтно, вместе с исходным completions.json и всей историей до PR-393. Новые номера не переиспользуют прежние task/branch identities. Удаленные спецификации доступны по исходному commit/blob и включены в резервный комплект; они не помечаются ложным DONE.

Среди 38 задач: 6 ограниченных investigations, 2 обновления документации и 30 задач реализации/проверки конкретного результата. Investigation заканчивается доказанным supported/unsupported выводом и ограниченным follow-up, а не разрешением на любую архитектуру. Неизвестные implementation scopes C01–C08 не выдаются за готовые runnable tasks.

Это подготовка плана. Runtime policy, pause, конфигурация, реальные secrets, production и branch protection не меняются. Текущая PR-459 продолжает существующий #620. Ее 2 P2 исправляются в том же PR; новая очередь не требует от всех остальных изменений ожидать репортера PR-459 или общего positive-outcome теста.

## Как применять

Применение только одним reviewed Git изменением с удалением старых specs и добавлением новых. Обычная загрузка только PR-468–505 через dashboard добавила бы второй конкурирующий план и не является способом этой миграции. tasks/QUEUE.md не редактируется вручную. Сначала проверяются pause/current ownership и receipts, затем candidate diff и CI/review; source merge и deployment являются разными действиями.

Перед merge/release повторно сверить: main и task bytes не изменились неожиданно, paused=true, активная task все еще PR-459/#620 либо ее доказанный терминальный исход, нет другого активного или pending Retry на удаляемые ID. Отсутствие PID/свежий snapshot не доказывают quiescence. Не выполнять destructive managed-checkout sync при неизвестном writer. Если #620 успел завершиться, сохранить новый completion/status и его evidence, не восстанавливать snapshot из этого комплекта.

После применения на daemon-managed task corpus проверить одну актуальную очередь без 64 старых runnable specs, все сохраненные positive completions и выбранные новые dependencies. Не очищать Redis/счетчики/старые причины ради нового плана и не снимать паузу автоматически. Canceled/rejected/failure history сохраняется для трассировки.

## Первые полезные результаты

1. PR-468 устраняет подтвержденную потерю tracked changes при статусной записи.
2. PR-469–471 дают известный PR/remote outcome без повторного создания; PR-472–474 доводят consumers CI/review до финального merge.
3. PR-495 доказывает настоящие merged result/base marker/DONE. Он не является общим prerequisite других исправлений.
4. PR-477/478 проверяют cleanup и минимальную сохранность. PR-497/498 включают продолжение только при положительных гарантиях, PR-499 доказывает A failed / C blocked / B merged. Полный dirty/restart маршрут остается C02 до проверки и отдельной реализации.
5. Независимо идут Retry/global-control/Stop, prospective DAG, metrics, uniqueness/presence и проверка реального применения Settings. Ни один из этих результатов не зависит от диагностического репортера.

Приоритеты 1/2 относятся к полезности и риску. Depends on содержит только конкретный недоставленный interface/acceptance fixture, который действительно требуется. Названия A/R/U/IMP в traceability не являются скрытыми dependencies. Cxx не являются task IDs и не включаются в DAG.

## Новые задачи и реальные зависимости

| Task | Приоритет | Результат | Depends on | Оценка prod/tests/other |
|---|---|---|---|---|
| PR-468 | 1 | Write task status without changing the coder checkout | none | 180/260/0 |
| PR-469 | 1 | Distinguish an exact PR outcome from incomplete discovery | none | 160/250/0 |
| PR-470 | 1 | Confirm the remote merge before recording task success | PR-468, PR-469 | 170/260/0 |
| PR-471 | 1 | Retain a published PR when the coder invocation fails | PR-469 | 190/270/0 |
| PR-472 | 1 | Keep required CI policy in dispatch-side PR hydration | none | 130/230/0 |
| PR-473 | 1 | Keep required CI policy through WATCH and recovery | none | 150/240/0 |
| PR-474 | 1 | Use complete current-HEAD evidence at the final merge decision | PR-470, PR-472, PR-473 | 180/270/0 |
| PR-475 | 1 | Prove an automatic review replacement with recorded provider evidence | none | 0/300/140 |
| PR-476 | 2 | Align managed review instructions with delivered actor and HEAD rules | none | 80/130/170 |
| PR-477 | 1 | Map the existing cleanup proof across ordinary exit and restart | none | 0/300/130 |
| PR-478 | 1 | Select the smallest preservation approach with a real restore proof | PR-468 | 0/330/150 |
| PR-479 | 1 | Skip temporarily unavailable pinned tasks without failing the repository | none | 180/260/0 |
| PR-480 | 1 | Recognize a completed Retry request before checking eligibility for a new one | none | 90/260/0 |
| PR-481 | 1 | Apply Retry only to its bound failure and required resource scope | PR-480 | 180/240/0 |
| PR-482 | 1 | Report global control results only after confirmed state writes | none | 180/250/0 |
| PR-483 | 1 | Retain Stop until the daemon acknowledges it or Resume explicitly cancels it | PR-482 | 150/250/0 |
| PR-484 | 1 | Validate the candidate task graph before accepting a batch | none | 180/260/0 |
| PR-485 | 2 | Preserve retry history when an upload does not change the specification | none | 130/230/0 |
| PR-486 | 2 | Discover persisted pending uploads after a missed wake without bypassing pause | PR-483, PR-484, PR-485 | 160/260/0 |
| PR-487 | 1 | Read task metrics through the existing MetricsStore contract | none | 100/210/0 |
| PR-488 | 2 | Prove Settings model and effort reach the next invocation | none | 0/300/60 |
| PR-489 | 2 | Show a persistent result and the automatic next step | PR-487 | 150/190/100 |
| PR-490 | 1 | Count each terminal error once across replay and restart | none | 140/230/0 |
| PR-491 | 1 | Keep background dashboard refresh from asserting operator presence | none | 130/250/40 |
| PR-492 | 2 | Bind an existing Approve decision to the current attempt and violation | PR-480 | 190/260/0 |
| PR-493 | 2 | Keep CI infrastructure outages out of code-FIX attempts | PR-473 | 150/240/0 |
| PR-494 | 2 | Prove the minimum durable final-Reject lifecycle before extending it | PR-469, PR-477 | 0/320/140 |
| PR-495 | 1 | Prove upload produces the matching merged change and completed task | PR-470 | 0/240/0 |
| PR-496 | 2 | Prove actionable review feedback reaches a new reviewed HEAD and merge | PR-474, PR-495 | 0/310/0 |
| PR-497 | 1 | Define when a terminal task may release independent work | PR-477 | 130/220/0 |
| PR-498 | 1 | Release independent work after a safely finalized terminal failure | PR-468, PR-470, PR-471, PR-497 | 200/280/0 |
| PR-499 | 1 | Prove a failed task does not prevent an independent merged result | PR-495, PR-498 | 0/320/50 |
| PR-500 | 2 | Verify a restart during FIX retains the same PR and work | PR-471, PR-474, PR-477 | 0/320/0 |
| PR-501 | 2 | Inventory authoritative data and prove a disposable restore | PR-477, PR-478, PR-483 | 0/280/170 |
| PR-502 | 2 | Verify compatible form-parsing protection for current dependencies | none | 0/260/170 |
| PR-503 | 2 | Expose complete attempt totals without treating missing telemetry as free work | PR-487 | 180/250/0 |
| PR-504 | 2 | Document one evidence-based release and rollback procedure | PR-501 | 0/0/200 |
| PR-505 | 2 | Verify Stop and Resume receipts remain truthful through browser refresh | PR-482, PR-483 | 0/320/0 |

## Условные продолжения, которые не потеряны при сокращении

Ниже находятся требования, а не исполняемые файлы PR-*.md и не обещание уже исправленной системы. Их допуск требует указанного доказательства, известного подхода и честной оценки по TASK_SCHEMA. Зависимые от неизвестного интерфейса tasks заранее не создаются. Независимая работа продолжает идти.

| ID | Необходимый результат | Когда уточнить implementation tasks | Исходная трассировка | Сохраненный scope |
|---|---|---|---|---|
| C01 | Автоматическое восстановление review без технического выбора оператора | PR-475 | PR-403–407; IMP-03 | Реальные request/full-SHA/current-invocation +1 linkage, bounded repost/reconcile, external push/restart. После доказательства переводятся все consumers и удаляется обычная migration panel. Новые implementation tasks оценить по результату, не писать вымышленный API. |
| C02 | Полная сохранность и автоматическое продолжение при dirty work/неизвестном restart owner | PR-477, PR-478 | PR-416/419/421/422/425–431/442/454; IMP-01/04 | Выбранные capture/index/local-history/restore и retained ownership, общий scoped допуск, миграция status/sync/error/FIX/merge/web/constructor. PR-468 решает известный status-only loss, PR-498 только подтвержденный clean/quiescent release. Не объявлять их полной реализацией C02. |
| C03 | Durable final Reject и единый changed-spec admission | PR-494 | PR-432–446/455/456; IMP-05/06 | Exact intent/attempt/failure/spec, close confirmation и late push, retained fence после Redis loss, consistent upload/direct Git admission/reservation, безопасная branch preparation. Новый PR после final Reject, тот же PR при обычном Retry. Использовать минимум existing stores; количество implementation tasks определить после boundary proof. |
| C04 | Недостающие persistence/backup/readiness гарантии | PR-501, PR-504 | A16/A38; IMP-10 | Конкретные выявленные потери данных/intent, off-host доступность, startup identity, health/readiness и state-compatible rollback. Не считать bundle или Up достаточной гарантией. Production drill не входит в AUTO PR. |
| C05 | Совместимое исправление применимой form-parsing уязвимости | PR-502 | A24; IMP-11 | Доставить проверенный compatible dependency upgrade или bounded body-limit mitigation с regression actual request.form path. До результата не выбирать вслепую независимый Starlette pin. |
| C06 | Остальные guardrail и входные границы | Проверка конкретного usage/perimeter; при изменении Approve использовать PR-492 | A20–23/A25/A27; R17–19/R24; IMP-05/11 | Полный набор нарушений и bounded label text, sensitive excerpts, Origin/JS-context/webhook destination, sandbox mode и build-context regression. Dockerignore/CLI pins уже существуют. Не вводить общую IAM/SSRF блокировку всех LAN webhook без учета фактического продукта. Новая task нужна для конкретно подтвержденного остатка, а не blanket security sprint. |
| C07 | Расширения account profiles, metered API budget, обучаемый selector, 20 repo | Реальная потребность в новом transport/account или росте нагрузки | A30/A31/A37; IMP-04/09 | Отложены. До metered API отдельно задать валюту/период/scope/soft-hard budget и reservations/settlement. Существующие quota/caps не снимать. До роста измерить capacity/fairness, не писать новый scheduler заранее. |
| C08 | Дополнительная browser/accessibility/auth/settings приемка и отдельные onboarding остатки | Конкретное изменение соответствующего UI/контракта | PR-464; A17/A19/A33/A34/A40/U01-F01–F12; IMP-07/08/09 | Login тесты только с synthetic responses; собственный regression включать в меняющийся control. Если PR-488 докажет конкретный effective-config/ACK defect, выделить ограниченный runtime delta, а не считать весь Settings сломанным. Доступность и audit metadata не объявлять закрытыми по task metrics fix. |

## Решение по всем 75 старым спецификациям

Строка с investigation заменяет прежнее преждевременно заданное проектирование, но сама по себе не закрывает исходный функциональный результат. Связанные Cxx явно сохраняют эту незавершенную обязанность. Никакие старые TODO не становятся DONE по переименованию.

| Старая task | Действие | Новый scope | Условный остаток | Исходный Git blob |
|---|---|---|---|---|
| PR-393 | Сохранить завершение/bytes | Отложено | Нет | fa6746654dbc5ce699f08e0ce468f0d345a0f9b6 |
| PR-394 | Сохранить завершение/bytes | Отложено | Нет | 5ebc05ef41f468b8ab5508a53ad94f1153618188 |
| PR-395 | Сохранить завершение/bytes | Отложено | Нет | 5e6edab57989a55e10d9c38a0bf4fae69c240cb4 |
| PR-396 | Сохранить завершение/bytes | Отложено | Нет | eb67160cd5a67cc7a48993a0af5d59e20f56a750 |
| PR-397 | Сохранить завершение/bytes | Отложено | Нет | 4bcb7e73a60d66b3f8bd97f3dd200cb11c70f72a |
| PR-398 | Убрать из runnable; заменить scope | PR-472 | Нет | 2e0ebc8ebd7f12bc1b1ae5deea322553049ef66c |
| PR-399 | Убрать из runnable; заменить scope | PR-473 | Нет | 13d6b6229903e05a190debd8a74827bbb1d3aba0 |
| PR-400 | Сохранить завершение/bytes | Отложено | Нет | 60a4b9e419d3f0fe894846d3c9415fd74ab3ca99 |
| PR-401 | Убрать из runnable; заменить scope | PR-474, PR-475 | Нет | c1afb20d6eb8492e2b04758e7ce91405ffa222b6 |
| PR-402 | Убрать из runnable; заменить scope | PR-474, PR-475 | Нет | 5d3f39efa8139fc456339981f83eb7d038403696 |
| PR-403 | Убрать из runnable; заменить scope | PR-475 | C01 | af837c729042cc0bd66341a8e483b617c8e5d905 |
| PR-404 | Убрать из runnable; заменить scope | PR-475 | C01 | 5346b710b60d881ee70f19a224aaba177371afe1 |
| PR-405 | Убрать из runnable; заменить scope | PR-475 | C01 | 403928d4df745c3cf2e4083f23b137acc54d308c |
| PR-406 | Убрать из runnable; заменить scope | PR-475 | C01 | 9fcef626311bde10fbba8b322cd8d3fc7762f7bd |
| PR-407 | Убрать из runnable; заменить scope | PR-475 | C01 | 6ce40bbe1a26a54759b9cc1210ce031b8af36c5f |
| PR-408 | Убрать из runnable; заменить scope | PR-476 | Нет | dc6da4073ad5a567de68f9c37bc7e7c80024ba7d |
| PR-409 | Сохранить завершение/bytes | Отложено | Нет | 67ad02dda584f084d3aa9b6decf0aa5fd25cf7f9 |
| PR-410 | Убрать из runnable; заменить scope | PR-474 | Нет | d5cb3f4ac638906a85accd2d0034b7cd53f50ce5 |
| PR-411 | Убрать из runnable; заменить scope | PR-474 | Нет | d11a4a512a6a890bebf13382b2782186a8015bea |
| PR-412 | Убрать из runnable; заменить scope | PR-469 | Нет | 807ce9380ad019c960e4c403861c4ca31f04a621 |
| PR-413 | Убрать из runnable; заменить scope | PR-470 | Нет | 212a4e4980ab8cd190d3016555a03488a0db781c |
| PR-414 | Убрать из runnable; заменить scope | PR-471 | Нет | 1b117c8347f514fb7b08af28bf7f36d44312a624 |
| PR-415 | Убрать из runnable; заменить scope | PR-471 | Нет | dc3907104df657ad95855b1b8e06c4386e3e8a88 |
| PR-416 | Убрать из runnable; заменить scope | PR-477 | C02 | 5138481a91fbb6dda2ff17236d5dbad1ff88dd80 |
| PR-417 | Убрать из runnable; заменить scope | PR-471 | Нет | 1afb18a6b87ffeb574d6a58f2f6479eb86057a6e |
| PR-418 | Сохранить завершение/bytes | Отложено | Нет | 6aff2e7e3e3bf915657762760fe3c8fe013823cb |
| PR-419 | Убрать из runnable; заменить scope | PR-477 | C02 | 22966b4a86c033b8d17ed6b4cdb3a25a4eed295a |
| PR-420 | Убрать из runnable; заменить scope | PR-477 | C02 | eb8d28e18b2ebbca8c568a93b2cb301a38513f6d |
| PR-421 | Убрать из runnable; заменить scope | PR-477 | C02 | 462dd8a749ca3e5ed522eac222ce03b9c341b5c1 |
| PR-422 | Убрать из runnable; заменить scope | PR-477 | C02 | 5101f17e56f76a10b043e19118e2bf47903873e2 |
| PR-423 | Убрать из runnable; заменить scope | PR-471 | Нет | 03e1b8b8e4aba7f95853d9d5d213679cbfe26cd6 |
| PR-424 | Убрать из runnable; заменить scope | PR-471 | Нет | 650942ba854d07fa5bf70255beb9cafcc0b585cc |
| PR-425 | Убрать из runnable; заменить scope | PR-478 | C02 | 350f8cf2e46d54cdd45c19c88ae1a0e698b6a2fc |
| PR-426 | Убрать из runnable; заменить scope | PR-478 | C02 | 4de0e86c9e4a8c806045a7a10178cbff1805361a |
| PR-427 | Убрать из runnable; заменить scope | PR-478 | C02 | 39142db48a4de9bf783aa4824278cbee8ae8ef97 |
| PR-428 | Убрать из runnable; заменить scope | PR-478 | C02 | 515c824b442e5ae499897c594d7d98c7cc9fa4a5 |
| PR-429 | Убрать из runnable; заменить scope | PR-468, PR-478 | C02 | a5f0d246e0361e3c29c17a135e553332d61250ec |
| PR-430 | Убрать из runnable; заменить scope | PR-478 | C02 | 4a7b4c6a7fedece1d83489c9960e14c4fca9a79b |
| PR-431 | Убрать из runnable; заменить scope | PR-478 | C02 | e7f6e154a3e224b4072e4cbd438ce2ad5ae64a45 |
| PR-432 | Убрать из runnable; заменить scope | PR-494 | C03 | 3a6c7b0030437c62fc42829059bba8ac73933bd9 |
| PR-433 | Убрать из runnable; заменить scope | PR-494 | C03 | 74cfb67370819122f8b33b670b96bc2048348151 |
| PR-434 | Убрать из runnable; заменить scope | PR-494 | C03 | 9a12c2d093eea532a81fbbcc56337a87ab260646 |
| PR-435 | Убрать из runnable; заменить scope | PR-494 | C03 | 0013f7f01961284cd8a177c10e3f02eb9eb69b2c |
| PR-436 | Убрать из runnable; заменить scope | PR-494 | C03 | b7bce3254a91c0ab45e00e6c36dcb1a8dae1b0d7 |
| PR-437 | Убрать из runnable; заменить scope | PR-494 | C03 | caa4361b7eeb3761164f9e4654ec7c6fd821455f |
| PR-438 | Убрать из runnable; заменить scope | PR-494 | C03 | b17291d6f40e2bf1cb4ae35bf6ce237e9c107635 |
| PR-439 | Убрать из runnable; заменить scope | PR-494 | C03 | 0c1464bf37c5410e200833bbc3d2ca57f4c0544e |
| PR-440 | Убрать из runnable; заменить scope | PR-494 | C03 | e4225417869aafd210c452cc6bb0fd403d43a004 |
| PR-441 | Убрать из runnable; заменить scope | PR-494 | C03 | 81f42e8156d04cc1c3e99647850aed8e82685581 |
| PR-442 | Убрать из runnable; заменить scope | PR-478, PR-494 | C02, C03 | 17258fa8e0de831a7ed223142c596c2743b3daa2 |
| PR-443 | Убрать из runnable; заменить scope | PR-494 | C03 | 32a24555fffca800465d80a56734b49800fed1b7 |
| PR-444 | Убрать из runnable; заменить scope | PR-485, PR-494 | C03 | ed8861c20589796c4d639135fe8f04811476febe |
| PR-445 | Убрать из runnable; заменить scope | PR-485, PR-494 | C03 | afe99edf9a04cfcb1d0f7a4d1dc8e05a92be76c0 |
| PR-446 | Убрать из runnable; заменить scope | PR-494 | C03 | 80c8937e540c0497dde35bbf4eddad430d30dfe1 |
| PR-447 | Убрать из runnable; заменить scope | PR-484 | Нет | e24cc84085986c0ab801b930c5f7dcbee31719da |
| PR-448 | Убрать из runnable; заменить scope | PR-485 | Нет | 31036e9579c88468fd3793fbcdeb06b691db1651 |
| PR-449 | Убрать из runnable; заменить scope | PR-486 | Нет | 163e8f15463b7154847ec06cea82b920ae559e99 |
| PR-450 | Убрать из runnable; заменить scope | PR-486 | Нет | caa76181a777a244c001f5f310ee7dbb1cfe0972 |
| PR-451 | Убрать из runnable; заменить scope | PR-474 | Нет | a1e638ce6e6641f747b2cfe7e09a3af6d1bf9a25 |
| PR-452 | Убрать из runnable; заменить scope | PR-470 | Нет | 47c828420ad79fef917c1cf5deb3b22af9e95b9e |
| PR-453 | Убрать из runnable; заменить scope | PR-471 | Нет | ba0407164c11e5ebdd8e623054bd7cd7aa84f715 |
| PR-454 | Убрать из runnable; заменить scope | PR-477 | C02 | 4340fdc94902f944fe52c331cca50a1ea1a89dc7 |
| PR-455 | Убрать из runnable; заменить scope | PR-494 | C03 | aadc9dc0c4ab8ad11029db7be2c18e064fc065df |
| PR-456 | Убрать из runnable; заменить scope | PR-494 | C03 | 68345c8e032267ffced5d8883df817201d4baf09 |
| PR-457 | Убрать из runnable; заменить scope | PR-476 | Нет | 5fc46afc6b48f52b42f7f4164d0c3102b9ee27fd |
| PR-458 | Сохранить завершение/bytes | Отложено | Нет | 6d2eb9e531bd25c768124f9a3dd62f1ad738dffc |
| PR-459 | Сохранить PR-459/#620/bytes | Отложено | Нет | 659601174fefa600b44eb94788632a8ffc2c6807 |
| PR-460 | Убрать из runnable; заменить scope | PR-495 | Нет | 0fa05172222e4f76589941a82a8d9f8e43d847f7 |
| PR-461 | Убрать из runnable; заменить scope | PR-496 | Нет | 33b2abe85805143088a81a89d6158ad99159ea51 |
| PR-462 | Убрать из runnable; заменить scope | PR-488 | Нет | 8eea8bf34228b73a9f9aaa0879e8a64581a63073 |
| PR-463 | Убрать из runnable; заменить scope | PR-500 | Нет | b516a6dbcd632894048badb1081b79312d014931 |
| PR-464 | Отложить отдельную UI-приемку | Отложено | C08 | 82c72e6b54891ef8a3c5835bb79e89c7da148f00 |
| PR-465 | Убрать из runnable; заменить scope | PR-505 | Нет | 26a24dcb3f1a25e8d7b208b628298e3c6e681304 |
| PR-466 | Убрать из runnable; заменить scope | PR-486 | Нет | 7f6de6ade6c8b4f4fdc00759286b08e9c588fd44 |
| PR-467 | Сохранить завершение/bytes | Отложено | Нет | a4463a635882cf573dc74263e4412198b45a9c09 |

## Общая приемка и граница завершения

У каждой task собственные meaningful regressions. CI/review readers проверяются реальными production evaluators, Git/process/Redis/DOM гарантии соответствующими настоящими границами. Existing GHA testbed сериализован и сохраняется. IDLE, API return 200, свежий snapshot и слово DONE по отдельности не доказывают полезный результат.

Итоговая приемка цикла требует: matching merged=true/commit/base content/DONE; FIX→new HEAD→fresh review→merge; A failed/C blocked/B merged; scoped quota recovery; response loss без дублей; сохранность и ownership across restart; truthful Stop/Resume/Retry/Approve/Reject; atomic admission и persisted pause; applied Settings и честные все attempts/unknown costs.

Слияние 38 новых задач не объявляет автоматически закрытыми C01–C08 и все 214 сценариев. Для каждой исходной находки требуется ее собственное evidence. Неизвестная recovery provenance, dirty/restart ownership и final Reject остаются безопасно held до доказанного исправления; они не обходятся ради непрерывности.

Политика 5/60 сохранена: AVAILABLE attention pause до Resume, AWAY без rate-only autopause. User Pause/Stop, runtime caps, credential isolation, exact HEAD/CI/trusted +1 и реальные полномочия не ослаблены. Estimates не превращаются в execution caps. Все новые task bodies составлены на английском и самодостаточны для AUTO PR.
