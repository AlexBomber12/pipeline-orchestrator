# Day 4: External failures и data integrity (автоматизировано)

## Что нового

Это **первый auto-день** testing-week. Pytest + Playwright вместо manual
copy-paste curl + скриншоты.

Что делает скрипт:
1. HTTP операции через `requests` (вместо твоего curl)
2. Браузер-действия через Playwright chromium (вместо твоих скринов)
3. Assertions на API + UI + Redis state
4. Evidence screenshots авто-сохраняются
5. HTML report с PASS/FAIL + embedded screenshots + tracebacks

Твоя роль:
1. Одна команда запуска
2. Смотрит HTML report
3. Обсуждаем findings (не PASS, только FAIL или surprising)

## Установка

**Шаг 1. Apply pack поверх существующего tests-manual/:**

    cd /home/alexey/pipeline-orchestrator
    unzip -o ~/Downloads/tests-manual-day4-pack.zip
    chmod +x tests-manual/auto/day4/*.sh

**Шаг 2. Установить pytest + Playwright (первый раз):**

    pip install pytest==8.3.0 pytest-html==4.1.1 pytest-playwright==0.5.0 requests redis

Или если есть venv:

    source /home/alexey/pipeline-orchestrator/.venv/bin/activate
    pip install pytest==8.3.0 pytest-html==4.1.1 pytest-playwright==0.5.0 requests redis

**Шаг 3. Установить Chromium browser для Playwright (первый раз):**

    playwright install chromium
    playwright install-deps chromium

Это скачает ~300MB. Один раз.

**Шаг 4. Smoke check:**

    cd /home/alexey/pipeline-orchestrator
    pytest tests-manual/auto/day4/ --collect-only

Должно показать список 8 тестов без ошибок.

## Запуск Day 4

**Один command:**

    cd /home/alexey/pipeline-orchestrator
    ./tests-manual/auto/day4/run.sh

Что происходит:
1. Скрипт проверяет что docker compose up (иначе завершается с инструкцией)
2. Запускает pytest headless chromium (видимого браузера не будет)
3. Прогон 8 тестов ~ 15-25 минут (зависит от claude CLI скорости на CODING scenarios)
4. На выходе создаёт `/tmp/day4-report.html` + evidence JPEG в `/tmp/day4-evidence/`
5. Автоматически открывает report в браузере (если $BROWSER set) или печатает путь

**Run с видимым браузером (для debug):**

    ./tests-manual/auto/day4/run.sh --headed

Увидишь реальный chromium window с Playwright actions.

**Run только одного теста:**

    pytest tests-manual/auto/day4/tests/test_ext_01_claude_cli_unavailable.py -v

## Что тестируется

8 scenarios, каждый в отдельном файле `tests/`:

1. **EXT-01** Claude CLI unavailable via shim → livelock prevention verified (PR-190a)
2. **EXT-02** Codex CLI unavailable → FIX cycle degradation
3. **EXT-03** GitHub unreachable via /etc/hosts → watch/merge degradation
4. **EXT-04** Redis disconnect mid-operation → daemon recovery
5. **EXT-05** Anthropic usage API broken → graceful fallback (verifies USAGE-API-01)
6. **DATA-01** SIGKILL daemon mid-QUEUE regen → data integrity at restart
7. **DATA-02** Upload during Redis disconnect → manifest persistence
8. **EVENT-RECHECK-01** Deliberate Stop sequence → verify EVENT-LOG-01 status

**Почему порядок важный.** EXT-01 первый потому что верифицирует PR-190a
works. Если этот тест FAIL — дальнейшие external failure тесты могут
трггернуть livelock. В этом случае suite останавливается с clear message.

## Screenshots policy

- **PASS** тест → 1 anchor screenshot (viewport, JPEG quality 60, ~15-25kb)
- **FAIL** тест → 2 screenshots (before state + after failure moment)
- **Visual regression** → не применяется в Day 4, добавим в Day 6

Общий объём evidence на Day 4: ~16 screenshots total ~300-400kb.

## Report structure

`/tmp/day4-report.html` открывается в любом браузере.

Секции:
1. **Summary** — всего N тестов, PASSED/FAILED/SKIPPED counts
2. **Environment** — версии pytest/playwright, OS, Python version
3. **Per-test** rows expandable:
   - PASS — зелёная строчка + anchor screenshot
   - FAIL — красная строчка + traceback + before/after screenshots + captured stdout/stderr
   - SKIPPED — yellow + reason

Если всё PASS — 5 минут листать. Если есть FAIL — 15-20 минут интерпретировать с моей помощью.

## Troubleshooting

**Error "playwright not found":**

    pip install playwright pytest-playwright
    playwright install chromium

**Error "docker compose daemon not running":**

    cd /home/alexey/pipeline-orchestrator
    docker compose up -d
    # Wait 30 seconds for Redis + daemon to be ready

**Test hangs mid-run:**

Ctrl+C once. pytest catches SIGINT and generates partial report for
completed tests. Скажи мне какой тест hanged и я разберусь.

**Redis state не возвращается между тестами:**

Suite имеет fixture `reset_daemon_state` который очищает Redis state
для testbed в начале каждого теста. Если видишь state leak между тестами
— отчёт conftest.py bug, пиши.

## Наблюдения в конце дня

В конце run открой `/tmp/day4-report.html` → собери findings в
`/tmp/observations-day4.md` (manual observation file, формат как Day 1-3).

Шаблон один, как в Day 3. Загружай в strategy chat для consolidate.

## Обратно в strategy chat

После Day 4:

1. Напиши "Day 4 finished" с attached `observations-day4.md`.
2. Я делаю consolidate + roadmap update.
3. Генерю Day 5 pack.
