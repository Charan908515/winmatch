import asyncio
import csv
import json
import os
import random
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 1  # Keep 1 to avoid IP Bans. Increase ONLY if you have proxies.
MAX_RETRIES = 3

ACCOUNTS_FILE = "accounts.csv"
RESULTS_FILE = "account_balances.csv"
PROGRESS_FILE = "progress.json"
SCREENSHOTS_DIR = "screenshots"
SELECTORS_FILE = "selectors.json"

# ================= GLOBALS =================
csv_lock = asyncio.Lock()
progress_lock = asyncio.Lock()
stats_lock = asyncio.Lock()

STATS = {
    "total": 0,
    "processed": 0,
    "success": 0,
    "failed": 0
}

# ================= PROGRESS & SAVING =================
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

async def save_result(row):
    async with csv_lock:
        file_exists = os.path.exists(RESULTS_FILE)
        with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["username", "password", "balance", "status", "error", "timestamp"]
            )
            if not file_exists: writer.writeheader()
            row["timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow(row)

async def log_progress():
    async with stats_lock:
        print(f"[PROGRESS] {STATS['processed']}/{STATS['total']} | "
              f"Success: {STATS['success']} | Failed: {STATS['failed']}", flush=True)

# ================= STEALTH & POPUPS =================
async def apply_stealth(page):
    """Manually hides bot signals."""
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        if (!window.chrome) { window.chrome = { runtime: {} }; }
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
            Promise.resolve({ state: 'denied' }) :
            originalQuery(parameters)
        );
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-IN', 'en-GB', 'en-US', 'en'],
        });
    """)

async def dismiss_overlays(page):
    """
    Aggressively closes popups using both JS removal and Button Clicking.
    """
    # 1. Javascript Removal (Targeting common overlay containers)
    try:
        await page.evaluate("""() => {
            const ids = [
                "strEchApp_ovrlay", "aviatrix-container_overlay", "mainPopupWrpr",
                "popup-overlay", "modal-overlay", "app-download-popup", "switchuser_riv"
            ];
            ids.forEach(id => {
                const el = document.getElementById(id);
                if (el) el.remove();
            });
            const classes = [
                "modal-backdrop", "fade", "show", "overlay", "popup-container"
            ];
            classes.forEach(cls => {
                const els = document.getElementsByClassName(cls);
                for(let i=0; i<els.length; i++) els[i].remove();
            });
        }""")
    except: pass

    # 2. Click Known Close Buttons
    close_patterns = [
        "button.animCLseBtn", "button.mnPopupClose", ".popup-close", ".modal-close",
        "button[aria-label='Close']", "[class*='close']", ".close-btn",
        ".pgSoftClsBtn"
    ]

    for selector in close_patterns:
        try:
            # Only try clicking if visible (avoids waiting)
            if await page.locator(selector).first.is_visible():
                await page.locator(selector).first.click(timeout=500)
                await asyncio.sleep(0.2)
        except: pass

# ================= CORE LOGIC =================
async def process_account(browser, account, selectors):
    username = account["username"]
    password = account["password"]

    # 1. Setup Context (Combined Settings)
    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        locale="en-IN",
        timezone_id="Asia/Kolkata",
        geolocation={"latitude": 17.3850, "longitude": 78.4867},
        permissions=["geolocation"],
        extra_http_headers={"Accept-Language": "en-IN,en;q=0.9"}
    )
    
    # Block media to speed up
    await context.route("**/*", lambda route: route.abort() 
        if route.request.resource_type in ["image", "media", "font"] 
        else route.continue_())

    page = await context.new_page()
    await apply_stealth(page)

    try:
        # 2. Navigate
        print(f"[{username}] Navigating...", flush=True)
        try:
            await page.goto(selectors["website"], wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            if "Timeout" in str(e):
                # Check if we got blocked during timeout
                title = await page.title()
                if not title: raise Exception("BLOCKED: Timeout & Empty Title")
            raise e

        # 3. Check for IP Block
        title = await page.title()
        if not title or title.strip() == "":
            raise Exception("BLOCKED: Generated empty page title.")

        # 4. Login (Hybrid Approach)
        print(f"[{username}] Logging in...", flush=True)
        
        # Clear initial popups
        for _ in range(2):
            await dismiss_overlays(page)
            await asyncio.sleep(0.5)

        # Click Login Button (Robust)
        login_clicked = False
        try:
            # Try standard click first
            await page.click(selectors["landing_page_login_button"], timeout=3000)
            login_clicked = True
        except:
            # Fallback to JS Click (Bypasses overlays)
            print(f"[{username}] Standard click failed. Using JS click...", flush=True)
            await page.evaluate(f"""() => {{
                const btn = document.querySelector('{selectors["landing_page_login_button"]}');
                if(btn) btn.click();
            }}""")
            login_clicked = True
        
        # Wait for Modal to Open
        try:
            await page.wait_for_selector(selectors["username_field"], state="visible", timeout=5000)
        except:
            if not login_clicked:
                # Last resort: search for "Login" text and click it
                await page.click("text=Login", timeout=2000)

        # Fill Credentials
        await dismiss_overlays(page)
        await page.fill(selectors["username_field"], username)
        await page.fill(selectors["password_field"], password)
        await page.press(selectors["password_field"], "Enter")

        # 5. Smart Balance Wait (Polling Loop)
        print(f"[{username}] Login submitted. Waiting for balance...", flush=True)
        
        balance = "N/A"
        start_time = asyncio.get_event_loop().time()
        
        # Poll for 30 seconds
        while (asyncio.get_event_loop().time() - start_time) < 30:
            try:
                # Crucial: Close popups inside the loop!
                await dismiss_overlays(page)
                
                # Check balance
                bal_el = page.locator(selectors["avaliable_balance"])
                if await bal_el.is_visible():
                    text = (await bal_el.inner_text()).strip()
                    
                    # VALIDATION: Must contain digits and NOT be "Loading..."
                    if any(c.isdigit() for c in text) and "loading" not in text.lower():
                        balance = text
                        print(f"[{username}] SUCCESS: Balance loaded: {balance}", flush=True)
                        break
            except: 
                pass
            
            await asyncio.sleep(1) # Wait 1s between checks

        if balance != "N/A":
            return {"username": username, "password": password, "balance": balance, "status": "Success", "error": ""}
        else:
            # Save screenshot for debugging
            os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
            await page.screenshot(path=f"{SCREENSHOTS_DIR}/{username}_failed.png")
            raise Exception("Balance check timed out (Popups might have blocked it)")

    except Exception as e:
        raise e
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
        
        # Random Delay (Important for Anti-Ban)
        delay = random.uniform(2, 8)
        print(f"[Worker {worker_id}] Sleeping {delay:.2f}s...", flush=True)
        await asyncio.sleep(delay)

        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"[Worker {worker_id}] {username} | Attempt {attempt}")
                result = await process_account(browser, account, selectors)
                
                await save_result(result)
                await save_progress(username)
                async with stats_lock: STATS["success"] += 1
                success = True
                break

            except Exception as e:
                err = str(e)
                print(f"[Worker {worker_id}] Error: {err[:100]}")
                
                if "BLOCKED" in err:
                    print("CRITICAL: IP seems blocked. Stopping worker.")
                    async with stats_lock: STATS["failed"] += 1
                    queue.task_done()
                    return

                if attempt == MAX_RETRIES:
                    async with stats_lock: STATS["failed"] += 1
                    await save_result({"username": username, "password": account["password"], "balance": "N/A", "status": "Failed", "error": err})
                else:
                    await asyncio.sleep(3)

        await log_progress()
        queue.task_done()

# ================= MAIN =================
async def main():
    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
    if not os.path.exists(SELECTORS_FILE):
        print(f"Error: {SELECTORS_FILE} missing.")
        return

    with open(SELECTORS_FILE, "r") as f: selectors = json.load(f)
    completed = load_progress()

    if not os.path.exists(ACCOUNTS_FILE):
        print(f"Error: {ACCOUNTS_FILE} missing.")
        return

    accounts = []
    with open(ACCOUNTS_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["username"] and row["username"] not in completed:
                accounts.append(row)

    STATS["total"] = len(accounts)
    queue = asyncio.Queue()
    for acc in accounts: queue.put_nowait(acc)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--headless=new",
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars"
            ]
        )
        
        workers = [asyncio.create_task(worker(i, queue, browser, selectors)) for i in range(MAX_WORKERS)]
        await asyncio.gather(*workers)
        await browser.close()

    print("All done.")

if __name__ == "__main__":
    asyncio.run(main())
