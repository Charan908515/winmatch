import asyncio
import csv
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 1
MAX_RETRIES = 2

ACCOUNTS_FILE = "ch_accounts_reversed.csv"
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

# ================= AGGRESSIVE POPUP REMOVAL =================
async def nuclear_popup_removal(page, username):
    """
    NUCLEAR option: Remove ALL overlays, modals, and high z-index elements.
    This is very aggressive and removes anything that looks like a popup.
    """
    try:
        removed_count = await page.evaluate("""() => {
            let count = 0;
            
            // 1. Remove by common overlay IDs
            const overlayIds = [
                'strEchApp_ovrlay', 'aviatrix-container_overlay', 'mainPopupWrpr',
                'popup-overlay', 'modal-overlay', 'app-download-popup', 'switchuser_riv',
                'modal', 'popup', 'overlay', 'dialog'
            ];
            
            overlayIds.forEach(id => {
                const el = document.getElementById(id);
                if (el) { el.remove(); count++; }
            });
            
            // 2. Remove by common overlay classes
            const overlayClasses = [
                'modal-backdrop', 'overlay', 'popup-container', 'modal', 'popup',
                'dialog', 'fade', 'show', 'aviatrix-container_overlay', 
                'instamatch-container_overlay', 'mainPopupWrpr', 'switchuser_riv'
            ];
            
            overlayClasses.forEach(cls => {
                const els = document.getElementsByClassName(cls);
                while(els.length > 0) {
                    els[0].remove();
                    count++;
                }
            });
            
            // 3. Remove elements with very high z-index (likely popups/overlays)
            const allElements = document.querySelectorAll('*');
            allElements.forEach(el => {
                const zIndex = parseInt(window.getComputedStyle(el).zIndex);
                if (zIndex > 9000) {
                    // Check if it's actually covering the screen
                    const rect = el.getBoundingClientRect();
                    if (rect.width > 500 && rect.height > 300) {
                        el.remove();
                        count++;
                    }
                }
            });
            
            // 4. Remove fixed/absolute positioned large elements (common popup pattern)
            document.querySelectorAll('div').forEach(div => {
                const style = window.getComputedStyle(div);
                const position = style.position;
                const display = style.display;
                
                if ((position === 'fixed' || position === 'absolute') && display !== 'none') {
                    const rect = div.getBoundingClientRect();
                    const opacity = parseFloat(style.opacity);
                    const bgColor = style.backgroundColor;
                    
                    // If it's a large semi-transparent overlay or has dark background
                    if (rect.width > window.innerWidth * 0.8 && rect.height > window.innerHeight * 0.5) {
                        if (opacity < 1 || bgColor.includes('rgba') || bgColor === 'rgb(0, 0, 0)') {
                            div.remove();
                            count++;
                        }
                    }
                }
            });
            
            // 5. Force hide body overflow (some sites use this to lock scrolling)
            document.body.style.overflow = 'auto';
            
            return count;
        }""")
        
        if removed_count > 0:
            print(f"[{username}] 🧹 Removed {removed_count} overlay elements", flush=True)
            
    except Exception as e:
        print(f"[{username}] Nuclear removal error: {str(e)[:50]}", flush=True)

async def click_all_close_buttons(page, username):
    """
    Click every possible close button on the page.
    """
    close_selectors = [
        # Generic close buttons
        "button[aria-label='Close']",
        "button[title='Close']",
        "[class*='close']",
        "[class*='Close']",
        "[id*='close']",
        "[id*='Close']",
        "button.close",
        "button.btn-close",
        ".modal-close",
        ".popup-close",
        
        # Specific to your site
        "button.animCLseBtn",
        "button.mnPopupClose",
        "button.pgSoftClsBtn",
        ".animCLseBtn",
        ".mnPopupClose",
        ".pgSoftClsBtn",
        
        # Common patterns
        "button:has-text('×')",
        "button:has-text('X')",
        "button:has-text('Close')",
        "span:has-text('×')",
        
        # Aggressive: any button in a modal/overlay
        ".modal button",
        ".popup button",
        ".overlay button",
        "[class*='overlay'] button",
        "[class*='popup'] button",
        "[class*='modal'] button"
    ]
    
    clicked = 0
    for selector in close_selectors:
        try:
            # Get all matching elements
            elements = await page.locator(selector).all()
            for el in elements[:3]:  # Limit to first 3 matches per selector
                try:
                    if await el.is_visible():
                        await el.click(timeout=300, force=True)  # Force click ignores visibility checks
                        clicked += 1
                        await asyncio.sleep(0.1)
                except:
                    pass
        except:
            pass
    
    if clicked > 0:
        print(f"[{username}] 🎯 Clicked {clicked} close buttons", flush=True)

async def aggressive_popup_cleanup(page, username, rounds=3):
    """
    Multi-round aggressive popup cleanup.
    Combines JS removal and button clicking.
    """
    for round_num in range(rounds):
        await nuclear_popup_removal(page, username)
        await asyncio.sleep(0.3)
        await click_all_close_buttons(page, username)
        await asyncio.sleep(0.3)

# ================= PAGE LOAD DETECTION =================
async def wait_for_page_fully_loaded(page, username, timeout_seconds=30):
    """
    Wait for the page to be fully loaded by checking:
    1. document.readyState is 'complete'
    2. No pending network requests for a stable period
    3. jQuery animations finished (if jQuery exists)
    """
    print(f"[{username}] ⏳ Waiting for page to fully load (spinner check)...", flush=True)
    
    max_checks = timeout_seconds * 2  # Check every 0.5 seconds
    stable_count = 0
    required_stable_checks = 6  # Must be stable for 3 seconds (6 x 0.5s)
    
    for check in range(max_checks):
        try:
            # Check if document is ready
            is_ready = await page.evaluate("""() => {
                // Check 1: Document ready state
                if (document.readyState !== 'complete') {
                    return false;
                }
                
                // Check 2: No jQuery animations (if jQuery exists)
                if (typeof jQuery !== 'undefined' && jQuery(':animated').length > 0) {
                    return false;
                }
                
                // Check 3: No active fetch/XHR (check for common loading indicators)
                const loadingElements = document.querySelectorAll('[class*="loading"], [class*="spinner"], [id*="loading"], [id*="spinner"]');
                for (let el of loadingElements) {
                    const style = window.getComputedStyle(el);
                    if (style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0') {
                        return false;
                    }
                }
                
                return true;
            }""")
            
            if is_ready:
                stable_count += 1
                if stable_count >= required_stable_checks:
                    print(f"[{username}] ✅ Page fully loaded and stable", flush=True)
                    return True
            else:
                stable_count = 0
            
            await asyncio.sleep(0.5)
            
        except Exception as e:
            print(f"[{username}] ⚠️ Load check error: {str(e)[:50]}", flush=True)
            await asyncio.sleep(0.5)
    
    print(f"[{username}] ⚠️ Load check timeout - proceeding anyway", flush=True)
    return False

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
    
    # Enhanced Stealth Injection
    stealth_js = """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        window.navigator.chrome = { runtime: {} };
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
            Promise.resolve({ state: 'denied' }) :
            originalQuery(parameters)
        );
    """
    await context.add_init_script(stealth_js)
    
    page = await context.new_page()

    try:
        # Speed optimization
        await page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "media", "font","stylesheet"]
            else route.continue_(),
        )

        # 1. Navigate
        print(f"[{username}] 🌐 Navigating...", flush=True)
        await page.goto(selectors["website"], timeout=600000)
        await asyncio.sleep(2)
        
        # 2. Initial cleanup
        print(f"[{username}] 🧹 Initial cleanup...", flush=True)
        await aggressive_popup_cleanup(page, username, rounds=2)

        # 3. Click Login Button
        print(f"[{username}] 🔑 Clicking login...", flush=True)
        try:
            await page.click(selectors["landing_page_login_button"], timeout=50000)
        except:
            # Fallback JS click
            try:
                await page.evaluate(f"document.querySelector('{selectors['landing_page_login_button']}').click()")
            except:
                print(f"[{username}] ⚠️  Login button click failed", flush=True)
        
        await asyncio.sleep(1)
        # DISABLED: Don't remove popups here - it closes the login modal!
        # await aggressive_popup_cleanup(page, username, rounds=1)

        # 4. Fill Credentials
        print(f"[{username}] ✍️  Filling credentials...", flush=True)
        await page.fill(selectors["username_field"], username)
        await page.fill(selectors["password_field"], password)
        await page.press(selectors["password_field"], "Enter")
        
        print(f"[{username}] 🔐 Credentials submitted, NOW cleaning popups...", flush=True)

        # 5. Wait for Login
        print(f"[{username}] ⏳ Waiting for login...", flush=True)
        try:
            await page.wait_for_load_state(timeout=100000)
        except:
            await asyncio.sleep(3)  # Fallback wait
        
        # 5.5. Check URL to detect OTP requirement
        otp=False
        for i in range(20):
            await asyncio.sleep(1)
            current_url = page.url
            print(f"[{username}] 🔍 Current URL: {current_url}", flush=True)
        
        # If URL contains ?uid=, login was successful
        # If not, it's asking for OTP - skip this account
            if "?uid=" in current_url:
                otp=True
                break
            
        if not otp:
            print(f"[{username}] ⚠️  OTP required (URL doesn't contain uid) - skipping account", flush=True)
            raise Exception("OTP required - account skipped")
        # 6. POST-LOGIN: Most aggressive cleanup
        print(f"[{username}] 🧹🧹🧹 Post-login cleanup...", flush=True)
        await aggressive_popup_cleanup(page, username, rounds=4)  # 4 rounds!

        # 6.5. Wait for page to fully load (spinner disappeared)
        await wait_for_page_fully_loaded(page, username)

        # 7. Extract Balance with continuous popup fighting
        print(f"[{username}] 💰 Looking for balance...", flush=True)
        balance = "N/A"
        
        # Strategy: Keep fighting popups while waiting for balance
        max_attempts = 20  # 20 attempts = ~30 seconds
        for attempt in range(max_attempts):
            try:
                # Clean popups every iteration
                if attempt % 2 == 0:  # Every other attempt
                    await nuclear_popup_removal(page, username)
                    await click_all_close_buttons(page, username)
                
                # Try to get balance
                bal_loc = page.locator(selectors["avaliable_balance"])
                
                if await bal_loc.is_visible():
                    text = (await bal_loc.inner_text(timeout=1000)).strip()
                    
                    # Validate: has digits, not loading text
                    if text and any(c.isdigit() for c in text) and "LOADING" not in text.upper() and "..." not in text:
                        balance = text
                        print(f"[{username}] ✅ Balance found: {balance}", flush=True)
                        with open("output_chakri.txt","a",encoding="utf-8") as f:
                            f.write(f"{username},{password},{balance}\n")
                        break
                
            except:
                pass
            
            await asyncio.sleep(1.5)
        
        # 8. Take screenshot
        os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
        await page.screenshot(
            path=os.path.join(SCREENSHOTS_DIR, f"{username}.png"),
            full_page=True,
        )

        if balance != "N/A":
            return {
                "username": username,
                "password": password,
                "balance": balance,
                "status": "Success",
                "error": "",
            }
        else:
            raise Exception("Balance not found - popups may be blocking view")

    except Exception as e:
        print(f"[{username}] ❌ Error: {str(e)[:100]}", flush=True)
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

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"[Worker {worker_id}] 🚀 Processing: {username} (Attempt {attempt})", flush=True)
                result = await process_account(browser, account, selectors)

                if result["balance"]:
                    await save_result(result)
                    await save_progress(username)
                    print(f"[Worker {worker_id}] ✅ SUCCESS: {username}", flush=True)
                    break
                else:
                    raise Exception("Empty balance")

            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f"[Worker {worker_id}] ❌ FAILED: {username}", flush=True)
                    await save_result({
                        "username": username,
                        "password": account["password"],
                        "balance": "N/A",
                        "status": "Failed",
                        "error": str(e),
                    })

                    # Take error screenshot
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

    # 1. READ ALL ACCOUNTS
    all_accounts = []
    if not os.path.exists(ACCOUNTS_FILE):
        print(f"Error: {ACCOUNTS_FILE} not found.")
        return

    with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["username"]:
                all_accounts.append(row)

    # 2. SHARDING LOGIC (ADDED HERE)
    try:
        shard_index = int(os.getenv("SHARD_INDEX", 0))
        total_shards = int(os.getenv("TOTAL_SHARDS", 1))
    except:
        shard_index = 0
        total_shards = 1

    total_len = len(all_accounts)
    
    # If using Sharding, slice the list
    if total_shards > 1:
        chunk_size = (total_len + total_shards - 1) // total_shards
        start_idx = shard_index * chunk_size
        end_idx = min(start_idx + chunk_size, total_len)
        
        my_accounts_raw = all_accounts[start_idx:end_idx]
        print(f"--- SHARD {shard_index + 1}/{total_shards} ---")
        print(f"Processing range: {start_idx} to {end_idx} (Count: {len(my_accounts_raw)})")
    else:
        my_accounts_raw = all_accounts
        print(f"Processing all {len(my_accounts_raw)} accounts")

    # 3. FILTER COMPLETED
    completed = load_progress()
    accounts = [acc for acc in my_accounts_raw if acc["username"] not in completed]

    if not accounts:
        print("Nothing left to process in this shard.")
        return

    queue = asyncio.Queue()
    for acc in accounts:
        queue.put_nowait(acc)

    # Force headless in CI (GitHub Actions)
    is_ci = os.getenv("GITHUB_ACTIONS") == "true"
    use_headless = True if is_ci else False

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=use_headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        workers = [
            asyncio.create_task(worker(i, queue, browser, selectors))
            for i in range(MAX_WORKERS)
        ]

        await asyncio.gather(*workers)
        await browser.close()

    print("🎉 All done!")

if __name__ == "__main__":
    asyncio.run(main())
