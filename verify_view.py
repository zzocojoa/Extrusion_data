
import os
import asyncio
import httpx
from core.config import load_config

async def verify_view():
    cfg, _ = load_config()
    base_url = cfg.get("SUPABASE_URL")
    anon_key = cfg.get("SUPABASE_ANON_KEY")
    
    if not base_url or not anon_key:
        print("Error: Supabase credentials not found in config.")
        return

    # Ensure URL doesn't end with slash
    base_url = base_url.rstrip("/")
    rest_url = f"{base_url}/rest/v1"

    headers = {
        "apikey": anon_key,
        "Authorization": f"Bearer {anon_key}",
        "Content-Type": "application/json"
    }
    
    # Increase timeout just in case
    timeout = httpx.Timeout(10.0, connect=5.0)

    print(f"Connecting to Supabase REST API: {rest_url}")

    async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
        try:
            # [Step 0] Get a valid cycle ID from raw table to test with
            print("\n[0] Fetching a sample cycle ID from 'all_metrics'...")
            # We want a cycle that likely has data. Sort desc by time to get recent.
            resp = await client.get(f"{rest_url}/all_metrics?select=billet_cycle_id&billet_cycle_id=not.is.null&limit=1&order=timestamp.desc")
            
            if resp.status_code != 200:
                print(f"Error fetching raw data: {resp.status_code} {resp.text}")
                return
            
            raw_data = resp.json()
            if not raw_data:
                print("No raw data found in 'all_metrics'.")
                return
                
            test_cycle_id = raw_data[0]['billet_cycle_id']
            print(f"Targeting Cycle ID: {test_cycle_id}")

            # [Step 0.5] Verify Index / Sort Performance
            # The View does: PARTITION BY billet_cycle_id ORDER BY timestamp
            # We simulate this on the raw table. If this is slow, Index is missing.
            print("\n[0.5] Testing Raw Table Sort Performance (Index Check)...")
            try:
                # Request sorted data for one cycle. 
                # If index "idx_metrics_cycle_time" exists, this should be instant.
                resp = await client.get(f"{rest_url}/all_metrics?select=timestamp&billet_cycle_id=eq.{test_cycle_id}&order=timestamp.asc&limit=1", timeout=2.0)
                if resp.status_code == 200:
                    print(" - Sort Check: OK (Fast response implies Index might exist or data is small)")
                else:
                     print(f" - Sort Check: Failed with {resp.status_code}")
            except httpx.ReadTimeout:
                print(" - Sort Check: TIMEOUT! (CRITICAL: Index 'idx_metrics_cycle_time' is definitely MISSING)")
                print("   Action: You MUST run the CREATE INDEX SQL provided.")
                return 

            # [Step 1] Check View for this specific cycle
            # This helps the DB optimizer focus on one partition
            print(f"\n[1] Checking View for Cycle {test_cycle_id}...")
            
            cnt_headers = headers.copy()
            cnt_headers["Prefer"] = "count=exact"
            
            # Query view with filter
            resp = await client.get(f"{rest_url}/view_aligned_metrics?select=timestamp&billet_cycle_id=eq.{test_cycle_id}&limit=1", headers=cnt_headers)
            
            if resp.status_code != 200:
                print(f"Error fetching view: {resp.status_code} {resp.text}")
                # If timeout still happens, it's definitely an index issue or view complexity
                print("Hint: If 500 Timeout, please create the Index suggested.")
                return

            print(f"Success! View returns data for cycle {test_cycle_id}.")

            # [Step 2] Check for any Offset existence (Global check might still timeout, try recent)
            # We'll skip the global check if it's too heavy. 
            # Let's try to check offset for the TEST cycle.
            print(f"\n[2] Checking Alignment Offset for Cycle {test_cycle_id}...")
            
            resp = await client.get(f"{rest_url}/view_aligned_metrics?select=_debug_offset_rows&billet_cycle_id=eq.{test_cycle_id}&limit=1")
            if resp.status_code == 200:
                 d = resp.json()
                 if d:
                     print(f" - Cycle {test_cycle_id}: Offset {d[0]['_debug_offset_rows']} rows")
                 else:
                     print("No data returned for offset check.")
            else:
                print(f"Error fetching offset: {resp.status_code}")

        except Exception as e:
            print(f"Verification failing: {e}")

if __name__ == "__main__":
    asyncio.run(verify_view())
