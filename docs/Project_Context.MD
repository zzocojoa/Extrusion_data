# Project Context & Environment Summary (Master v4.1 - Final Optimized)

I am building a local data analysis pipeline for Aluminum Extrusion PLC data
using Supabase CLI.

**CRITICAL:** This project uses a specific local Docker setup with strict
network/auth rules. Do not suggest standard solutions that conflict with this
configuration.

## 1. Infrastructure (Local Hybrid)

- **Host OS:** Windows 10/11 (WSL2 Backend enabled)

- **Guest OS (Server):** Ubuntu (running on WSL2)

- **Physical Storage:**
  - **Database:** WSL Root Filesystem (`ext4.vhdx`).
  - **Backup Storage:** `E:\backups` (USB Drive).
    - **Note:** `E:\supabase-data.vhdx` is attached to WSL but NOT used for live
      DB storage. It is used as a mount point for USB backups.

## 2. Tech Stack & Network (Fixed)

- **Tool:** Supabase CLI (executed from Project Root:
  `/mnt/c/Users/user/Documents/GitHub/Extrusion_data`)

- **Shared Network:** `supabase_network_Extrusion_data`

- **Database (Supabase):**

  - Container Name: `supabase_db_Extrusion_data`

  - **External Port (Host/DBeaver):** `25432` (Mapped via `config.toml`)

  - **Internal Port (Docker/Grafana):** `5432`

- **Visualization (Grafana):**

  - Container Name: `grafana_local`

  - Connection Host: `supabase_db_Extrusion_data:5432` (Direct
    container-to-container)

  - URL: `http://localhost:3001`

## 3. Client Configuration (Python GUI)

- **File:** `core/upload.py`

- **Config Management:** `config.ini` (Git-ignored).

- **Auth Strategy:**

  - URL: `http://127.0.0.1:54321` (Kong Gateway)

  - Key: `sb_publishable_...` (Anon Key) from `config.ini`.

  - **CRITICAL HEADER:** Requests MUST include `apikey` header (same as Anon
    Key) to bypass Kong Gateway.

- **New Features (v4.2):**
  - **Smart Sync:** Checks server for latest data timestamp before uploading to
    avoid duplicates.
  - **Parallel Upload:** Uses `ThreadPoolExecutor` for concurrent file uploads
    (default 4 threads).
  - **Real-time Progress:** GUI displays individual file progress and overall
    status.

## 4. Edge Function Configuration (Deno)

- **File:** `supabase/functions/upload-metrics/index.ts`

- **Auth Strategy (Env Vars):**

  - **NO HARDCODED KEYS.** Code uses
    `Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')`.

  - **URL:** Uses `Deno.env.get('SUPABASE_URL')`.

- **Logic:**
  - `POST`: Uploads data batches.
  - `GET`: Returns latest timestamp for a given `device_id` (used for Smart
    Sync).
  - Manual header validation (`getAuthToken`) is REMOVED. Relying on Gateway
    validation.

## 5. Operational Procedures & Automation

- **System Startup (After Reboot):**

  1. **Windows (Admin PowerShell):**
     `wsl --mount --vhd "E:\supabase-data.vhdx" --bare`

  2. **Ubuntu (Terminal):**

     - `cd /mnt/c/Users/user/Documents/GitHub/Extrusion_data`

     - Run **`./startup.sh`** (Located in Project Root)
     - _This script checks for project directory and Docker status, starts
       Supabase/Grafana, and ensures Cron service is running._

- **Backup Strategy:**

  - Script: `./supabase/auto/backup_daily.sh` (Uses `.env` for credentials)
  - **Scope:** `public` schema only.
  - **Location:**
    1. Local: `~/Documents/GitHub/Extrusion_data/backups`
    2. USB: `/mnt/e/backups` (if mounted)
  - **Automation:** WSL Cron job scheduled at **18:30 Daily**.

- **Restore Strategy:** `./restore_latest.sh`

## 6. ⛔ Constraints & Forbidden Actions (Do Not Touch)

1. **NO SECRETS IN CODE:** Source files must use Environment Variables (`.env`)
   or config files ignored by Git.

2. **DO NOT use `localhost` inside Containers:** Use container names (e.g.,
   `supabase_db_Extrusion_data`).

3. **DO NOT touch `~/supabase/docker`:** Legacy path. Work ONLY in Project Root.

4. **DO NOT remove `apikey` header:** Python client fails without it.
