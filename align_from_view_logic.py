
import pandas as pd
import numpy as np
import os
import sys

# Constants matching SQL Logic
# Logic 1: Real Start 
# ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
START_PAST_WINDOW = 60 
START_SPEED_TRIGGER = 0.1
# Logic using MIN (Corrected from SQL confusion)
START_PAST_MIN_SPEED = 0.8  
# Logic for finding exact zero point (10 rows back)
START_ZERO_SEARCH_WINDOW = 10
START_ZERO_SPEED_THRESHOLD = 0.05

# Logic 2: Stable Phase
# ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING (Modified Window)
STABLE_PAST_WINDOW = 30
STABLE_SPEED_TRIGGER = 1.0
STABLE_PAST_MIN_SPEED = 1.0 # Logic using MIN
STABLE_FUTURE_WINDOW = 10
STABLE_VARIATION_LIMIT = 0.05

def align_csv_file(input_path, output_path):
    print(f"Processing: {input_path}")
    
    # 1. Load Data
    try:
        df = pd.read_csv(input_path)
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Normalize columns
    df.columns = df.columns.str.strip().str.lower()
    
    required_cols = ['time', 'billet_cycle_id', 'current_speed', 'temperature']
    for c in required_cols:
        if c not in df.columns:
            print(f"Missing required column: {c}")
            return

    # Ensure sorting
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values(['billet_cycle_id', 'time']).reset_index(drop=True)

    aligned_dfs = []
    
    # Process by Cycle (Partition)
    cycles = df['billet_cycle_id'].dropna().unique()
    print(f"Found {len(cycles)} cycles.")

    for cycle_id in cycles:
        cdf = df[df['billet_cycle_id'] == cycle_id].copy().reset_index(drop=True)
        n = len(cdf)
        
        if n < 60:
            aligned_dfs.append(cdf)
            continue
            
        # Simulating SQL Window Functions in Pandas
        # To avoid slow iteration, we use rolling windows where possible, 
        # but for complex step-finding logic, iteration is often clearer and robust for this size.
        
        # [Logic 1] Real Start Point
        # SQL: MIN(current_speed) OVER (ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING)
        # Pandas: rolling(60).min().shift(1) (approx)
        
        real_start_rn = None
        
        # Optimization: Pre-calculate rolling min
        # rolling(60) includes current, so shift(1) to make it PRECEDING 60..1
        past_60_min = cdf['current_speed'].rolling(window=START_PAST_WINDOW, closed='left').min()
        
        # Iterate to find first trigger match
        for i in range(START_PAST_WINDOW, n):
            current_spd = cdf.at[i, 'current_speed']
            min_past = cdf['current_speed'].iloc[i-START_PAST_WINDOW:i].min() # Safe slice
            
            if current_spd >= START_SPEED_TRIGGER and min_past < START_PAST_MIN_SPEED:
                # Found Real Start Candidate via Trigger
                # Now finding exact zero point in past 10 rows
                # SQL: MAX(CASE WHEN < 0.05 THEN rn) over 10 PRECEDING
                
                # Search back 10 rows for speed < 0.05
                search_end = i
                search_start = max(0, i - START_ZERO_SEARCH_WINDOW)
                
                last_zero_rn = i # Default to current if not detected (COALESCE)
                found_zero = False
                
                for j in range(search_end - 1, search_start - 1, -1):
                    if cdf.at[j, 'current_speed'] < START_ZERO_SPEED_THRESHOLD:
                        last_zero_rn = j
                        found_zero = True
                        break
                
                real_start_rn = last_zero_rn
                break # SQL: MIN(start_rn) -> Stop at first valid occurrence
        
        if real_start_rn is None:
            # No start found, no alignment
            aligned_dfs.append(cdf)
            continue

        # [Logic 2] Stable Phase
        # SQL: MIN(current_speed) over 30 PRECEDING
        stable_rn = None
        candidates = []
        
        for i in range(STABLE_PAST_WINDOW, n - STABLE_FUTURE_WINDOW):
            current_spd = cdf.at[i, 'current_speed']
            
            # Trigger
            if current_spd > STABLE_SPEED_TRIGGER:
                # Context Check (Past 30)
                min_past = cdf['current_speed'].iloc[i-STABLE_PAST_WINDOW:i].min()
                
                if min_past < STABLE_PAST_MIN_SPEED:
                    # Stability Check (Future 10)
                    future_slice = cdf['current_speed'].iloc[i:i+STABLE_FUTURE_WINDOW]
                    avg_fut = future_slice.mean()
                    max_fut = future_slice.max()
                    min_fut = future_slice.min()
                    
                    if avg_fut > 0:
                        variation = (max_fut - min_fut) / avg_fut
                        if variation <= STABLE_VARIATION_LIMIT:
                            # It's a candidate
                            temp = cdf.at[i, 'temperature']
                            candidates.append((i, temp))

        if not candidates:
             aligned_dfs.append(cdf)
             continue
             
        # SQL: ORDER BY temperature ASC, timestamp ASC LIMIT 1
        candidates.sort(key=lambda x: (x[1], x[0]))
        stable_rn = candidates[0][0]
        
        # [Calculate Offset]
        offset = stable_rn - real_start_rn
        
        # [Apply Shift]
        # SQL: t2.rn = t1.rn + offset
        # Pandas: shift(-offset) pushes data UP (Left)
        
        print(f"Cycle {cycle_id}: Shift {offset} rows (Start@{real_start_rn} -> Stable@{stable_rn})")
        
        if offset != 0:
            cdf['temperature'] = cdf['temperature'].shift(-offset)
            # The last 'offset' rows will be NaN.
            
        aligned_dfs.append(cdf)

    # Recombine
    final_df = pd.concat(aligned_dfs, ignore_index=True)
    final_df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")

if __name__ == "__main__":
    if len(sys.argv) > 2:
        align_csv_file(sys.argv[1], sys.argv[2])
    else:
        # Default fallback for testing
        input_f = "New panel-data-2025-12-16 14_02_32.csv"
        output_f = "New panel-data-shifted-v2.csv"
        if os.path.exists(input_f):
            align_csv_file(input_f, output_f)
        else:
            print("Usage: python align_from_view_logic.py <input_csv> <output_csv>")
