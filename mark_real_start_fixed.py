import pandas as pd
import numpy as np
import os

def process_and_mark_real_start():
    # Configuration
    input_path = "Factory_Integrated_Log_20251216_000000.csv"
    output_path = "Factory_Integrated_Log_20251216_000000_processed_v2.csv"
    
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return

    print(f"Reading Raw CSV: {input_path}...")
    try:
        df = pd.read_csv(input_path, encoding='cp949')
    except Exception:
        print("cp949 failed, trying utf-8-sig...")
        df = pd.read_csv(input_path, encoding='utf-8-sig')

    # ---------------------------------------------------------
    # STEP 1: Recalculate Billet_CycleID (Hybrid Logic)
    # ---------------------------------------------------------
    print("Step 1: Recalculating Cycle IDs (Hybrid: Fix Noise, Keep Nulls)...")
    
    # Logic: continuous blocks of speed >= 0.1
    df['is_active'] = df['현재속도'] >= 0.1
    df['block_id'] = (df['is_active'] != df['is_active'].shift()).cumsum()
    
    # Map blocks to Cycle ID using Mode of '생산카운터'
    block_groups = df[df['is_active']].groupby('block_id')
    block_cycle_map = {}
    
    for bid, group in block_groups:
        mode_val = group['생산카운터'].mode()
        if not mode_val.empty:
            block_cycle_map[bid] = mode_val.iloc[0]
            
    # Assign new IDs
    def get_new_id(row):
        if not row['is_active']:
            return np.nan
        return block_cycle_map.get(row['block_id'], np.nan)
        
    # [Hybrid Logic]
    # 1. Calculate New IDs
    df['Recalculated_ID'] = df.apply(get_new_id, axis=1)
    
    # 2. Preserve Original Null Locations
    df['Original_ID'] = pd.to_numeric(df['Billet_CycleID'], errors='coerce')
    original_null_mask = df['Original_ID'].isna()
    
    # 3. Apply Recalculated ID
    df['Billet_CycleID'] = df['Recalculated_ID'].astype('Int64')
    
    # 4. Restore Nulls
    if original_null_mask.any():
        df.loc[original_null_mask, 'Billet_CycleID'] = np.nan
        print(f" - Restored {original_null_mask.sum()} Null IDs (kept distinct from 0-noise).")
    
    # Drop temp cols
    df.drop(columns=['Recalculated_ID', 'Original_ID'], inplace=True, errors='ignore')

    # Generate Cycle_Start_Signal (First row of each ID)
    df['Cycle_Start_Signal'] = 0
    valid_ids = df[df['Billet_CycleID'].notna()]
    if not valid_ids.empty:
        # dropped duplicates keeps first occurrence
        start_indices = valid_ids.drop_duplicates(subset=['Billet_CycleID'], keep='first').index
        df.loc[start_indices, 'Cycle_Start_Signal'] = 1
        print(f" - Identified {len(start_indices)} cycle starts.")

    # ---------------------------------------------------------
    # STEP 2: Mark Real Start Point (Logic 3: Min Temp < 530)
    # ---------------------------------------------------------
    print("Step 2: Marking Real Start Points (Min Temp < 530)...")
    
    # Filter for rows with valid cycle IDs
    valid_cycles_df = df[df['Billet_CycleID'].notna()]
    
    if not valid_cycles_df.empty:
        # [MODIFIED] Limit search to first 300 rows per cycle to avoid tail-end cooling
        search_limit = 300
        valid_indices = []
        
        # Iterate groups manually to apply head(300) limit robustly
        for cycle_id, group in valid_cycles_df.groupby('Billet_CycleID'):
            # Take only the first N rows
            subset = group.head(search_limit)
            
            if subset.empty:
                continue
                
            # Find index of min temp in this subset
            min_idx = subset['Temperature'].idxmin()
            
            # Check threshold
            try:
                # Need to lookup value in original df using the index
                val = df.at[min_idx, 'Temperature']
                if val < 530:
                    valid_indices.append(min_idx)
            except KeyError:
                pass
                
        # 3. Mark detected points
        df['Real_Start_Point'] = 0
        if valid_indices:
            df.loc[valid_indices, 'Real_Start_Point'] = 1
        
        points_found = len(valid_indices)
        print(f" - Found {points_found} Real Start Points (Min Temp < 530 within first {search_limit} rows).")
    else:
        print(" - No valid cycles found for detection.")

    # ---------------------------------------------------------
    # STEP 3: Save Output
    # ---------------------------------------------------------
    # Define Column Order (Standard + New Flags)
    final_cols = [
        "Date", "Time", "Temperature", "메인압력", "빌렛길이",
        "콘테이너온도 앞쪽", "콘테이너온도 뒷쪽", "생산카운터", "현재속도",
        "압출종료 위치", "Mold1", "Mold2", "Mold3", "Mold4",
        "Mold5", "Mold6", "Billet_Temp", "At_Pre", "At_Temp",
        "DIE_ID", "Billet_CycleID",
        "Cycle_Start_Signal",
        "Real_Start_Point",
        "datetime"
    ]
    
    out_cols = [c for c in final_cols if c in df.columns]
    
    print(f"Saving to {output_path}...")
    df[out_cols].to_csv(output_path, index=False, encoding='cp949', errors='replace')
    print("Done.")

if __name__ == "__main__":
    process_and_mark_real_start()
