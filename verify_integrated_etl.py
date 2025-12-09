
import pandas as pd
from core.transform import build_records_plc

def test_integrated_log():
    # Use the existing sample file path
    file_path = "c:\\Users\\user\\Documents\\GitHub\\Extrusion_data\\Factory_Integrated_Log_20251209_145008.csv"
    
    print(f"Testing ETL with: {file_path}")
    
    try:
        df = build_records_plc(file_path, "251209") 
        # filename arg is used for legacy date parsing fallback, 
        # but integrated logic should override it using Date/Time cols.
        
        if df.empty:
            print("FAILED: DataFrame is empty.")
            return

        print("SUCCESS: DataFrame created.")
        print("Columns:", df.columns.tolist())
        print("Head (5 rows):")
        print(df.head())
        
        # Verify specific integrated columns exist
        expected_cols = ["mold_1", "billet_temp", "at_pre", "timestamp"]
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            print(f"FAILED: Missing columns: {missing}")
        else:
            print("SUCCESS: All expected integrated columns found.")
            
        # Verify timestamp parsing
        print(f"Timestamp Example: {df['timestamp'].iloc[0]}")
        if "device_id" in df.columns:
            print(f"WARNING: device_id still present: {df['device_id'].iloc[0]}")
        else:
            print("SUCCESS: device_id column correctly removed.")

    except Exception as e:
        with open("error.log", "w") as f:
            import traceback
            traceback.print_exc(file=f)
        print(f"ERROR: {e}")

if __name__ == "__main__":
    test_integrated_log()
