import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime, timedelta
import threading

# DB Config - Should ideally come from a config file or env
DB_PARAMS = {
    "host": "127.0.0.1",
    "port": 25432,
    "user": "postgres",
    "password": "postgres",
    "dbname": "postgres"
}

PRESSURE_THRESHOLD = 30.0
MIN_DURATION = 30.0
MIN_MAX_PRESSURE = 100.0

class CycleProcessor:
    def __init__(self, log_callback=None, progress_callback=None):
        self.log_callback = log_callback
        self.progress_callback = progress_callback
        self._stop_event = threading.Event()

    def log(self, message):
        if self.log_callback:
            self.log_callback(message)
        else:
            print(message)

    def stop(self):
        self._stop_event.set()

    def get_db_connection(self):
        return psycopg2.connect(**DB_PARAMS)

    def load_work_logs(self, start_from):
        conn = self.get_db_connection()
        query = """
            SELECT id as work_log_id, machine_id, start_time, end_time, die_id
            FROM tb_work_log
            WHERE end_time >= %s OR end_time IS NULL
        """
        df = pd.read_sql(query, conn, params=(start_from,))
        conn.close()
        
        if df.empty:
            return df
            
        df['start_time'] = pd.to_datetime(df['start_time'], utc=True)
        df['end_time'] = pd.to_datetime(df['end_time'], utc=True)
        
        df = df.sort_values(['machine_id', 'start_time'])
        df['next_start'] = df.groupby('machine_id')['start_time'].shift(-1)
        df['end_time'] = df['end_time'].fillna(df['next_start'])
        df['end_time'] = df['end_time'].fillna(pd.Timestamp.now(tz='UTC'))
        
        return df

    def process_chunk(self, metrics_df, work_log_df):
        metrics_df['is_active'] = metrics_df['main_pressure'] > PRESSURE_THRESHOLD
        metrics_df['active_change'] = metrics_df['is_active'].astype(int).diff()
        
        starts = metrics_df[metrics_df['active_change'] == 1].index
        ends = metrics_df[metrics_df['active_change'] == -1].index
        
        cycles = []
        s_ptr = 0
        e_ptr = 0
        
        while s_ptr < len(starts) and e_ptr < len(ends):
            if self._stop_event.is_set(): break
            
            s_idx = starts[s_ptr]
            while e_ptr < len(ends) and ends[e_ptr] < s_idx:
                e_ptr += 1
            if e_ptr >= len(ends): break
            e_idx = ends[e_ptr]
            
            start_time = metrics_df.loc[s_idx, 'timestamp']
            end_time = metrics_df.loc[e_idx, 'timestamp']
            duration = (end_time - start_time).total_seconds()
            
            cycle_slice = metrics_df.loc[s_idx:e_idx]
            max_p = cycle_slice['main_pressure'].max()
            cycle_counter = metrics_df.loc[e_idx, 'production_counter']
            
            is_valid = (duration >= MIN_DURATION) and (max_p >= MIN_MAX_PRESSURE)
            
            machine_id = '2호기(창녕)' 
            
            work_log_id = None
            if not work_log_df.empty:
                match = work_log_df[
                    (work_log_df['machine_id'] == machine_id) &
                    (work_log_df['start_time'] <= start_time) &
                    (work_log_df['end_time'] >= start_time)
                ]
                if not match.empty:
                    work_log_id = match.iloc[0]['work_log_id']
                
            cycles.append((
                machine_id,
                start_time,
                end_time,
                int(cycle_counter) if pd.notnull(cycle_counter) else None,
                int(work_log_id) if work_log_id is not None else None,
                float(duration),
                float(max_p),
                bool(is_valid),
                bool(not is_valid)
            ))
            
            s_ptr += 1
            e_ptr += 1
            
        return cycles

    def run_incremental(self):
        self._stop_event.clear()
        try:
            self.update_progress(0.0)
            conn = self.get_db_connection()
            cur = conn.cursor()
            
            self.log("Checking last processed time...")
            cur.execute("SELECT MAX(end_time) FROM tb_cycle_log")
            last_processed = cur.fetchone()[0]
            
            if last_processed is None:
                self.log("No existing cycles found. Please run full history processing first.")
                cur.close()
                conn.close()
                return

            self.log(f"Last processed: {last_processed}")
            self.update_progress(0.2)
            
            start_time = last_processed - timedelta(minutes=1)
            
            self.log("Loading work logs...")
            work_log_df = self.load_work_logs(start_time)
            self.update_progress(0.4)
            
            self.log("Fetching new metrics...")
            query = """
                SELECT timestamp, main_pressure, production_counter
                FROM all_metrics
                WHERE timestamp >= %s
                ORDER BY timestamp ASC
            """
            metrics_df = pd.read_sql(query, conn, params=(start_time,))
            
            if metrics_df.empty:
                self.log("No new data found.")
                self.update_progress(1.0)
                cur.close()
                conn.close()
                return
                
            metrics_df['timestamp'] = pd.to_datetime(metrics_df['timestamp'], utc=True)
            self.log(f"Fetched {len(metrics_df)} rows. Processing...")
            self.update_progress(0.6)
            
            cycles = self.process_chunk(metrics_df, work_log_df)
            
            if self._stop_event.is_set():
                self.log("Processing stopped by user.")
                cur.close()
                conn.close()
                return

            self.update_progress(0.8)
            new_cycles = [c for c in cycles if c[2] > last_processed]
            
            if new_cycles:
                self.log(f"Found {len(new_cycles)} new cycles. Inserting...")
                insert_query = """
                    INSERT INTO tb_cycle_log 
                    (machine_id, start_time, end_time, production_counter, work_log_id, duration_sec, max_pressure, is_valid, is_test_run)
                    VALUES %s
                """
                execute_values(cur, insert_query, new_cycles)
                conn.commit()
                self.log("Incremental update complete!")
            else:
                self.log("No new complete cycles found.")
            
            self.update_progress(1.0)
            cur.close()
            conn.close()
            
        except Exception as e:
            self.log(f"Error during processing: {e}")
    
    def update_progress(self, value):
        """Call progress callback if available"""
        if self.progress_callback:
            self.progress_callback(value)
    
    def run_range(self, mode='all', custom_date=None):
        """Process cycles for a specific date range"""
        self._stop_event.clear()
        try:
            self.update_progress(0.0)
            conn = self.get_db_connection()
            cur = conn.cursor()
            
            # Determine start/end times based on mode
            from datetime import timezone
            KST = timezone(timedelta(hours=9))
            now = datetime.now(KST)
            
            if mode == 'all':
                self.log("Processing entire history...")
                cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM all_metrics")
                start_time, end_time = cur.fetchone()
            elif mode == 'today':
                start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
                end_time = now
            elif mode == 'yesterday':
                yesterday = now - timedelta(days=1)
                start_time = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
                end_time = yesterday.replace(hour=23, minute=59, second=59)
            elif mode == 'custom' and custom_date:
                try:
                    start_time = pd.to_datetime(custom_date).tz_localize(KST)
                    end_time = now
                except:
                    self.log(f"Invalid date format: {custom_date}")
                    return
            else:
                self.log("Invalid mode or missing custom_date")
                return
                
            self.log(f"Processing range: {start_time} ~ {end_time}")
            self.update_progress(0.1)
            
            # Load work logs for this range
            self.log("Loading work logs...")
            work_log_df = self.load_work_logs(start_time)
            self.update_progress(0.2)
            
            # Process in chunks (hourly)
            current_ts = start_time
            total_cycles_inserted = 0
            chunk_count = 0
            
            while current_ts < end_time:
                if self._stop_event.is_set():
                    self.log("Processing stopped by user.")
                    break
                    
                next_ts = current_ts + timedelta(hours=1)
                if next_ts > end_time:
                    next_ts = end_time + timedelta(seconds=1)
                
                chunk_count += 1
                self.log(f"Processing chunk {chunk_count}: {current_ts} ~ {next_ts}...")
                
                # Fetch chunk
                query = """
                    SELECT timestamp, main_pressure, production_counter
                    FROM all_metrics
                    WHERE timestamp >= %s AND timestamp < %s
                    ORDER BY timestamp ASC
                """
                chunk_df = pd.read_sql(query, conn, params=(current_ts, next_ts))
                
                if not chunk_df.empty:
                    chunk_df['timestamp'] = pd.to_datetime(chunk_df['timestamp'], utc=True)
                    
                    # Process chunk
                    cycles = self.process_chunk(chunk_df, work_log_df)
                    
                    if cycles:
                        insert_query = """
                            INSERT INTO tb_cycle_log 
                            (machine_id, start_time, end_time, production_counter, work_log_id, duration_sec, max_pressure, is_valid, is_test_run)
                            VALUES %s
                        """
                        execute_values(cur, insert_query, cycles)
                        conn.commit()
                        total_cycles_inserted += len(cycles)
                        self.log(f"  -> Found {len(cycles)} cycles. Total: {total_cycles_inserted}")
                
                # Update progress (0.2 to 0.9 range based on time progress)
                time_progress = (current_ts - start_time).total_seconds() / (end_time - start_time).total_seconds()
                self.update_progress(0.2 + time_progress * 0.7)
                
                current_ts = next_ts
            
            self.log(f"Range processing complete! Total cycles: {total_cycles_inserted}")
            self.update_progress(1.0)
            
            cur.close()
            conn.close()
            
        except Exception as e:
            self.log(f"Error during range processing: {e}")
            import traceback
            self.log(traceback.format_exc())

