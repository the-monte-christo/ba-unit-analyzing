import duckdb
import time
import os

def convert_csv_to_parquet(csv_file):
    """
    Konvertiert CSV zu Parquet-Datei
    """
    print("🔄 CSV → Parquet Konvertierung...")
    print("="*50)
    
    # File-Namen
    parquet_file = csv_file.replace('.csv', '.parquet')
    
    # Größe checken
    csv_size_gb = os.path.getsize(csv_file) / (1024**3)
    print(f"📁 Input:  {csv_file} ({csv_size_gb:.1f} GB)")
    print(f"📁 Output: {parquet_file}")
    
    # DuckDB
    print("🚀 Starte Konvertierung mit DuckDB...")
    start_time = time.time()
    
    conn = duckdb.connect()
    
    query = f"""
    COPY (SELECT * FROM '{csv_file}') 
    TO '{parquet_file}' 
    (FORMAT PARQUET, COMPRESSION 'SNAPPY')
    """
    
    try:
        conn.execute(query)
        conversion_time = time.time() - start_time
        
        # Ergebnis checken
        parquet_size_gb = os.path.getsize(parquet_file) / (1024**3)
        compression_ratio = (1 - parquet_size_gb / csv_size_gb) * 100
        
        print("✅ FERTIG!")
        print("="*50)
        print(f"⏱️  Zeit:        {conversion_time/60:.1f} Minuten")
        print(f"📊 CSV:         {csv_size_gb:.2f} GB")
        print(f"📦 Parquet:     {parquet_size_gb:.2f} GB") 
        print(f"💾 Ersparnis:   {compression_ratio:.0f}%")
        
    except Exception as e:
        print(f"❌ Fehler: {e}")
    
    finally:
        conn.close()

# So benutzt du es:
if __name__ == "__main__":
    # Deine CSV-Datei
    csv_file = "G:/DataMining/battles/broken_arrow/units.csv"  # ← Dein Dateiname
    
    # Konvertierung
    convert_csv_to_parquet(csv_file)