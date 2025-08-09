#parquet_file = "G:/DataMining/battles/broken_arrow/units.parquet"  # ← Dein Dateiname
#database_file = "G:/DataMining/database_unitsonly.json" 
import duckdb
import pandas as pd
import json

def load_unit_database(database_json_file):
    """
    Lädt die Unit-Database und erstellt ein Mapping: ID -> HUDName
    """
    print(f"📚 Lade Unit-Database: {database_json_file}")
    
    try:
        with open(database_json_file, 'r', encoding='utf-8') as f:
            units_data = json.load(f)
        
        # Check ob es eine Liste oder ein Dictionary ist
        if isinstance(units_data, list):
            units = units_data  # Es ist bereits eine Liste
        elif isinstance(units_data, dict):
            # Falls es ein Dictionary ist, nehme die Values
            units = list(units_data.values())
        else:
            print(f"❌ Unbekanntes Datenformat: {type(units_data)}")
            return {}
        
        # Mapping erstellen: ID -> HUDName
        id_to_name = {}
        for unit in units:
            # Prüfe ob unit ein Dictionary ist
            if not isinstance(unit, dict):
                print(f"⚠️ Skip invalid unit: {unit}")
                continue
                
            unit_id = unit.get('Id')
            hud_name = unit.get('HUDName', unit.get('Name', f"Unknown Unit {unit_id}"))
            
            if unit_id is not None:
                id_to_name[unit_id] = hud_name
        
        print(f"✅ {len(id_to_name)} Units geladen")
        
        # Zeige ein paar Beispiele
        print("📋 Beispiel Units:")
        for i, (unit_id, name) in enumerate(list(id_to_name.items())[:5]):
            print(f"   ID {unit_id:3d} → {name}")
        
        return id_to_name
        
    except FileNotFoundError:
        print(f"❌ Datei nicht gefunden: {database_json_file}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ JSON Fehler: {e}")
        return {}
    except Exception as e:
        print(f"❌ Fehler beim Laden der Database: {e}")
        print(f"   Datentyp: {type(units_data) if 'units_data' in locals() else 'Unknown'}")
        return {}

def analyze_units_with_names(csv_file, database_json_file):
    """
    Analysiert deine Unit-Daten mit echten Namen statt IDs
    """
    print("🚀 Starte Unit-Analyse mit Unit-Namen...")
    
    # Unit-Namen laden
    id_to_name = load_unit_database(database_json_file)
    
    if not id_to_name:
        print("⚠️ Keine Unit-Namen gefunden, verwende IDs...")
        return analyze_units_fallback(csv_file)
    
    # DuckDB connection
    conn = duckdb.connect()
    
    # Query mit Join zu Unit-Namen
    query = f"""
    SELECT 
        unit_id,
        COUNT(*) as count,
        ROUND(SUM(killed_count) * 1.0 / COUNT(*), 4) as efficiency,
        ROUND(AVG(player_rating), 2) as average_rating,
        ROUND(MEDIAN(player_rating), 2) as median_rating,
        SUM(killed_count) as total_kills,
        MIN(player_rating) as min_rating,
        MAX(player_rating) as max_rating
    FROM '{csv_file}'
    GROUP BY unit_id
    ORDER BY count DESC
    """
    
    try:
        # Query ausführen
        result = conn.execute(query).fetchdf()
        
        # Unit-Namen hinzufügen
        result['unit_name'] = result['unit_id'].map(
            lambda x: id_to_name.get(int(x), f"Unknown Unit {int(x)}")
        )
        
        # Spalten umordnen: Name zuerst, dann ID
        result = result[['unit_name', 'unit_id', 'count', 'efficiency', 'average_rating', 'median_rating', 'total_kills', 'min_rating', 'max_rating']]
        
        print(f"✅ Analyse fertig! {len(result)} Units gefunden.")
        print("\n📊 TOP 20 UNITS (mit Namen):")
        print("="*120)
        
        # Schönere Ausgabe mit Namen
        for i, (_, row) in enumerate(result.head(20).iterrows()):
            unit_name = row['unit_name']
            unit_id = int(float(row['unit_id']))
            count = int(float(row['count']))
            efficiency = row['efficiency']
            avg_rating = row['average_rating']
            median_rating = row['median_rating']
            total_kills = int(float(row['total_kills']))
            
            print(f"{i+1:2d}. {unit_name:<25} (ID:{unit_id:3d}) | {count:4d} uses | {efficiency:.3f} eff | {avg_rating:4.0f} avg elo | {total_kills:4d} kills")
        
        # Speichern
        output_file = csv_file.replace('.parquet', '_analysis_with_names.csv')
        result.to_csv(output_file, index=False)
        print(f"\n💾 Vollständige Ergebnisse gespeichert: {output_file}")
        
        return result
        
    except Exception as e:
        print(f"❌ Fehler: {e}")
        return None
    
    finally:
        conn.close()

def analyze_units_fallback(csv_file):
    """
    Fallback ohne Unit-Namen (falls Database nicht geladen werden kann)
    """
    conn = duckdb.connect()
    
    query = f"""
    SELECT 
        unit_id,
        COUNT(*) as count,
        ROUND(SUM(killed_count) * 1.0 / COUNT(*), 4) as efficiency,
        ROUND(AVG(player_rating), 2) as average_rating,
        ROUND(MEDIAN(player_rating), 2) as median_rating,
        SUM(killed_count) as total_kills
    FROM '{csv_file}'
    GROUP BY unit_id
    ORDER BY count DESC
    LIMIT 20
    """
    
    result = conn.execute(query).fetchdf()
    conn.close()
    
    print("📊 TOP 20 UNITS (nur IDs):")
    print(result.to_string(index=False))
    
    return result

# So benutzt du es:
if __name__ == "__main__":
    # Deine Dateien
    parquet_file = "G:/DataMining/battles/broken_arrow/units.parquet"  # ← Dein Dateiname
    database_file = "G:/DataMining/database_unitsonly.json" 
    
    # Hauptanalyse mit Namen
    results = analyze_units_with_names(parquet_file, database_file)
    
    print("\n💡 Jetzt mit Parquet - 10-50x schneller! 🚀")