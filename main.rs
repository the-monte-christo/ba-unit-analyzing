// Cargo.toml
// [dependencies]
// serde = { version = "1.0", features = ["derive"] }
// serde_json = "1.0"
// rayon = "1.8"
// walkdir = "2.4"

use serde::Deserialize;
use std::collections::HashMap;
use rayon::prelude::*;
use std::io::Write;

// Minimale structs - nur was wir brauchen
#[derive(Deserialize)]
struct Game {
    #[serde(rename = "Data")]
    players: HashMap<String, Player>,
}

#[derive(Deserialize)]
struct Player {
    #[serde(rename = "NewRating")]
    new_rating: Option<f64>,
    #[serde(rename = "UnitData")]
    units: HashMap<String, Unit>,
}

#[derive(Deserialize)]
struct Unit {
    #[serde(rename = "Id")]
    id: i32,
    #[serde(rename = "OptionIds")]
    option_ids: Option<Vec<i32>>,
    #[serde(rename = "KilledCount")]
    killed_count: Option<i32>,
}

// Konfiguration - hier kannst du die Separatoren ändern
const CSV_SEPARATOR: &str = ",";        // Trennt die CSV-Spalten
const OPTION_IDS_SEPARATOR: &str = ";"; // Trennt die Option IDs innerhalb einer Zelle

// Output struct - genau deine 4 Spalten
#[derive(Debug, Clone)]
struct UnitRecord {
    unit_id: i32,
    option_ids: String,    // option-ids getrennt durch OPTION_IDS_SEPARATOR
    killed_count: i32,
    player_rating: f64,
}

fn process_json_file(file_path: &std::path::Path) -> Vec<UnitRecord> {
    let mut records = Vec::new();
    
    // JSON lesen
    let content = match std::fs::read_to_string(file_path) {
        Ok(c) => c,
        Err(_) => return records, // Skip fehlerhafte Dateien
    };
    
    // JSON parsen
    let game: Game = match serde_json::from_str(&content) {
        Ok(g) => g,
        Err(_) => return records, // Skip fehlerhafte JSON
    };
    
    // Durch alle Spieler iterieren
    for (_player_id, player) in game.players {
        // Hat dieser Spieler eine Elo? Wenn nein, skip
        if let Some(player_rating) = player.new_rating {
            
            // Durch alle Units dieses Spielers iterieren
            for (_unit_instance, unit) in player.units {
                
                // Option IDs als configurable-separated string
                let option_ids = match unit.option_ids {
                    Some(ids) => ids.iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(OPTION_IDS_SEPARATOR),  // Verwendet die Konstante oben
                    None => String::new(),
                };
                
                // Record erstellen
                records.push(UnitRecord {
                    unit_id: unit.id,
                    option_ids,
                    killed_count: unit.killed_count.unwrap_or(0),
                    player_rating,
                });
            }
        }
        // Wenn kein NewRating -> skip (macht nichts)
    }
    
    records
}

fn process_all_files(input_dir: &str) -> Result<Vec<UnitRecord>, Box<dyn std::error::Error>> {
    use walkdir::WalkDir;
    
    println!("🔍 Sammle JSON-Dateien...");
    
    // Alle JSON-Dateien finden
    let json_files: Vec<_> = WalkDir::new(input_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
        .collect();
    
    println!("📁 Gefunden: {} JSON-Dateien", json_files.len());
    println!("🚀 Starte parallele Verarbeitung...");
    
    // Parallel processing - alle CPUs nutzen!
    let all_records: Vec<UnitRecord> = json_files
        .par_iter()
        .enumerate()
        .flat_map(|(i, entry)| {
            // Progress alle 1000 Dateien
            if i % 1000 == 0 {
                println!("📊 Verarbeitet: {}/{}", i, json_files.len());
            }
            
            process_json_file(entry.path())
        })
        .collect();
    
    println!("✅ Fertig! {} Unit-Records gesammelt", all_records.len());
    Ok(all_records)
}

fn write_to_csv(records: &[UnitRecord], filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("💾 Schreibe {} Records zu CSV...", records.len());
    println!("📝 CSV-Separator: '{}' | Option-IDs-Separator: '{}'", CSV_SEPARATOR, OPTION_IDS_SEPARATOR);
    
    let mut file = std::fs::File::create(filename)?;
    
    // Header
    writeln!(file, "unit_id{}option_ids{}killed_count{}player_rating", CSV_SEPARATOR, CSV_SEPARATOR, CSV_SEPARATOR)?;
    
    // Rows
    for record in records {
        writeln!(file, "{}{}{}{}{}{}{}", 
            record.unit_id, 
            CSV_SEPARATOR,
            record.option_ids, 
            CSV_SEPARATOR,
            record.killed_count, 
            CSV_SEPARATOR,
            record.player_rating
        )?;
    }
    
    println!("✅ CSV geschrieben: {}", filename);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ========== HIER DEINE PFADE EINTRAGEN ==========
    let input_dir = "G:/DataMining/battles/broken_arrow/fights";    // ← Pfad zu JSON-Dateien
    let output_csv = "units.csv";                  // ← Output CSV
    // ===============================================
    
    println!("🚀 Starte minimalen Unit-Converter...");
    println!("📂 Input:  {}", input_dir);
    println!("📄 Output: {}", output_csv);
    
    // Alle Dateien verarbeiten
    let records = process_all_files(input_dir)?;
    
    if records.is_empty() {
        println!("⚠️  Keine Records gefunden. Überprüfe:");
        println!("   • Pfad korrekt?");
        println!("   • JSON-Dateien vorhanden?");
        println!("   • NewRating in den Daten?");
        return Ok(());
    }
    
    // CSV schreiben
    write_to_csv(&records, output_csv)?;
    
    println!("\n🎉 Fertig! Du kannst jetzt Queries machen:");
    println!("   DuckDB:  SELECT unit_id, COUNT(*), AVG(player_rating) FROM '{}' GROUP BY unit_id ORDER BY COUNT(*) DESC;", output_csv);
    println!("   Python:  df = pd.read_csv('{}'); df.groupby('unit_id').agg({{'player_rating': ['count', 'mean', 'median']}});", output_csv);
    
    Ok(())
}

// Test mit kleinem sample
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    
    #[test]
    fn test_sample_processing() {
        // Erstelle test JSON
        let test_json = r#"{
            "Data": {
                "123": {
                    "NewRating": 1500.0,
                    "UnitData": {
                        "unit1": {
                            "Id": 42,
                            "OptionIds": [1, 2, 3],
                            "KilledCount": 5
                        },
                        "unit2": {
                            "Id": 99,
                            "KilledCount": null
                        }
                    }
                },
                "456": {
                    "UnitData": {
                        "unit3": {
                            "Id": 77,
                            "KilledCount": 10
                        }
                    }
                }
            }
        }"#;
        
        // Temp file erstellen
        let temp_path = std::path::Path::new("test.json");
        let mut file = std::fs::File::create(temp_path).unwrap();
        file.write_all(test_json.as_bytes()).unwrap();
        
        // Verarbeiten
        let records = process_json_file(temp_path);
        
        // Cleanup
        std::fs::remove_file(temp_path).unwrap();
        
        // Tests
        assert_eq!(records.len(), 2); // Nur Spieler 123 hat NewRating
        assert_eq!(records[0].unit_id, 42);
        assert_eq!(records[0].option_ids, "1,2,3");
        assert_eq!(records[0].killed_count, 5);
        assert_eq!(records[0].player_rating, 1500.0);
        
        assert_eq!(records[1].unit_id, 99);
        assert_eq!(records[1].option_ids, "");
        assert_eq!(records[1].killed_count, 0); // null -> 0
        assert_eq!(records[1].player_rating, 1500.0);
        
        println!("✅ Test erfolgreich!");
    }
}