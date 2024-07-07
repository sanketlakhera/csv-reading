use csv::ReaderBuilder;
use std::error::Error;
use std::fs;
use std::path::Path;

fn main_with_dir(dir_path: &str) -> Result<(), Box<dyn Error>> {
    // let dir_path = "path/to/your/directory"; // Replace with your actual directory path

    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("csv") {
            let file_name = path.file_name().unwrap().to_str().unwrap();
            let symbol = get_symbol_from_filename(file_name);
            println!("Processing file: {}, Symbol: {}", file_name, symbol);

            process_csv_file(&path, &symbol)?;
        }
    }

    Ok(())
}

fn get_symbol_from_filename(filename: &str) -> String {
    Path::new(filename)
        .file_stem()
        .and_then(|os_str| os_str.to_str())
        .unwrap_or("")
        .trim_start_matches(".")
        .to_string()
}

fn process_csv_file(file_path: &Path, symbol: &str) -> Result<(), Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;

    for result in rdr.deserialize() {
        let record: StockPrice = result?;
        println!(
            "Symbol: {}, Date: {}, High: {}",
            symbol, record.date, record.high
        );
    }

    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct StockPrice {
    date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_get_symbol_from_filename() {
        assert_eq!(get_symbol_from_filename("CAMPUS.csv"), "CAMPUS");
        assert_eq!(get_symbol_from_filename("3MINDIA.csv"), "3MINDIA");
        assert_eq!(
            get_symbol_from_filename("SYMBOL_WITH_UNDERSCORE.csv"),
            "SYMBOL_WITH_UNDERSCORE"
        );
        assert_eq!(get_symbol_from_filename(".hidden_file.csv"), "hidden_file");
        assert_eq!(get_symbol_from_filename("no_extension"), "no_extension");
    }

    #[test]
    fn test_process_csv_file() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;
        let file_path = dir.path().join("TEST.csv");
        let mut file = File::create(&file_path)?;

        writeln!(file, "date,open,high,low,close,volume")?;
        writeln!(file, "2023-07-05,100.0,105.0,98.0,102.0,1000")?;
        writeln!(file, "2023-07-06,102.0,107.0,101.0,106.0,1200")?;

        // Redirect stdout to capture printed output
        let mut output = Vec::new();
        {
            use std::io::{self, Write};
            let mut stdout = io::Cursor::new(&mut output);

            // Process the CSV file
            process_csv_file(&file_path, "TEST")?;

            stdout.flush()?;
        }

        // Convert captured output to string
        let output = String::from_utf8(output)?;

        // Check if the output contains expected data
        assert!(output.contains("Symbol: TEST, Date: 2023-07-05, High: 105"));
        assert!(output.contains("Symbol: TEST, Date: 2023-07-06, High: 107"));

        Ok(())
    }

    #[test]
    fn test_main_function() -> Result<(), Box<dyn Error>> {
        let dir = tempdir()?;

        // Create test CSV files
        create_test_csv(&dir, "TEST1.csv")?;
        create_test_csv(&dir, "TEST2.csv")?;

        // Redirect stdout to capture printed output
        let mut output = Vec::new();
        {
            use std::io::{self, Write};
            let mut stdout = io::Cursor::new(&mut output);

            // Call main function with test directory
            main_with_dir(dir.path().to_str().unwrap())?;

            stdout.flush()?;
        }

        // Convert captured output to string
        let output = String::from_utf8(output)?;

        // Check if the output contains expected data
        assert!(output.contains("Processing file: TEST1.csv, Symbol: TEST1"));
        assert!(output.contains("Processing file: TEST2.csv, Symbol: TEST2"));
        assert!(output.contains("Symbol: TEST1, Date: 2023-07-05, High: 105"));
        assert!(output.contains("Symbol: TEST2, Date: 2023-07-06, High: 107"));

        Ok(())
    }

    fn create_test_csv(dir: &tempfile::TempDir, filename: &str) -> Result<(), Box<dyn Error>> {
        let file_path = dir.path().join(filename);
        let mut file = File::create(&file_path)?;

        writeln!(file, "date,open,high,low,close,volume")?;
        writeln!(file, "2023-07-05,100.0,105.0,98.0,102.0,1000")?;
        writeln!(file, "2023-07-06,102.0,107.0,101.0,106.0,1200")?;

        Ok(())
    }
}

// // You'll need to modify your main function to accept a directory path:
// fn main_with_dir(dir_path: &str) -> Result<(), Box<dyn Error>> {
//     // ... (same as your main function, but using dir_path instead of hardcoded path)
//     Ok(())
// }

// Your original main function can then call this:
fn main() -> Result<(), Box<dyn Error>> {
    main_with_dir("csv")
}
