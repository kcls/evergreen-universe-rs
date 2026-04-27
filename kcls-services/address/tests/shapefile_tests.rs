use kcls_service_address::shapefile_util::shapefile_contains;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;

const TEST_POINTS_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_points.json");

fn default_data_dir() -> String {
    format!("{}/data/shapefiles", env!("CARGO_MANIFEST_DIR"))
}

fn data_dir() -> String {
    std::env::var("SHAPEFILE_DATA_DIR").unwrap_or_else(|_| default_data_dir())
}

fn load_test_points() -> Vec<Value> {
    let content = std::fs::read_to_string(TEST_POINTS_FILE)
        .unwrap_or_else(|e| panic!("Cannot read {TEST_POINTS_FILE}: {e}"));
    let parsed: Value = serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("Cannot parse {TEST_POINTS_FILE}: {e}"));
    parsed["points"]
        .as_array()
        .expect("test_points.json must have a 'points' array")
        .clone()
}

fn discover_shapefiles(subdir: &str) -> Vec<(String, String)> {
    let dir = format!("{}/{}", data_dir(), subdir);
    let base = Path::new(&dir);

    if !base.exists() {
        return Vec::new();
    }

    let mut results = Vec::new();

    if subdir == "home-orgs" {
        if let Ok(entries) = std::fs::read_dir(base) {
            for entry in entries.flatten() {
                let code = entry.file_name().to_string_lossy().to_string();
                let shp = format!("{}/{code}/{code}.shp", dir);
                if Path::new(&shp).exists() {
                    results.push((code, shp));
                }
            }
        }
    } else {
        if let Ok(entries) = std::fs::read_dir(base) {
            for entry in entries.flatten() {
                let fname = entry.file_name().to_string_lossy().to_string();
                if fname.ends_with(".shp") {
                    let code = fname.trim_end_matches(".shp").to_string();
                    let shp = format!("{dir}/{fname}");
                    results.push((code, shp));
                }
            }
        }
    }

    results.sort_by(|a, b| a.0.cmp(&b.0));
    results
}

/// Run with: cargo test -p kcls-service-address report -- --ignored --nocapture
#[test]
#[ignore]
fn report_shapefile_containment() {
    let points = load_test_points();
    let home_orgs = discover_shapefiles("home-orgs");
    let districts = discover_shapefiles("districts");

    println!();
    println!("=== Shapefile Containment Report ===");
    println!("Data dir: {}", data_dir());
    println!("Home-org shapefiles: {}", home_orgs.len());
    println!("District shapefiles: {}", districts.len());
    println!();

    for point in &points {
        let name = point["name"].as_str().unwrap_or("unnamed");
        let lat = point["lat"].as_f64().expect("lat must be a number");
        let long = point["long"].as_f64().expect("long must be a number");
        let desc = point["description"].as_str().unwrap_or("");

        println!("--- {name} ({lat}, {long}) ---");
        if !desc.is_empty() {
            println!("    {desc}");
        }

        let mut found_any = false;

        let mut matched_home_orgs: Vec<&str> = Vec::new();
        for (code, shp) in &home_orgs {
            match shapefile_contains(shp, lat, long) {
                Ok(true) => matched_home_orgs.push(code),
                Ok(false) => {}
                Err(e) => eprintln!("    ERROR reading {code}: {e}"),
            }
        }
        if !matched_home_orgs.is_empty() {
            found_any = true;
            println!("    Home orgs: {}", matched_home_orgs.join(", "));
        }

        let mut matched_districts: Vec<&str> = Vec::new();
        for (code, shp) in &districts {
            match shapefile_contains(shp, lat, long) {
                Ok(true) => matched_districts.push(code),
                Ok(false) => {}
                Err(e) => eprintln!("    ERROR reading {code}: {e}"),
            }
        }
        if !matched_districts.is_empty() {
            found_any = true;
            println!("    Districts: {}", matched_districts.join(", "));
        }

        if !found_any {
            println!("    (no matches)");
        }

        println!();
    }
}

#[test]
fn shapefile_contains_rejects_nonexistent_file() {
    let result = shapefile_contains("/no/such/file.shp", 47.6, -122.3);
    assert_eq!(result.unwrap(), false);
}

#[test]
fn shapefile_contains_coordinate_sanity() {
    let home_orgs = discover_shapefiles("home-orgs");
    if home_orgs.is_empty() {
        eprintln!("No home-org shapefiles found at {}; skipping", data_dir());
        return;
    }

    // A point in the Pacific Ocean should not be in any home-org shapefile
    for (code, shp) in &home_orgs {
        let result = shapefile_contains(shp, 47.0, -130.0)
            .unwrap_or_else(|e| panic!("Error reading {code}: {e}"));
        assert!(
            !result,
            "Pacific Ocean point (47.0, -130.0) should not be inside home-org {code}"
        );
    }
}

#[test]
fn district_shapefiles_reject_ocean() {
    let districts = discover_shapefiles("districts");
    if districts.is_empty() {
        eprintln!("No district shapefiles found at {}; skipping", data_dir());
        return;
    }

    for (code, shp) in &districts {
        let result = shapefile_contains(shp, 47.0, -130.0)
            .unwrap_or_else(|e| panic!("Error reading {code}: {e}"));
        assert!(
            !result,
            "Pacific Ocean point (47.0, -130.0) should not be inside district {code}"
        );
    }
}

#[test]
fn all_shapefiles_are_readable() {
    let home_orgs = discover_shapefiles("home-orgs");
    let districts = discover_shapefiles("districts");

    // Use a point that's roughly central to King County
    let lat = 47.5;
    let long = -122.2;

    for (code, shp) in home_orgs.iter().chain(districts.iter()) {
        let result = shapefile_contains(&shp, lat, long);
        assert!(
            result.is_ok(),
            "Shapefile {code} ({shp}) should be readable without error: {:?}",
            result.err()
        );
    }
}

fn expected_set(point: &Value, key: &str) -> Option<BTreeSet<String>> {
    point.get(key).and_then(|v| {
        v.as_array().map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
    })
}

fn match_shapefiles(shapefiles: &[(String, String)], lat: f64, long: f64) -> BTreeSet<String> {
    let mut matched = BTreeSet::new();
    for (code, shp) in shapefiles {
        match shapefile_contains(shp, lat, long) {
            Ok(true) => { matched.insert(code.clone()); }
            Ok(false) => {}
            Err(e) => panic!("Error reading shapefile {code}: {e}"),
        }
    }
    matched
}

#[test]
fn assert_expected_containment() {
    let points = load_test_points();
    let home_orgs = discover_shapefiles("home-orgs");
    let districts = discover_shapefiles("districts");
    let mut tested = 0;
    let mut failures: Vec<String> = Vec::new();

    for point in &points {
        let name = point["name"].as_str().unwrap_or("unnamed");
        let lat = point["lat"].as_f64().expect("lat must be a number");
        let long = point["long"].as_f64().expect("long must be a number");

        if let Some(expected) = expected_set(point, "expected_home_orgs") {
            let actual = match_shapefiles(&home_orgs, lat, long);
            if actual != expected {
                failures.push(format!(
                    "{name}: home_orgs expected {expected:?} but got {actual:?}"
                ));
            }
            tested += 1;
        }

        if let Some(expected) = expected_set(point, "expected_districts") {
            let actual = match_shapefiles(&districts, lat, long);
            if actual != expected {
                failures.push(format!(
                    "{name}: districts expected {expected:?} but got {actual:?}"
                ));
            }
            tested += 1;
        }
    }

    assert!(tested > 0, "No points had expected_home_orgs or expected_districts");

    if !failures.is_empty() {
        panic!(
            "{} assertion(s) failed:\n  {}",
            failures.len(),
            failures.join("\n  ")
        );
    }
}

#[test]
fn test_points_json_is_valid() {
    let points = load_test_points();
    assert!(!points.is_empty(), "test_points.json must contain at least one point");

    for (i, point) in points.iter().enumerate() {
        assert!(
            point["name"].is_string(),
            "Point {i} is missing 'name'"
        );
        let name = point["name"].as_str().unwrap();

        assert!(
            point["lat"].is_f64() || point["lat"].is_i64(),
            "Point '{name}' (index {i}) has invalid 'lat'"
        );
        assert!(
            point["long"].is_f64() || point["long"].is_i64(),
            "Point '{name}' (index {i}) has invalid 'long'"
        );

        let lat = point["lat"].as_f64().unwrap();
        let long = point["long"].as_f64().unwrap();

        assert!(
            (-90.0..=90.0).contains(&lat),
            "Point '{name}' lat {lat} out of range [-90, 90]"
        );
        assert!(
            (-180.0..=180.0).contains(&long),
            "Point '{name}' long {long} out of range [-180, 180]"
        );
    }
}
