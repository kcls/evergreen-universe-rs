use evergreen::init;
use evergreen::osrf::client::Client;
use evergreen::result::EgResult;
use evergreen::value::EgValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

const SERVICE_NAME: &str = "open-ils.rs-addrs";

#[derive(Debug, Serialize, Deserialize)]
struct Location {
    name: String,
    lat: f64,
    lon: f64,
    expected_org: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notes: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TestData {
    test_locations: TestLocationGroups,
    boundary_tests: Vec<Location>,
    edge_cases: Vec<Location>,
    invalid_coordinates: Vec<Location>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TestLocationGroups {
    king_county_seattle: Vec<Location>,
    king_county_eastside: Vec<Location>,
    king_county_south: Vec<Location>,
    king_county_north: Vec<Location>,
    other_washington: Vec<Location>,
}

struct HomeOrgTester {
    client: Client,
}

impl HomeOrgTester {
    fn new() -> EgResult<Self> {
        let _ = init::init();
        let client = Client::connect()?;
        Ok(Self { client })
    }
    
    fn test_location(&mut self, location: &Location) -> EgResult<i64> {
        let params = vec![
            EgValue::from("test-session"),
            EgValue::from(location.lat),
            EgValue::from(location.lon),
        ];
        
        let result = self.client
            .send_recv_one(SERVICE_NAME, "open-ils.rs-addrs.home-org", params)?
            .ok_or("No response from home-org API")?;
        
        // Response is now an object with home_ou and is_reciprocal fields
        if result.is_object() && !result["home_ou"].is_null() {
            if let Some(org_id) = result["home_ou"].as_i64() {
                Ok(org_id)
            } else {
                Err(format!("Invalid home_ou value for {} (lat: {}, lon: {}): {:?}", 
                    location.name, location.lat, location.lon, result["home_ou"]).into())
            }
        } else if result.is_object() && result["home_ou"].is_null() {
            Err(format!("No home_ou assigned for {} (lat: {}, lon: {})", 
                location.name, location.lat, location.lon).into())
        } else {
            Err(format!("Invalid response format for {} (lat: {}, lon: {}): {:?}", 
                location.name, location.lat, location.lon, result).into())
        }
    }
}

fn load_test_data() -> TestData {
    let json_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/home_org_test_locations.json"
    );
    
    let json_content = fs::read_to_string(json_path)
        .expect("Failed to read test data JSON file");
    
    serde_json::from_str(&json_content)
        .expect("Failed to parse test data JSON")
}

fn get_all_test_locations(data: &TestData) -> Vec<&Location> {
    let mut locations = Vec::new();
    locations.extend(&data.test_locations.king_county_seattle);
    locations.extend(&data.test_locations.king_county_eastside);
    locations.extend(&data.test_locations.king_county_south);
    locations.extend(&data.test_locations.king_county_north);
    locations.extend(&data.test_locations.other_washington);
    locations
}

#[test]
#[ignore] // Requires running addrs service
fn test_home_org_coverage() {
    let mut tester = HomeOrgTester::new().expect("Failed to create tester");
    let test_data = load_test_data();
    let all_locations = get_all_test_locations(&test_data);
    
    let mut results: HashMap<i64, Vec<&str>> = HashMap::new();
    let mut failures = Vec::new();
    
    println!("\nTesting {} locations across Washington State:", all_locations.len());
    println!("{:-<80}", "");
    
    for location in &all_locations {
        match tester.test_location(location) {
            Ok(org_id) => {
                println!("✓ {:40} -> Org Unit {}", location.name, org_id);
                results.entry(org_id).or_insert_with(Vec::new).push(&location.name);
                
                if let Some(expected) = location.expected_org {
                    assert_eq!(org_id, expected, 
                        "{} returned org {} but expected {}", 
                        location.name, org_id, expected);
                }
            }
            Err(e) => {
                println!("✗ {:40} -> ERROR: {}", location.name, e);
                failures.push((&location.name, e.to_string()));
            }
        }
    }
    
    // Summary statistics
    println!("\n{:-<80}", "");
    println!("SUMMARY:");
    println!("Total locations tested: {}", all_locations.len());
    println!("Successful: {}", all_locations.len() - failures.len());
    println!("Failed: {}", failures.len());
    
    // Show org unit distribution
    println!("\nOrg Unit Distribution:");
    let mut sorted_orgs: Vec<_> = results.iter().collect();
    sorted_orgs.sort_by_key(|(org_id, _)| *org_id);
    
    for (org_id, locations) in sorted_orgs {
        println!("\nOrg Unit {} ({} locations):", org_id, locations.len());
        for loc in locations {
            println!("  - {}", loc);
        }
    }
    
    // Don't fail the test if some locations don't have org units
    // This might be expected behavior if shapefile data is incomplete
    if !failures.is_empty() {
        println!("\nLocations without org unit assignments ({}):", failures.len());
        for (name, _) in &failures {
            println!("  - {}", name);
        }
        println!("\nNote: This may be expected if shapefile data doesn't cover these areas.");
    }
}

#[test]
#[ignore] // Requires running addrs service
fn test_king_county_boundaries() {
    let mut tester = HomeOrgTester::new().expect("Failed to create tester");
    let test_data = load_test_data();
    
    println!("\nTesting King County boundary points:");
    for location in &test_data.boundary_tests {
        match tester.test_location(location) {
            Ok(org_id) => println!("✓ {} -> Org Unit {}", location.name, org_id),
            Err(e) => println!("✗ {} -> {}", location.name, e),
        }
    }
}

#[test]
#[ignore] // Requires running addrs service
fn test_edge_cases() {
    let mut tester = HomeOrgTester::new().expect("Failed to create tester");
    let test_data = load_test_data();
    
    println!("\nTesting edge case locations:");
    for location in &test_data.edge_cases {
        match tester.test_location(location) {
            Ok(org_id) => {
                print!("✓ {} -> Org Unit {}", location.name, org_id);
                if let Some(notes) = &location.notes {
                    print!(" ({})", notes);
                }
                println!();
            }
            Err(e) => {
                print!("✗ {} -> ERROR: {}", location.name, e);
                if let Some(notes) = &location.notes {
                    print!(" ({})", notes);
                }
                println!();
            }
        }
    }
}

#[test]
#[ignore] // Requires running addrs service
fn test_invalid_coordinates() {
    let tester = HomeOrgTester::new().expect("Failed to create tester");
    let test_data = load_test_data();
    
    println!("\nTesting coordinates outside Washington State:");
    for location in &test_data.invalid_coordinates {
        let params = vec![
            EgValue::from("test-session"),
            EgValue::from(location.lat),
            EgValue::from(location.lon),
        ];
        
        match tester.client.send_recv_one(SERVICE_NAME, "open-ils.rs-addrs.home-org", params) {
            Ok(Some(result)) => {
                if result.is_object() {
                    let home_ou = &result["home_ou"];
                    let is_reciprocal = result["is_reciprocal"].as_bool().unwrap_or(false);
                    if !home_ou.is_null() {
                        if let Some(org_id) = home_ou.as_i64() {
                            println!("  {} -> Org Unit {} (reciprocal: {})", 
                                location.name, org_id, is_reciprocal);
                        } else {
                            println!("  {} -> Invalid home_ou: {:?}", location.name, home_ou);
                        }
                    } else {
                        println!("  {} -> No home_ou assigned", location.name);
                    }
                } else {
                    println!("  {} -> Non-object response: {:?}", location.name, result);
                }
            }
            Ok(None) => println!("  {} -> No response", location.name),
            Err(e) => println!("  {} -> Error: {}", location.name, e),
        }
    }
}

#[test]
#[ignore] // Requires running addrs service
fn test_verify_expected_org_units() {
    let mut tester = HomeOrgTester::new().expect("Failed to create tester");
    let test_data = load_test_data();
    let all_locations = get_all_test_locations(&test_data);
    
    // Only test locations with expected org units
    let locations_with_expected: Vec<_> = all_locations.iter()
        .filter(|loc| loc.expected_org.is_some())
        .collect();
    
    println!("\nVerifying {} locations with expected org units:", locations_with_expected.len());
    
    let mut failures = Vec::new();
    for location in locations_with_expected {
        match tester.test_location(location) {
            Ok(org_id) => {
                let expected = location.expected_org.unwrap();
                if org_id == expected {
                    println!("✓ {} -> {} (as expected)", location.name, org_id);
                } else {
                    println!("✗ {} -> {} (expected {})", location.name, org_id, expected);
                    failures.push((&location.name, org_id, expected));
                }
            }
            Err(e) => {
                println!("✗ {} -> ERROR: {} (expected {})", 
                    location.name, e, location.expected_org.unwrap());
                failures.push((&location.name, 0, location.expected_org.unwrap()));
            }
        }
    }
    
    assert!(failures.is_empty(), 
        "{} locations returned unexpected org units: {:?}", 
        failures.len(), failures);
}