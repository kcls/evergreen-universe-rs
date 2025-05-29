use evergreen::init;
use evergreen::osrf::client::Client;
use evergreen::result::EgResult;
use evergreen::value::EgValue;
use std::collections::HashMap;

// Test configuration
const SERVICE_NAME: &str = "open-ils.rs-addrs";

struct AddrsApiTester {
    client: Client,
}

impl AddrsApiTester {
    fn new() -> EgResult<Self> {
        // Initialize Evergreen
        let _ = init::init();
        
        // Create sync client
        let client = Client::connect()?;
        
        Ok(Self { client })
    }
    
    fn call_api(
        &mut self,
        method: &str,
        params: Vec<EgValue>,
    ) -> EgResult<Vec<EgValue>> {
        let mut results = Vec::new();
        for resp in self.client.send_recv_iter(SERVICE_NAME, method, params)? {
            results.push(resp?);
        }
        Ok(results)
    }
    
    fn call_api_one(
        &mut self,
        method: &str,
        params: Vec<EgValue>,
    ) -> EgResult<Option<EgValue>> {
        self.client.send_recv_one(SERVICE_NAME, method, params)
    }
}

#[test]
#[ignore] // Requires running addrs service
fn test_home_org_lookup() {
    let mut tester = AddrsApiTester::new().expect("Failed to create tester");
    
    // Test coordinates for Seattle area
    let lat = 47.54030395464964;
    let lon = -122.05041577546649;
    
    let params = vec![
        EgValue::from("placeholder-session-token"),
        EgValue::from(lat),
        EgValue::from(lon),
    ];
    
    let result = tester
        .call_api_one("open-ils.rs-addrs.home-org", params)
        .expect("API call failed");
    
    assert!(result.is_some(), "Should receive a response");
    
    let response = result.unwrap();
    println!("Home org response: {:?}", response);
    
    // Verify response has expected structure
    assert!(response.is_object(), "Response should be an object");
    assert!(response["home_ou"].is_number() || response["home_ou"].is_null(), 
        "home_ou should be a number or null");
    assert!(response["is_reciprocal"].is_boolean(), "is_reciprocal should be a boolean");
}

#[test]
#[ignore] // Requires running addrs service
fn test_address_lookup() {
    let mut tester = AddrsApiTester::new().expect("Failed to create tester");
    
    // Create search object for address lookup
    let mut search = HashMap::new();
    search.insert("street".to_string(), EgValue::from("1 Microsoft Way"));
    search.insert("city".to_string(), EgValue::from("Redmond"));
    search.insert("state".to_string(), EgValue::from("WA"));
    search.insert("zipcode".to_string(), EgValue::from("98052"));
    
    let params = vec![
        EgValue::from("placeholder-session-token"),
        EgValue::Hash(search),
    ];
    
    let result = tester
        .call_api_one("open-ils.rs-addrs.lookup", params)
        .expect("API call failed");
    
    assert!(result.is_some(), "Should receive a response");
    
    let response = result.unwrap();
    println!("Address lookup response: {:?}", response);
    
    // Verify response structure
    if response.is_object() {
        // Check for common address fields
        assert!(
            !response["metadata"]["latitude"].is_null() || !response["error"].is_null(),
            "Response should contain latitude or error"
        );
    }
}

#[test]
#[ignore] // Requires running addrs service
fn test_address_autocomplete() {
    let mut tester = AddrsApiTester::new().expect("Failed to create tester");
    
    // Create search object for autocomplete
    let mut search = HashMap::new();
    search.insert("search".to_string(), EgValue::from("1 Microsoft Way Redmond"));
    search.insert("state_filter".to_string(), EgValue::from("WA"));
    
    let params = vec![
        EgValue::from("placeholder-session-token"),
        EgValue::Hash(search),
    ];
    
    let results = tester
        .call_api("open-ils.rs-addrs.autocomplete", params)
        .expect("API call failed");
    
    println!("Autocomplete results: {:?}", results);
    
    // Autocomplete might return empty results if no matches found
    // This is valid behavior, not an error
    if !results.is_empty() {
        // Verify first result structure if we got results
        if let Some(first) = results.first() {
            if first.is_object() {
                assert!(
                    !first["street_line"].is_null() || !first["error"].is_null(),
                    "Result should contain street_line or error"
                );
            }
        }
    } else {
        println!("No autocomplete suggestions found (this is valid behavior)");
    }
}

#[test]
#[ignore] // Requires running addrs service
fn test_invalid_session_token() {
    let mut tester = AddrsApiTester::new().expect("Failed to create tester");
    
    // For now, any token works, so this test just verifies the API responds
    let params = vec![
        EgValue::from("any-token-works"),
        EgValue::from(47.5),
        EgValue::from(-122.0),
    ];
    
    let result = tester.call_api_one("open-ils.rs-addrs.home-org", params);
    
    // Should succeed since token validation is not implemented
    assert!(result.is_ok(), "API call should succeed");
    
    if let Ok(Some(response)) = result {
        println!("Response with any token: {:?}", response);
        assert!(response.is_object(), "Response should be an object");
        assert!(response.has_key("home_ou"), "Response should contain home_ou");
        assert!(response.has_key("is_reciprocal"), "Response should contain is_reciprocal");
    }
}

#[test]
#[ignore] // Requires running addrs service
fn test_missing_parameters() {
    let mut tester = AddrsApiTester::new().expect("Failed to create tester");
    
    // Call with missing longitude
    let params = vec![
        EgValue::from("placeholder-session-token"),
        EgValue::from(47.5),
    ];
    
    let result = tester.call_api_one("open-ils.rs-addrs.home-org", params);
    
    // Should fail due to missing parameter
    assert!(
        result.is_err() || result.as_ref().unwrap().is_none(),
        "Should fail with missing parameter"
    );
}

#[test]
#[ignore] // Requires running addrs service
fn test_concurrent_requests() {
    use std::thread;
    
    let handles: Vec<_> = (0..5)
        .map(|i| {
            thread::spawn(move || {
                let mut tester = AddrsApiTester::new().expect("Failed to create tester");
                
                let params = vec![
                    EgValue::from("placeholder-session-token"),
                    EgValue::from(47.5 + (i as f64) * 0.01),
                    EgValue::from(-122.0),
                ];
                
                tester
                    .call_api_one("open-ils.rs-addrs.home-org", params)
                    .expect("API call failed")
            })
        })
        .collect();
    
    // Wait for all threads to complete
    for handle in handles {
        let result = handle.join().expect("Thread panicked");
        assert!(result.is_some(), "Each request should succeed");
    }
}