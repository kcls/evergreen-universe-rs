use evergreen::assert_api_success;
use evergreen::common::service_test::ServiceApiTester;
use evergreen::value::EgValue;
use std::collections::HashMap;

const SERVICE_NAME: &str = "open-ils.rs-addrs";

#[test]
#[ignore] // Requires running addrs service
fn test_home_org_with_generic_tester() {
    let mut tester = ServiceApiTester::new().expect("Failed to create tester");

    // Test coordinates for Seattle area
    let params = vec![
        EgValue::from("placeholder-session-token"),
        EgValue::from(47.54030395464964),
        EgValue::from(-122.05041577546649),
    ];

    let result = tester
        .call_one(SERVICE_NAME, "open-ils.rs-addrs.home-org", params)
        .expect("API call failed");

    assert_api_success!(result);
    println!("Home org result: {:?}", result);

    // Verify the new response structure
    if let Some(resp) = &result {
        assert!(resp.is_object(), "Response should be an object");
        assert!(resp.has_key("home_ou"), "Response should contain home_ou");
        assert!(
            resp.has_key("is_reciprocal"),
            "Response should contain is_reciprocal"
        );
    }
}

#[test]
#[ignore] // Requires running addrs service
fn test_address_lookup_with_generic_tester() {
    let mut tester = ServiceApiTester::new().expect("Failed to create tester");

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
        .call_one(SERVICE_NAME, "open-ils.rs-addrs.lookup", params)
        .expect("API call failed");

    assert_api_success!(result);
    println!("Address lookup result: {:?}", result);
}

#[test]
#[ignore] // Requires running addrs service
fn test_any_token_works() {
    let mut tester = ServiceApiTester::new().expect("Failed to create tester");

    // For now, any token works
    let params = vec![
        EgValue::from("any-token-value"),
        EgValue::from(47.5),
        EgValue::from(-122.0),
    ];

    let result = tester
        .call_one(SERVICE_NAME, "open-ils.rs-addrs.home-org", params)
        .expect("API call failed");

    assert_api_success!(result);

    // Verify the new response structure
    if let Some(resp) = &result {
        assert!(resp.is_object(), "Response should be an object");
        assert!(resp.has_key("home_ou"), "Response should contain home_ou");
        assert!(
            resp.has_key("is_reciprocal"),
            "Response should contain is_reciprocal"
        );
    }
}
