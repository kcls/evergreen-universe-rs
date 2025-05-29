# Service API Testing Guide

This guide explains how to create and run API tests for Evergreen services.

## Test Framework

The `evergreen` crate provides a generic test framework in `evergreen::common::service_test::ServiceApiTester` that simplifies writing synchronous API tests.

## Basic Usage

```rust
use evergreen::common::service_test::ServiceApiTester;
use evergreen::value::EgValue;

// Create tester with authentication
let mut tester = ServiceApiTester::new_with_auth()?;

// Make API call expecting single response
let result = tester.call_one(
    "service-name",
    "method-name", 
    vec![param1, param2],
)?;

// Make API call expecting multiple responses
let results = tester.call(
    "service-name",
    "method-name",
    vec![param1, param2],
)?;
```

## Environment Variables

Set these environment variables to configure test authentication:
- `EG_TEST_USERNAME` (default: "admin")
- `EG_TEST_PASSWORD` (default: "demo123")
- `EG_TEST_WORKSTATION` (optional)

## Running Tests

```bash
# Run all tests for a service (skips ignored tests)
cargo test --package eg-service-addrs

# Run ignored tests (requires running services)
cargo test --package eg-service-addrs -- --ignored

# Run specific test
cargo test --package eg-service-addrs test_home_org -- --ignored

# Run with output
cargo test --package eg-service-addrs -- --ignored --nocapture
```

## Writing Service Tests

### 1. Add test configuration to Cargo.toml:
```toml
[[test]]
name = "api"
path = "tests/api.rs"
```

### 2. Create test file structure:
```
service-name/
  tests/
    mod.rs      # mod api;
    api.rs      # Your API tests
```

### 3. Write tests using the framework:
```rust
use evergreen::common::service_test::ServiceApiTester;
use evergreen::value::EgValue;
use evergreen::{assert_api_success, assert_api_error};

const SERVICE_NAME: &str = "open-ils.rs-service";

#[test]
#[ignore] // Requires running service
fn test_api_method() {
    let mut tester = ServiceApiTester::new_with_auth()
        .expect("Failed to create tester");
    
    // Add auth token automatically
    let params = tester.params_with_auth(vec![
        EgValue::from("param1"),
        EgValue::from(123),
    ]).expect("Failed to build params");
    
    let result = tester
        .call_one(SERVICE_NAME, "method.name", params)
        .expect("API call failed");
    
    assert_api_success!(result);
}
```

## Test Patterns

### Test successful response:
```rust
assert_api_success!(result);
```

### Test error response:
```rust
assert_api_error!(result);
```

### Test with custom timeout:
```rust
use std::time::Duration;

tester.set_timeout(Duration::from_secs(30));
// or
let result = tester.call_one_with_timeout(
    SERVICE_NAME, 
    "method", 
    params,
    Some(Duration::from_secs(5))
)?;
```

### Test concurrent requests:
```rust
use std::thread;

let handles: Vec<_> = (0..10)
    .map(|_| {
        thread::spawn(|| {
            let mut tester = ServiceApiTester::new_with_auth()?;
            // Make API calls
        })
    })
    .collect();

for handle in handles {
    handle.join().expect("Thread failed");
}
```

## Example: Testing the addrs service

See `evergreen-services/addrs/tests/api.rs` for a complete example of testing:
- `home-org` - Find org unit by coordinates
- `lookup` - Verify address details
- `autocomplete` - Get address suggestions
- Error handling tests
- Concurrent request tests