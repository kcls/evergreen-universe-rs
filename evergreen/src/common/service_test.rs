use crate::init;
use crate::osrf::client::Client;
use crate::result::EgResult;
use crate::value::EgValue;
use std::collections::HashMap;

/// Generic test framework for OpenSRF service API testing
pub struct ServiceApiTester {
    client: Client,
    authtoken: Option<String>,
}

impl ServiceApiTester {
    /// Create a new tester with authentication
    pub fn new_with_auth() -> EgResult<Self> {
        let mut tester = Self::new()?;
        tester.authenticate()?;
        Ok(tester)
    }
    
    /// Create a new tester without authentication
    pub fn new() -> EgResult<Self> {
        let _ = init::init(); // Init may already be done
        
        let client = Client::connect()?;
        
        Ok(Self {
            client,
            authtoken: None,
        })
    }
    
    /// Authenticate and store auth token
    pub fn authenticate(&mut self) -> EgResult<()> {
        let username = std::env::var("EG_TEST_USERNAME").unwrap_or("admin".to_string());
        let password = std::env::var("EG_TEST_PASSWORD").unwrap_or("demo123".to_string());
        let workstation = std::env::var("EG_TEST_WORKSTATION").ok();
        
        let mut login_params = vec![
            EgValue::from(username),
            EgValue::from(password),
        ];
        
        if let Some(ws) = workstation {
            login_params.push(EgValue::from(ws));
        }
        
        let auth_resp = self.client
            .send_recv_one(
                "open-ils.auth",
                "open-ils.auth.login",
                login_params,
            )?
            .ok_or("No response from auth service")?;
        
        let authtoken = auth_resp["payload"]["authtoken"]
            .as_str()
            .ok_or("Failed to get authtoken")?
            .to_string();
        
        self.authtoken = Some(authtoken);
        Ok(())
    }
    
    /// Get the auth token
    pub fn authtoken(&self) -> Option<&str> {
        self.authtoken.as_deref()
    }
    
    /// Make an API call expecting multiple responses
    pub fn call(
        &mut self,
        service: &str,
        method: &str,
        params: Vec<EgValue>,
    ) -> EgResult<Vec<EgValue>> {
        let mut results = Vec::new();
        for resp in self.client.send_recv_iter(service, method, params)? {
            results.push(resp?);
        }
        Ok(results)
    }
    
    /// Make an API call expecting a single response
    pub fn call_one(
        &mut self,
        service: &str,
        method: &str,
        params: Vec<EgValue>,
    ) -> EgResult<Option<EgValue>> {
        self.client.send_recv_one(service, method, params)
    }
    
    /// Helper to build params with auth token prepended
    pub fn params_with_auth(&self, mut params: Vec<EgValue>) -> EgResult<Vec<EgValue>> {
        match &self.authtoken {
            Some(token) => {
                let mut new_params = vec![EgValue::from(token.as_str())];
                new_params.append(&mut params);
                Ok(new_params)
            }
            None => Err("No auth token available".into()),
        }
    }
    
    /// Helper to create a HashMap for API parameters
    pub fn create_param_map() -> HashMap<&'static str, EgValue> {
        HashMap::new()
    }
}

/// Helper macro to assert API response is successful
#[macro_export]
macro_rules! assert_api_success {
    ($response:expr) => {
        match &$response {
            Some(resp) => {
                if let Some(ilsevent) = resp["ilsevent"].as_i64() {
                    panic!("API returned error event: {}", ilsevent);
                }
            }
            None => panic!("Expected response but got None"),
        }
    };
}

/// Helper macro to assert API response contains error
#[macro_export]
macro_rules! assert_api_error {
    ($response:expr) => {
        match &$response {
            Some(resp) => {
                assert!(
                    resp["ilsevent"].is_number() || resp["error"].is_string(),
                    "Expected error response"
                );
            }
            None => {} // None is also acceptable as error
        }
    };
}