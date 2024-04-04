# Evergreen Rust...  Featuring OpenSRF

## There's an Editor

```rs
use evergreen as eg;                                                           
                                                                               
fn main() -> eg::EgResult<()> {                                                
    let ctx = eg::init()?;                                                     
    let mut editor = eg::Editor::new(ctx.client());                            
                                                                               
    let orgs = editor.search("aou", eg::hash! {"id": {">": 0}})?;              
                                                                               
    for org in orgs {                                                          
       println!("Org: {org}");                                                 
    }                                                                          
                                                                               
    let mut new_org = eg::hash! {                                              
        "ou_type": 1,                                                          
        "shortname": "TEST",                                                   
        "name": "TEST NAME",                                                   
    };                                                                         
                                                                               
	// Turn a bare hash into a blessed org unit ("aou") value.
    new_org.bless("aou")?;                                                     
                                                                               
    // Modify a value after instantiation                                      
    new_org["email"] = "home@example.org".into();                              
                                                                               
    // Start a database transaction so we can modify data.                     
    editor.xact_begin()?;                                                      
                                                                               
    // Editor::create() returns the newly created value.                         
    new_org = editor.create(new_org)?;                                         
                                                                               
    println!("Add Org: {new_org} email={}", new_org["email"]);                 
                                                                               
    // Rollback the transaction and disconnect                                 
    editor.rollback()
}
```






