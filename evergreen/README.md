# Evergreen Rust...  Featuring OpenSRF

## API Docs

[https://kcls.github.io/evergreen-universe-rs/evergreen/index.html](https://kcls.github.io/evergreen-universe-rs/evergreen/index.html)

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

    // Create an org unit ("aou") value by hand.
    let mut new_org = eg::blessed! {
        "_classname": "aou",
        "ou_type": 1,
        "shortname": "TEST",
        "name": "TEST NAME",
    }?;

    // Modify a value after instantiation
    new_org["email"] = "home@example.org".into();

    // Start a database transaction so we can modify data.
    editor.xact_begin()?;

    // Create a new org unit
    editor.create(new_org)?;

    // Search for the newly created org unit
    // NOTE: ^-- editor.create() also returns the newly created value.
    let org_list = editor.search("aou", eg::hash! {"shortname": "TEST"})?;

    // If found, log it
    if let Some(org) = org_list.get(0) {
        println!("Add Org: {org} email={}", org["email"]);
    }

    // Rollback the transaction and disconnect
    editor.rollback()
}
```

