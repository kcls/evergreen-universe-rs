use eg::common::bib;
use eg::common::holds;
use eg::editor::Editor;
use eg::result::EgResult;
use evergreen as eg;

fn main() -> EgResult<()> {
    let client = eg::init()?;
    let mut editor = Editor::new(&client);

    let bib_ids = &[1, 2, 3];

    let display_attrs = bib::get_display_attrs(&mut editor, bib_ids)?;

    for (bib_id, attr_set) in display_attrs {
        println!("TITLE: {}", attr_set.first_value("title"));

        for attr in attr_set.attrs() {
            println!(
                "Bib {bib_id} [{}] ({}) => {}",
                attr.name(),
                attr.label(),
                attr.value().first()
            );
        }
    }

    let related = holds::related_to_copy(&mut editor, 3000, Some(4), None, None, None).unwrap();
    for hold in related {
        println!("related hold: {hold}");
    }

    let mvr = bib::map_to_mvr(&mut editor, 5)?;

    println!("MVR\n{}", mvr.dump());

    Ok(())
}
